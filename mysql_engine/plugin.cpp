#include "pinba_config.h"

#include "mysql_engine/plugin.h"
#include "mysql_engine/handler.h"

#include "pinba/dictionary.h"

#include <time.h>   // localtime_r (non-portable include?)
#include <stdio.h>  // stderr, just in case :-|

#include <meow/format/format.hpp>
#include <meow/format/inserter/as_printf.hpp>
#include <meow/format/sink/char_buffer.hpp>
#include <meow/format/sink/fd.hpp>
#include <meow/unix/time.hpp>
#include <meow/unix/resource.hpp>

////////////////////////////////////////////////////////////////////////////////////////////////

static std::unique_ptr<pinba_mysql_ctx_t> pinba_MYSQL__instance;

pinba_mysql_ctx_t* pinba_MYSQL()
{
	return pinba_MYSQL__instance.get();
}

////////////////////////////////////////////////////////////////////////////////////////////////

static pinba_variables_t pinba_variables_ = {};

pinba_variables_t* pinba_variables()
{
	return &pinba_variables_;
}

////////////////////////////////////////////////////////////////////////////////////////////////

static pinba_status_variables_t  pinba_status_variables_;
static std::mutex                pinba_status_lock_;

pinba_status_variables_t* pinba_status_variables()
{
	return &pinba_status_variables_;
}

pinba_status_variables_ptr pinba_collect_status_variables()
{
	auto        vars  = meow::make_unique<pinba_status_variables_t>();
	auto const *stats = P_G_->stats();

	vars->uptime = timeval_to_double(os_unix::clock_monotonic_now() - stats->start_tv);

	{
		os_rusage_t ru = os_unix::getrusage_ex(RUSAGE_SELF);
		vars->ru_utime = timeval_to_double(timeval_from_os_timeval(ru.ru_utime));
		vars->ru_stime = timeval_to_double(timeval_from_os_timeval(ru.ru_stime));
	}

	// udp

	vars->udp_poll_total        = stats->udp.poll_total;
	vars->udp_recv_total        = stats->udp.recv_total;
	vars->udp_recv_eagain       = stats->udp.recv_eagain;
	vars->udp_recv_bytes        = stats->udp.recv_bytes;
	vars->udp_recv_packets      = stats->udp.recv_packets;
	vars->udp_packet_decode_err = stats->udp.packet_decode_err;
	vars->udp_batch_send_total  = stats->udp.batch_send_total;
	vars->udp_batch_send_err    = stats->udp.batch_send_err;
	vars->udp_packet_send_total = stats->udp.packet_send_total;
	vars->udp_packet_send_err   = stats->udp.packet_send_err;

	{
		std::lock_guard<std::mutex> lk_(stats->mtx);

		vars->udp_ru_utime = 0;
		vars->udp_ru_stime = 0;

		for (auto const& curr : stats->collector_threads)
		{
			vars->udp_ru_utime += timeval_to_double(curr.ru_utime);
			vars->udp_ru_stime += timeval_to_double(curr.ru_stime);
		}
	}

	// repacker

	vars->repacker_poll_total          = stats->repacker.poll_total;
	vars->repacker_recv_total          = stats->repacker.recv_total;
	vars->repacker_recv_eagain         = stats->repacker.recv_eagain;
	vars->repacker_recv_packets        = stats->repacker.recv_packets;
	vars->repacker_packet_validate_err = stats->repacker.packet_validate_err;
	vars->repacker_batch_send_total    = stats->repacker.batch_send_total;
	vars->repacker_batch_send_by_timer = stats->repacker.batch_send_by_timer;
	vars->repacker_batch_send_by_size  = stats->repacker.batch_send_by_size;

	{
		std::lock_guard<std::mutex> lk_(stats->mtx);

		vars->repacker_ru_utime = 0;
		vars->repacker_ru_stime = 0;

		for (auto const& curr : stats->repacker_threads)
		{
			vars->repacker_ru_utime += timeval_to_double(curr.ru_utime);
			vars->repacker_ru_stime += timeval_to_double(curr.ru_stime);
		}
	}

	// coordinator

	vars->coordinator_batches_received = stats->coordinator.batches_received;
	vars->coordinator_batch_send_total = stats->coordinator.batch_send_total;
	vars->coordinator_batch_send_err   = stats->coordinator.batch_send_err;
	vars->coordinator_control_requests = stats->coordinator.control_requests;

	{
		std::lock_guard<std::mutex> lk_(stats->mtx);

		vars->coordinator_ru_utime = timeval_to_double(stats->coordinator.ru_utime);
		vars->coordinator_ru_stime = timeval_to_double(stats->coordinator.ru_stime);
	}

	// dictionary

	{
		dictionary_t *dictionary = P_G_->dictionary();

		vars->dictionary_size     = dictionary->size();

		dictionary_memory_t const dmem = dictionary->memory_used();
		vars->dictionary_mem_hash    = dmem.hash_bytes;
		vars->dictionary_mem_list    = dmem.wordlist_bytes + dmem.freelist_bytes;
		vars->dictionary_mem_strings = dmem.strings_bytes;
	}

	// extras
	auto const extra_str = [&]()
	{
		std::lock_guard<std::mutex> lk_(P_CTX_->lock);

		auto const& cnt = P_CTX_->counters;
		auto const& obj = PINBA_STATS_(objects);

		std::string result;
		ff::fmt(result, "n_handlers: {0}, n_shares: {1}, n_views: {2}\n", cnt.n_handlers, cnt.n_shares, cnt.n_handlers);
		ff::fmt(result, "n_raw_batches: {0}, n_packet_batches: {1}\n", (uint64_t)obj.n_raw_batches, (uint64_t)obj.n_packet_batches);
		ff::fmt(result, "n_repacker_words: {0}, n_repacker_wordslices: {1}\n", (uint64_t)obj.n_repacker_dict_words, (uint64_t)obj.n_repacker_dict_ws);
		ff::fmt(result, "n_report_snapshots: {0}, n_report_ticks: {1}\n", (uint64_t)obj.n_report_snapshots, (uint64_t)obj.n_report_ticks);
		ff::fmt(result, "n_coord_requests: {0}\n", (uint64_t)obj.n_coord_requests);

		return result;
	}();
	snprintf(vars->extra, sizeof(vars->extra), "%s", extra_str.c_str());

	// version and build info
	snprintf(vars->version_info, sizeof(vars->version_info),
		"%s %s, git: %s, branch: %s, modified: %s",
		PINBA_PACKAGE_NAME, PINBA_VERSION, PINBA_VCS_FULL_HASH, PINBA_VCS_BRANCH, PINBA_VCS_WC_MODIFIED);

	snprintf(vars->build_string, sizeof(vars->build_string), "%s", PINBA_BUILD_STRING);

	return vars;
}

////////////////////////////////////////////////////////////////////////////////////////////////

static int pinba_engine_init(void *p)
{
	DBUG_ENTER(__func__);

	auto const log_level = meow::logging::log_level::enum_from_str_ref(pinba_variables()->log_level);
	assert(log_level != meow::logging::log_level::_none); // checked in mysql sysvar check function

	auto logger = [&]()
	{
		using namespace meow;
		using namespace meow::logging;
		namespace ff = meow::format;

		struct mysql_logger_t : public logger_t
		{
		private:
			ff::FILE_sink_t  sink_;
			log_level_t      level_;

		public:

			explicit mysql_logger_t(FILE *f, log_level_t lvl = log_level::debug)
				: sink_(f)
				, level_(lvl)
			{
			}

			virtual log_level_t level() const override { return level_; }
			virtual log_level_t set_level(log_level_t l) override { return level_ = l; }
			virtual bool        does_accept(log_level_t l) const override { return l <= level_; }

			virtual void write(
							  log_level_t 	lvl
							, line_mode_t 	lmode
							, size_t 		total_len
							, str_t const 	*slices
							, size_t 		n_slices
							)
							override
			{
				// as stderr is usually unbuffered -> writing one line in multiple write()-s might produce garbage in logs
				// try avoid that - by concatenating everything to one stack buffer to write everything in one go and avoid interleaves in log
				constexpr size_t const prefix_buf_size = 256;
				char *buf = (char*)alloca(prefix_buf_size + total_len);

				ff::char_buffer_sink_t tmp_sink { buf, prefix_buf_size + total_len };

				{
					struct tm curr_tm;
					time_t const curr_ts = my_time(0);
					localtime_r(&curr_ts, &curr_tm); // why localtime? don't ask, mysql does that

					// transform our log level to mysql's
					str_ref const level_str = [lvl]()
					{
						if (lvl >= log_level::notice)
							return meow::ref_lit("Note");

						if (lvl >= log_level::warn)
							return meow::ref_lit("Warning");

						return meow::ref_lit("ERROR");
					}();

					// write in mysql format, mildly copy-pasted from log.cc print_buffer_to_file()
					ff::fmt(tmp_sink,
						"{0} [{1}] PINBA: ",
						ff::as_printf("%d-%02d-%02d %02d:%02d:%02d %lu",
							curr_tm.tm_year + 1900,
							curr_tm.tm_mon + 1,
							curr_tm.tm_mday,
							curr_tm.tm_hour,
							curr_tm.tm_min,
							curr_tm.tm_sec,
							current_pid /* this is some mysql global */),
						level_str);

					// append message
					ff::write_to_sink(tmp_sink, total_len, slices, n_slices);

					// maybe append newline - as needed
					if (meow::line_mode::suffix & lmode)
						ff::write(tmp_sink, "\n");
				}

				// now write to stderr
				ff::write(sink_, str_ref{tmp_sink.buf(), tmp_sink.size()});

				// and flush just in case
				fflush(stderr);
			}
		};

		// don't care about std::bad_alloc exception here, let's crash!
		// need logger to be initialized to log real engine init errors
		return std::make_shared<mysql_logger_t>(stderr, log_level);
	}();

	LOG_NOTICE(logger, "version {0}, git: {1}, build: {2}", PINBA_VERSION, PINBA_VCS_FULL_HASH, PINBA_BUILD_STRING);

	try
	{
		// TODO: take more values from global mysql config (aka pinba_variables)
		static pinba_options_t options = {
			.net_address              = pinba_variables()->address,
			.net_port                 = ff::write_str(pinba_variables()->port),

			.udp_threads              = pinba_variables()->udp_reader_threads,
			.udp_batch_messages       = 256,
			.udp_batch_timeout        = 50 * d_millisecond,

			.repacker_threads         = pinba_variables()->repacker_threads,
			.repacker_input_buffer    = pinba_variables()->repacker_input_buffer,
			.repacker_batch_messages  = pinba_variables()->repacker_batch_messages,
			.repacker_batch_timeout   = pinba_variables()->repacker_batch_timeout_ms * d_millisecond,

			.coordinator_input_buffer = pinba_variables()->coordinator_input_buffer,
			.report_input_buffer      = pinba_variables()->report_input_buffer,

			.logger                   = logger,

			.packet_debug             = (bool)pinba_variables()->packet_debug,
			.packet_debug_fraction    = pinba_variables()->packet_debug_fraction,
		};

		pinba_MYSQL__instance = [&]()
		{
			auto PM = meow::make_unique<pinba_mysql_ctx_t>();
			PM->logger = logger;

			// process defaults
			PM->settings.time_window = pinba_variables()->default_history_time_sec * d_second;
			PM->settings.tick_count  = pinba_variables()->default_history_time_sec; // 1 second wide ticks

			// startup
			PM->engine = pinba_engine_init(&options); // construct objects, validate things
			PM->engine->startup();                    // start kicking ass

			return PM;
		}();

		LOG_NOTICE(logger, "engine initialized on {0}:{1}", options.net_address, options.net_port);
	}
	catch (std::exception const& e)
	{
		LOG_ERROR(logger, "engine initialization failed: {0}", e.what());
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
	}

	auto *h = static_cast<handlerton*>(p);
	h->state = SHOW_OPTION_YES;
	h->flags = HTON_ALTER_NOT_SUPPORTED | HTON_NO_PARTITION | HTON_TEMPORARY_NOT_SUPPORTED;
	h->create = [](handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root) -> handler* {
		return new (mem_root) pinba_handler_t(hton, table);
	};

	DBUG_RETURN(0);
}

static int pinba_engine_shutdown(void *p)
{
	DBUG_ENTER(__func__);

	pinba_MYSQL__instance->open_shares.clear();
	pinba_MYSQL__instance->engine->shutdown();
	pinba_MYSQL__instance.reset();

	DBUG_RETURN(0);
}

////////////////////////////////////////////////////////////////////////////////////////////////
// mysql plugin definitions

static MYSQL_SYSVAR_INT(port,
	pinba_variables()->port,
	PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
	"UDP port to listen at, default: 3002",
	NULL,
	NULL,
	3002,  // def
	0,     // min
	65536, // max
	0);

static MYSQL_SYSVAR_STR(address,
	pinba_variables()->address,
	PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
	"IP address to listen at (use * for all IPs, 0.0.0.0 for all v4 IPs, :: for all v6 IPs), must not be empty",
	[](MYSQL_THD thd, struct st_mysql_sys_var *var, void *tmp, struct st_mysql_value *value)
	{
		// disallow empty address
		char address_tmp[128];
		int address_sz = sizeof(address_tmp);
		char const *str = value->val_str(value, address_tmp, &address_sz);
		if (!str || *str == '\0' || address_sz == 0)
			return 1;
		return 0;
	},
	NULL,
	"*");

static MYSQL_SYSVAR_STR(log_level,
	pinba_variables()->log_level,
	PLUGIN_VAR_RQCMDARG,
	"log level, default: 'info'",
	[](MYSQL_THD thd, struct st_mysql_sys_var *var, void *res_for_update, struct st_mysql_value *value) // check
	{
		// disallow empty
		char tmp[128];
		int sz = sizeof(tmp);
		char const *str = value->val_str(value, tmp, &sz);
		if (!str || *str == '\0' || sz == 0)
			return 1;

		auto const lvl = meow::logging::log_level::enum_from_str_ref(str_ref{str, size_t(sz)});
		if (meow::logging::log_level::_none == lvl)
			return 1;

		*static_cast<int*>(res_for_update) = (int)lvl;

		return 0;
	},
	[](MYSQL_THD thd, struct st_mysql_sys_var *var, void *out_to_mysql, const void *saved_from_update) // update
	{
		// FIXME: unsure if this is always called after module init?
		//        if not - going to segfault

		auto const lvl = *(meow::logging::log_level_t*)saved_from_update;
		*static_cast<char const**>(out_to_mysql) = meow::logging::log_level::enum_as_str_ref(lvl).data();
		P_CTX_->logger->set_level(lvl);
	},
	"info");

static MYSQL_SYSVAR_UINT(default_history_time_sec,
	pinba_variables()->default_history_time_sec,
	PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
	"Default history time for reports (seconds)",
	NULL,
	NULL,
	60,
	10,
	INT_MAX,
	0);

static MYSQL_SYSVAR_UINT(udp_reader_threads,
	pinba_variables()->udp_reader_threads,
	PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
	"Number of UDP reader threads, default (2) is usually enough here, max 16",
	NULL,
	NULL,
	2,
	1,
	16,
	0);

static MYSQL_SYSVAR_UINT(repacker_threads,
	pinba_variables()->repacker_threads,
	PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
	"Number of internal packet-repack threads, try tunning higher if stats udp_batches_lost is > 0, max: 32",
	NULL,
	NULL,
	2,
	1,
	32,
	0);

static MYSQL_SYSVAR_UINT(repacker_input_buffer,
	pinba_variables()->repacker_input_buffer,
	PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
	"Buffer for X message for udp-reader -> packet-repack threads",
	NULL,
	NULL,
	512, // def: 256 udp batches, 256 pinba requests each ~= 128K packets, per each repacker thread
	128,
	16 * 1024,
	0);

static MYSQL_SYSVAR_UINT(repacker_batch_messages,
	pinba_variables()->repacker_batch_messages,
	PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
	"Batch size for packet-repack -> reports communication",
	NULL,
	NULL,
	1024,
	128,
	16 * 1024,
	0);

static MYSQL_SYSVAR_UINT(repacker_batch_timeout_ms,
	pinba_variables()->repacker_batch_timeout_ms,
	PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
	"Max delay between packet-repack thread batches generation (in milliseconds!)",
	NULL,
	NULL,
	100,
	1,
	1000,
	0);

static MYSQL_SYSVAR_UINT(coordinator_input_buffer,
	pinba_variables()->coordinator_input_buffer,
	PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
	"coordinator thread input buffer (packet batches coming from packet-repack threads)",
	NULL,
	NULL,
	128, // def: 128 batches, 1024 packets each
	16,
	1 * 1024,
	0);

static MYSQL_SYSVAR_UINT(report_input_buffer,
	pinba_variables()->report_input_buffer,
	PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
	"Buffer for X batches (each has max repacker_batch_messages packets) for each activated report",
	NULL,
	NULL,
	128, // def: 128 batches, 1024 packets each
	16,
	8 * 1024,
	0);

static MYSQL_SYSVAR_BOOL(packet_debug,
	pinba_variables()->packet_debug,
	PLUGIN_VAR_RQCMDARG,
	"dump incoming packets to log (at INFO level)",
	[](MYSQL_THD thd, struct st_mysql_sys_var *var, void *res_for_update, struct st_mysql_value *value) // check
	{
		long long tmp;
		int const is_null = value->val_int(value, &tmp);

		*static_cast<char*>(res_for_update) = (char)((is_null) ? 0 : tmp);

		return 0;
	},
	[](MYSQL_THD thd, struct st_mysql_sys_var *var, void *out_to_mysql, const void *saved_from_update) // update
	{
		char const saved_val = *(char*)saved_from_update;

		P_E_->options_mutable()->packet_debug = (bool)saved_val;
		*static_cast<char*>(out_to_mysql) = saved_val;
	},
	0);

static MYSQL_SYSVAR_DOUBLE(packet_debug_fraction,
	pinba_variables()->packet_debug_fraction,
	PLUGIN_VAR_RQCMDARG,
	"fraction of incoming packets to dump (when packet_debug) is set",
	[](MYSQL_THD thd, struct st_mysql_sys_var *var, void *res_for_update, struct st_mysql_value *value) // check
	{
		double tmp;
		int const is_null = value->val_real(value, &tmp);

		*static_cast<double*>(res_for_update) = (is_null) ? 0.01 : tmp;

		return 0;
	},
	[](MYSQL_THD thd, struct st_mysql_sys_var *var, void *out_to_mysql, const void *saved_from_update) // update
	{
		double const saved_val = *(double*)saved_from_update;

		P_E_->options_mutable()->packet_debug_fraction = saved_val;
		*static_cast<double*>(out_to_mysql) = saved_val;

	},
	0.01, // def: every 100th
	0.000000001,
	1.0,
	0);

static struct st_mysql_sys_var* system_variables[]= {
	MYSQL_SYSVAR(port),
	MYSQL_SYSVAR(address),
	MYSQL_SYSVAR(log_level),
	MYSQL_SYSVAR(default_history_time_sec),
	MYSQL_SYSVAR(udp_reader_threads),
	MYSQL_SYSVAR(repacker_threads),
	MYSQL_SYSVAR(repacker_input_buffer),
	MYSQL_SYSVAR(repacker_batch_messages),
	MYSQL_SYSVAR(repacker_batch_timeout_ms),
	MYSQL_SYSVAR(coordinator_input_buffer),
	MYSQL_SYSVAR(report_input_buffer),
	MYSQL_SYSVAR(packet_debug),
	MYSQL_SYSVAR(packet_debug_fraction),
	NULL
};

////////////////////////////////////////////////////////////////////////////////////////////////

static void status_variables_show_func(THD*, SHOW_VAR *var, char *buf)
{
	static SHOW_VAR const vars[] = {
	#define SVAR(name, show_type) \
		{ #name, (char*)&pinba_status_variables()->name, show_type }, \
	/**/

		SVAR(uptime,                            SHOW_DOUBLE)
		SVAR(ru_utime,                          SHOW_DOUBLE)
		SVAR(ru_stime,                          SHOW_DOUBLE)
		SVAR(udp_poll_total,                    SHOW_LONGLONG)
		SVAR(udp_recv_total,                    SHOW_LONGLONG)
		SVAR(udp_recv_eagain,                   SHOW_LONGLONG)
		SVAR(udp_recv_bytes,                    SHOW_LONGLONG)
		SVAR(udp_recv_packets,                  SHOW_LONGLONG)
		SVAR(udp_packet_decode_err,             SHOW_LONGLONG)
		SVAR(udp_batch_send_total,              SHOW_LONGLONG)
		SVAR(udp_batch_send_err,                SHOW_LONGLONG)
		SVAR(udp_packet_send_total,             SHOW_LONGLONG)
		SVAR(udp_packet_send_err,               SHOW_LONGLONG)
		SVAR(udp_ru_utime,                      SHOW_DOUBLE)
		SVAR(udp_ru_stime,                      SHOW_DOUBLE)
		SVAR(repacker_poll_total,               SHOW_LONGLONG)
		SVAR(repacker_recv_total,               SHOW_LONGLONG)
		SVAR(repacker_recv_eagain,              SHOW_LONGLONG)
		SVAR(repacker_recv_packets,             SHOW_LONGLONG)
		SVAR(repacker_packet_validate_err,      SHOW_LONGLONG)
		SVAR(repacker_batch_send_total,         SHOW_LONGLONG)
		SVAR(repacker_batch_send_by_timer,      SHOW_LONGLONG)
		SVAR(repacker_batch_send_by_size,       SHOW_LONGLONG)
		SVAR(repacker_ru_utime,                 SHOW_DOUBLE)
		SVAR(repacker_ru_stime,                 SHOW_DOUBLE)
		SVAR(coordinator_batches_received,      SHOW_LONGLONG)
		SVAR(coordinator_batch_send_total,      SHOW_LONGLONG)
		SVAR(coordinator_batch_send_err,        SHOW_LONGLONG)
		SVAR(coordinator_control_requests,      SHOW_LONGLONG)
		SVAR(coordinator_ru_utime,              SHOW_DOUBLE)
		SVAR(coordinator_ru_stime,              SHOW_DOUBLE)
		SVAR(dictionary_size,                   SHOW_LONGLONG)
		SVAR(dictionary_mem_hash,               SHOW_LONGLONG)
		SVAR(dictionary_mem_list,               SHOW_LONGLONG)
		SVAR(dictionary_mem_strings,            SHOW_LONGLONG)
		SVAR(extra,                             SHOW_CHAR)
		SVAR(version_info,                      SHOW_CHAR)
		SVAR(build_string,                      SHOW_CHAR)

	#undef SVAR
		{ NullS, NullS, SHOW_LONG }
	};

	// XXX: assume that system vars are only selected with some kind of lock held
	//      otherwise - i've got no idea how to protect the global datastructure
	//      against modification while data is being read
	//      i mean there is no way to lock things here, since we're not going to get called again to unlock
	//      and can't just clone the data, since there is no way to free afterwards
	//
	//      so just lock on our side to prevent multiple 'show status' queries from fucking up the data
	//      copy needed, since our var pointers are fixed in SVAR macros above
	{
		std::lock_guard<std::mutex> lk_(pinba_status_lock_);
		*pinba_status_variables() = *pinba_collect_status_variables();
	}

	var->type = SHOW_ARRAY;
	var->value = (char*)&vars;
}

static SHOW_VAR status_variables_export[]= {
	{
		.name  = "Pinba",
		.value = (char*)&status_variables_show_func,
		.type  = SHOW_FUNC,
	},
	{NullS, NullS, SHOW_LONG}
};

////////////////////////////////////////////////////////////////////////////////////////////////

struct st_mysql_storage_engine pinba_storage_engine = { MYSQL_HANDLERTON_INTERFACE_VERSION };

mysql_declare_plugin(pinba)
{
	MYSQL_STORAGE_ENGINE_PLUGIN,
	&pinba_storage_engine,
	"PINBA",
	"Anton Povarov",
	"Pinba storage engine v2",
	PLUGIN_LICENSE_BSD,                           /* make this BSD maybe ? */
	pinba_engine_init,                            /* Plugin Init */
	pinba_engine_shutdown,                        /* Plugin Deinit */
	0x0200                                        /* version: 2.0 */,
	status_variables_export,                      /* status variables */
	system_variables,                             /* system variables */
	NULL,                                         /* config options */
	0,                                            /* flags */
}
mysql_declare_plugin_end;
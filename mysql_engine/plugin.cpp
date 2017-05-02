#include "mysql_engine/plugin.h"
#include "mysql_engine/handler.h"

#include "pinba/dictionary.h"

#define MEOW_FORMAT_FD_SINK_NO_WRITEV 1
#include <meow/logging/fd_logger.hpp>

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

	// udp

	vars->udp_poll_total        = stats->udp.poll_total;
	vars->udp_recv_total        = stats->udp.recv_total;
	vars->udp_recv_eagain       = stats->udp.recv_eagain;
	vars->udp_recv_bytes        = stats->udp.recv_bytes;
	vars->udp_recv_packets      = stats->udp.recv_packets;
	vars->udp_packet_decode_err = stats->udp.packet_decode_err;
	vars->udp_batch_send_total  = stats->udp.batch_send_total;
	vars->udp_batch_send_err    = stats->udp.batch_send_err;

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
		vars->dictionary_mem_used = dictionary->memory_used();
	}

	// extras
	auto const extra_str = [&]()
	{
		std::lock_guard<std::mutex> lk_(P_CTX_->lock);

		auto const& cnt = P_CTX_->counters;
		return ff::fmt_str("n_handlers: {0}, n_shares: {1}, n_views: {2}", cnt.n_handlers, cnt.n_shares, cnt.n_handlers);
	}();
	snprintf(vars->extra, sizeof(vars->extra), "%s", extra_str.c_str());


	return std::move(vars);
}

////////////////////////////////////////////////////////////////////////////////////////////////

static int pinba_engine_init(void *p)
{
	DBUG_ENTER(__func__);

	auto *h = static_cast<handlerton*>(p);
	h->state = SHOW_OPTION_YES;
	h->create = [](handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root) -> handler* {
		return new (mem_root) pinba_handler_t(hton, table);
	};

	auto logger = [&]()
	{
		// TODO: this will suffice for now, but improve logger to look like mysql native thing
		auto logger = std::make_shared<meow::logging::fd_logger_t<meow::logging::default_prefix_t>>(STDERR_FILENO);

		logger->set_level(meow::logging::log_level::debug);
		logger->prefix().set_fields(meow::logging::prefix_field::_all);
		logger->prefix().set_log_name("pinba");

		return logger;
	}();

	// TODO: take more values from global mysql config (aka pinba_variables)
	static pinba_options_t options = {
		.net_address              = pinba_variables()->address,
		.net_port                 = ff::write_str(pinba_variables()->port),

		.udp_threads              = pinba_variables()->udp_reader_threads,
		.udp_batch_messages       = 256,
		.udp_batch_timeout        = 10 * d_millisecond,

		.repacker_threads         = pinba_variables()->repacker_threads,
		.repacker_input_buffer    = pinba_variables()->repacker_input_buffer,
		.repacker_batch_messages  = pinba_variables()->repacker_batch_messages,
		.repacker_batch_timeout   = pinba_variables()->repacker_batch_timeout_ms * d_millisecond,

		.coordinator_input_buffer = pinba_variables()->coordinator_input_buffer,
		.report_input_buffer      = pinba_variables()->report_input_buffer,

		.logger                   = logger,
	};

	try
	{
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

			return move(PM);
		}();
	}
	catch (std::exception const& e)
	{
		LOG_ALERT(logger, "init failed: {0}\n", e.what());
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
	}

	DBUG_RETURN(0);
}

static int pinba_engine_shutdown(void *p)
{
	DBUG_ENTER(__func__);

	// FIXME: this is very likely to cause a segfault
	//  since pinba internals shutdown is not implemented well, if at all
	pinba_MYSQL__instance->engine->shutdown();
	pinba_MYSQL__instance.reset();

	DBUG_RETURN(0);
}

////////////////////////////////////////////////////////////////////////////////////////////////
// mysql plugin definitions

static MYSQL_SYSVAR_INT(port,
	pinba_variables()->port,
	PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
	"UDP port to listen at",
	NULL,
	NULL,
	3002,  // def
	0,     // min
	65536, // max
	0);

static MYSQL_SYSVAR_STR(address,
	pinba_variables()->address,
	PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
	"IP address to listen at (use 0.0.0.0 here if you want to listen on all IPs), must not be empty",
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
	"0.0.0.0");

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
	"Number of UDP reader threads, default (4) is usually enough here, max 16",
	NULL,
	NULL,
	4,
	1,
	16,
	0);

static MYSQL_SYSVAR_UINT(repacker_threads,
	pinba_variables()->repacker_threads,
	PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
	"Number of internal packet-repack threads, try tunning higher if stats udp_batches_lost is > 0, max: 16",
	NULL,
	NULL,
	4,
	1,
	16,
	0);

static MYSQL_SYSVAR_UINT(repacker_input_buffer,
	pinba_variables()->repacker_input_buffer,
	PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
	"Buffer for X message for udp-reader -> packet-repack threads",
	NULL,
	NULL,
	4 * 1024, // def: 4096 udp batches, 256 pinba requests each ~= 1M packets, per each repacker thread
	128,
	128 * 1024,
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

static struct st_mysql_sys_var* system_variables[]= {
	MYSQL_SYSVAR(port),
	MYSQL_SYSVAR(address),
	MYSQL_SYSVAR(default_history_time_sec),
	MYSQL_SYSVAR(udp_reader_threads),
	MYSQL_SYSVAR(repacker_threads),
	MYSQL_SYSVAR(repacker_input_buffer),
	MYSQL_SYSVAR(repacker_batch_messages),
	MYSQL_SYSVAR(repacker_batch_timeout_ms),
	MYSQL_SYSVAR(coordinator_input_buffer),
	MYSQL_SYSVAR(report_input_buffer),
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
		SVAR(udp_poll_total,                    SHOW_LONGLONG)
		SVAR(udp_recv_total,                    SHOW_LONGLONG)
		SVAR(udp_recv_eagain,                   SHOW_LONGLONG)
		SVAR(udp_recv_bytes,                    SHOW_LONGLONG)
		SVAR(udp_recv_packets,                  SHOW_LONGLONG)
		SVAR(udp_packet_decode_err,             SHOW_LONGLONG)
		SVAR(udp_batch_send_total,              SHOW_LONGLONG)
		SVAR(udp_batch_send_err,                SHOW_LONGLONG)
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
		SVAR(dictionary_mem_used,               SHOW_LONGLONG)
		SVAR(extra,                             SHOW_CHAR)

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
	PLUGIN_LICENSE_GPL /*PLUGIN_LICENSE_BSD*/,    /* make this BSD maybe ? */
	pinba_engine_init,                            /* Plugin Init */
	pinba_engine_shutdown,                        /* Plugin Deinit */
	0x0200                                        /* version: 2.0 */,
	status_variables_export,                      /* status variables */
	system_variables,                             /* system variables */
	NULL,                                         /* config options */
	HTON_ALTER_NOT_SUPPORTED,                     /* flags */
}
mysql_declare_plugin_end;
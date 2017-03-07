#include "mysql_engine/pinba_mysql.h" // should be first mysql-related include
#include "mysql_engine/handler.h"

#ifdef PINBA_USE_MYSQL_SOURCE
#include <mysql_version.h>
#include <mysql/plugin.h>
#else
#include <mysql/mysql_version.h>
#include <mysql/plugin.h>
#endif // PINBA_USE_MYSQL_SOURCE

#include <meow/format/format.hpp>
#include <meow/format/format_to_string.hpp>
#include <meow/logging/log_level.hpp>
#include <meow/logging/log_prefix.hpp>
#include <meow/logging/logger_impl.hpp>

namespace ff = meow::format;

////////////////////////////////////////////////////////////////////////////////////////////////

static pinba_variables_t pinba_variables_;

pinba_variables_t* pinba_variables()
{
	return &pinba_variables_;
}

////////////////////////////////////////////////////////////////////////////////////////////////

static std::unique_ptr<pinba_mysql_ctx_t> pinba_MYSQL__instance;

pinba_mysql_ctx_t* pinba_MYSQL()
{
	return pinba_MYSQL__instance.get();
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

		.coordinator_input_buffer = pinba_variables()->report_input_buffer,
	};

	try
	{
		pinba_MYSQL__instance = [&]()
		{
			auto PM = meow::make_unique<pinba_mysql_ctx_t>();
			PM->logger = [&]()
			{
				// TODO: this will suffice for now, but improve logger to look like mysql native thing
				auto logger = meow::make_unique<meow::logging::logger_impl_t<meow::logging::default_prefix_t>>();

				logger->set_level(meow::logging::log_level::debug);
				logger->prefix().set_fields(meow::logging::prefix_field::_all);
				logger->prefix().set_log_name("pinba");

				logger->set_writer([](size_t total_len, str_ref const *slices, size_t n_slices)
				{
					for (size_t i = 0; i < n_slices; ++i)
						ff::write(stderr, slices[i]);
				});
				return move(logger);
			}();

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
		// no logger here, so just dump stuff to stderr directly
		ff::fmt(stderr, "[pinba] init failed: {0}\n", e.what());
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
	8 * 1024,
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

static MYSQL_SYSVAR_UINT(report_input_buffer,
	pinba_variables()->report_input_buffer,
	PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
	"Buffer for X batches (each has max repacker_batch_messages packets) for each activated report",
	NULL,
	NULL,
	1024,
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
	MYSQL_SYSVAR(report_input_buffer),
	NULL
};

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
	NULL,                                         /* status variables */
	system_variables,                             /* system variables */
	NULL,                                         /* config options */
	HTON_ALTER_NOT_SUPPORTED,                     /* flags */
}
mysql_declare_plugin_end;
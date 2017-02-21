#include "mysql_engine/handler.h" // make sure - that this is the first mysql-related include

#include <mysql_version.h>
#include <sql/handler.h>

#include <meow/logging/log_level.hpp>
#include <meow/logging/log_prefix.hpp>
#include <meow/logging/logger_impl.hpp>

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

	// FIXME: take values from global mysql config
	static pinba_options_t options = {
		.net_address              = "0.0.0.0",
		.net_port                 = "3002",

		.udp_threads              = 4,
		.udp_batch_messages       = 256,
		.udp_batch_timeout        = 10 * d_millisecond,

		.repacker_threads         = 4,
		.repacker_input_buffer    = 8 * 1024,
		.repacker_batch_messages  = 1024,
		.repacker_batch_timeout   = 100 * d_millisecond,

		.coordinator_input_buffer = 1 * 1024,
	};

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

		PM->engine = pinba_engine_init(&options); // construct objects, validate things
		PM->engine->startup();                    // start kicking ass

		return move(PM);
	}();

	DBUG_RETURN(0);
}

static int pinba_engine_shutdown(void *p)
{
	DBUG_ENTER(__func__);

	// FIXME: this is very likely to cause a segfault
	//  since pinba internals shutdown is not implemented well, if at all
	pinba_MYSQL__instance.reset();

	DBUG_RETURN(0);
}

////////////////////////////////////////////////////////////////////////////////////////////////
// mysql plugin definitions

static int port_var = 0;
static char *address_var = NULL;
static unsigned int stats_gathering_period_var = 0;
// static unsigned int log_level_var = P_ERROR | P_WARNING | P_NOTICE;

static MYSQL_SYSVAR_INT(port,
	port_var,
	PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
	"UDP port to listen at",
	NULL,
	NULL,
	3002,
	0,
	65536,
	0);

static MYSQL_SYSVAR_STR(address,
	address_var,
	PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
	"IP address to listen at (leave it empty if you want to listen on all IPs)",
	NULL,
	NULL,
	NULL);

static MYSQL_SYSVAR_UINT(stats_gathering_period,
	stats_gathering_period_var,
	PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
	"Request stats gathering period (microseconds)",
	NULL,
	NULL,
	10000,
	10,
	INT_MAX,
	0);

// static MYSQL_SYSVAR_UINT(log_level,
//   log_level_var,
//   PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
//   "Set log level",
//   NULL,
//   NULL,
//   P_ERROR | P_WARNING | P_NOTICE,
//   0,
//   INT_MAX,
//   0);

static struct st_mysql_sys_var* system_variables[]= {
	MYSQL_SYSVAR(port),
	MYSQL_SYSVAR(address),
	// MYSQL_SYSVAR(log_level),
	MYSQL_SYSVAR(stats_gathering_period),
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
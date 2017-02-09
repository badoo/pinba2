#include <mysql_version.h>
#include <sql/handler.h>

#include "pinba/handler.h"

////////////////////////////////////////////////////////////////////////////////////////////////

// static handler* pinba_create_handler(handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root) /* {{{ */
// {
// 	return new (mem_root) ha_pinba(hton, table);
// }

static int pinba_engine_init(void *p)
{
	DBUG_ENTER(__func__);

	auto h = static_cast<handlerton*>(p);
	h->state = SHOW_OPTION_YES;
	// h->create = pinba_create_handler;
	h->create = [](handlerton *hton, TABLE_SHARE *table, MEM_ROOT *mem_root) -> handler* {
		return new (mem_root) ha_pinba(hton, table);
	};

	DBUG_RETURN(0);
}

static int pinba_engine_shutdown(void *p)
{
	DBUG_ENTER(__func__);
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

mysql_declare_plugin(example)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &pinba_storage_engine,
  "PINBA",
  "Antony Dovgal, Anton Povarov",
  "Pinba storage engine",
  PLUGIN_LICENSE_GPL /*PLUGIN_LICENSE_BSD*/,    /* make this BSD maybe ? */
  pinba_engine_init,                            /* Plugin Init */
  pinba_engine_shutdown,                        /* Plugin Deinit */
  0x0200 /* 2.0 */,
  NULL,                                         /* status variables */
  system_variables,                             /* system variables */
  NULL,                                         /* config options */
  0,                                            /* flags */
}
mysql_declare_plugin_end;
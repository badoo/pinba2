#ifndef PINBA__MYSQL_ENGINE__PINBA_MYSQL_H_
#define PINBA__MYSQL_ENGINE__PINBA_MYSQL_H_

#if defined(PINBA_ENGINE_DEBUG_ON) && !defined(DBUG_ON)
# undef DBUG_OFF
# define DBUG_ON
#endif

#if defined(PINBA_ENGINE_DEBUG_OFF) && !defined(DBUG_OFF)
# define DBUG_OFF
# undef DBUG_ON
#endif


#include <memory>
#include <mutex>
#include <unordered_map>

#include <boost/noncopyable.hpp>

#include <meow/logging/logger.hpp>
#include <meow/logging/log_write.hpp>

#include "pinba/globals.h"
#include "pinba/engine.h"
#include "pinba/report.h"

////////////////////////////////////////////////////////////////////////////////////////////////

using pinba_report_ptr = report_ptr;

struct pinba_mysql_ctx_t;
struct pinba_handler_t;

struct pinba_share_t;
using pinba_share_ptr = std::shared_ptr<pinba_share_t>;

////////////////////////////////////////////////////////////////////////////////////////////////

using pinba_open_shares_t = std::unordered_map<std::string, pinba_share_ptr>;

struct pinba_mysql_ctx_t : private boost::noncopyable
{
	std::mutex          lock;

	struct {
		duration_t  time_window = {};
		uint32_t    tick_count  = 0;
	} settings;

	struct {
		uint32_t n_handlers = 0;
		uint32_t n_shares   = 0;
		uint32_t n_views    = 0;
	} counters;

	pinba_logger_ptr    logger;
	pinba_engine_ptr    engine;
	pinba_open_shares_t open_shares;
};

// a global singleton for this plugin
// instantiated and managed in plugin.cpp
pinba_mysql_ctx_t* pinba_MYSQL();

#define P_CTX_  pinba_MYSQL()
#define P_E_    (P_CTX_->engine)
#define P_G_    (P_E_->globals())
#define P_L_    (P_CTX_->logger.get())

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__MYSQL_ENGINE__PINBA_MYSQL_H_

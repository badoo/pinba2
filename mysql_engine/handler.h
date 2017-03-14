#ifndef PINBA__HANDLER_H_
#define PINBA__HANDLER_H_

#include "pinba_mysql.h" // should be first mysql-related include

#ifdef PINBA_USE_MYSQL_SOURCE
#include <mysql/plugin.h>
#include <sql/handler.h>
#else
#include <mysql/plugin.h>
#include <mysql/private/handler.h>
#endif // PINBA_USE_MYSQL_SOURCE

#include <string>
#include <memory>
#include <thread>
#include <mutex>
#include <unordered_map>

#include <meow/intrusive_ptr.hpp>
#include <meow/logging/logger.hpp>
#include <meow/logging/log_write.hpp>

#include "pinba/engine.h"

////////////////////////////////////////////////////////////////////////////////////////////////

using logger_t              = meow::logging::logger_t;
using pinba_report_ptr      = report_ptr;

struct pinba_handler_t;

////////////////////////////////////////////////////////////////////////////////////////////////

// this object represents a table open in each thread (i.e. this object doesn't need locking)
// references a share (and corresponding report)
// is basically an abstraction over table data, enabling iteration and stuff
// useful because each select has it's own copy of table data that is iterated separately
// also hides all table data details from pinba_handler_t
struct pinba_view_t;
using  pinba_view_ptr = std::unique_ptr<pinba_view_t>;

struct pinba_view_kind
{
	enum type : uint32_t
	{
		stats                  = 0,
		active_reports         = 1,
		report_by_request_data = 2,
		report_by_timer_data   = 3,
		report_by_packet_data  = 4,
	};
};
typedef pinba_view_kind::type pinba_view_kind_t;
#if 0
struct pinba_view_conf_t
{
	std::string                 orig_comment;

	pinba_view_kind_t           kind;
	duration_t                  time_window;
	uint32_t                    tick_count;

	std::vector<str_ref>        keys;

	struct filter_spec_t
	{
		str_ref key;
		str_ref value;
	};
	std::vector<filter_spec_t>  key_filters;

	uint32_t                    hv_bucket_count;
	duration_t                  hv_bucket_d;
	std::vector<double>         percentiles;

	duration_t                  min_time;    // 0 if unset
	duration_t                  max_time;    // 0 if unset
};
#endif
struct pinba_view_conf_t;
using pinba_view_conf_ptr = std::unique_ptr<pinba_view_conf_t>; // incomplete type here, code using will see the complete type

struct pinba_view_t : private boost::noncopyable
{
	virtual ~pinba_view_t() {}

	virtual int  rnd_init(pinba_handler_t*, bool scan) = 0;
	virtual int  rnd_end(pinba_handler_t*) = 0;
	virtual int  rnd_next(pinba_handler_t*, uchar *buf) = 0;
	virtual int  rnd_pos(pinba_handler_t*, uchar *buf, uchar *pos) const = 0;
	virtual void position(pinba_handler_t*, const uchar *record) const = 0;
	virtual int  info(pinba_handler_t*, uint) const = 0;
	virtual int  extra(pinba_handler_t*, enum ha_extra_function operation) const = 0;
};

////////////////////////////////////////////////////////////////////////////////////////////////

// shared table descriptor structure
// these **ARE NOT** deleted on handler::close(),
// since we need to retain information on what reports are active inside pinba engine
// regardless of what tables are open in mysql
// these ARE deleted on 'drop table' (aka handler::delete_table())
// and corresponding report is dropped from pinba engine as well
struct pinba_share_t
	: private boost::noncopyable
	, boost::intrusive_ref_counter<pinba_share_t>
{
	THR_LOCK               lock;                 // mysql thread table lock (no idea, mon)

	std::string            mysql_name;           // mysql table name (can be changed)
	pinba_view_conf_ptr    view_conf;            // config parsed from mysql table comment

	pinba_report_ptr       report;               // stored here after creation, before activation
	std::string            report_name;          // pinba engine report name (immune to mysql table renames)
	bool                   report_active;        // has the report (above) been activated with pinba engine?
	bool                   report_needs_engine;  // if this report exists in pinba engine

	pinba_share_t(std::string const& table_name);
	~pinba_share_t();
};
typedef boost::intrusive_ptr<pinba_share_t> pinba_share_ptr;

////////////////////////////////////////////////////////////////////////////////////////////////

using pinba_open_shares_t = std::unordered_map<std::string, pinba_share_ptr>;

struct pinba_mysql_ctx_t : private boost::noncopyable
{
	std::mutex                 lock;
	pinba_engine_ptr           engine;
	std::unique_ptr<logger_t>  logger;

	pinba_open_shares_t        open_shares;

	struct {
		duration_t  time_window;
		uint32_t    tick_count;
	} settings;
};

// a global singleton for this plugin
// instantiated and managed in plugin.cpp
pinba_mysql_ctx_t* pinba_MYSQL();

#define P_CTX_  pinba_MYSQL()
#define P_E_    (P_CTX_->engine)
#define P_G_    (P_E_->globals())
#define P_L_    (P_CTX_->logger.get())

// #define PG_(...) pinba_MYSQL()->##__VA_ARGS__

////////////////////////////////////////////////////////////////////////////////////////////////

class pinba_handler_t : public handler
{
	THR_LOCK_DATA lock_data;   ///< MySQL lock

	pinba_share_ptr share_;     ///< current-table shared info
	pinba_view_ptr pinba_view_; // currently open pinba table wrapper

public:
	logger_t      *log_;

	pinba_share_ptr current_share() const;
	TABLE*          current_table() const;

public:

	pinba_handler_t(handlerton *hton, TABLE_SHARE *table_arg);
	~pinba_handler_t();

	/** @brief
		The name that will be used for display purposes.
	*/
	const char *table_type() const { return "PINBA"; }

	/** @brief
		The name of the index type that will be used for display.
		Don't implement this method unless you really have indexes.
	*/
	// const char *index_type(uint inx) { return "HASH"; }

	/** @brief
		The file extensions.
	*/
	const char **bas_ext() const
	{
		static const char *ha_pinba_exts[] = { NullS };
		return ha_pinba_exts;
	}

	/** @brief
		This is a list of flags that indicate what functionality the storage engine
		implements. The current table flags are documented in handler.h
	*/
	ulonglong table_flags() const
	{
		// HA_REC_NOT_IN_SEQ
		// HA_FAST_KEY_READ
		// HA_NO_BLOBS
		// HA_REQUIRE_PRIMARY_KEY
		// HA_BINLOG_ROW_CAPABLE

		// TODO
		// | HA_STATS_RECORDS_IS_EXACT

		return ( HA_NO_AUTO_INCREMENT | HA_NO_TRANSACTIONS);
	}

	/** @brief
		This is a bitmap of flags that indicates how the storage engine
		implements indexes. The current index flags are documented in
		handler.h. If you do not implement indexes, just return zero here.

	  @details
		part is the key part to check. First key part is 0.
		If all_parts is set, MySQL wants to know the flags for the combined
		index, up to and including 'part'.
	*/
	ulong index_flags(uint inx, uint part, bool all_parts) const
	{
		return 0;
	}
	// ulong index_flags(uint inx, uint part, bool all_parts) const
	// {
	// 	return HA_READ_NEXT | HA_READ_PREV | HA_READ_ORDER | HA_ONLY_WHOLE_INDEX;;
	// }

	/** @brief
		unireg.cc will call max_supported_record_length(), max_supported_keys(),
		max_supported_key_parts(), uint max_supported_key_length()
		to make sure that the storage engine can handle the data it is about to
		send. Return *real* limits of your storage engine here; MySQL will do
		min(your_limits, MySQL_limits) automatically.
	*/
	uint max_supported_record_length() const { return HA_MAX_REC_LENGTH; }

	/** @brief
		unireg.cc will call this to make sure that the storage engine can handle
		the data it is about to send. Return *real* limits of your storage engine
		here; MySQL will do min(your_limits, MySQL_limits) automatically.

		@details
		There is no need to implement ..._key_... methods if your engine doesn't
		support indexes.
	*/
	  // uint max_supported_keys()          const { return 2; /* FIXME: add a constant for this */ }

	/** @brief
		unireg.cc will call this to make sure that the storage engine can handle
		the data it is about to send. Return *real* limits of your storage engine
		here; MySQL will do min(your_limits, MySQL_limits) automatically.

		@details
		There is no need to implement ..._key_... methods if your engine doesn't
		support indexes.
	*/
	// uint max_supported_key_parts()     const { return 1; }

	/** @brief
		unireg.cc will call this to make sure that the storage engine can handle
		the data it is about to send. Return *real* limits of your storage engine
		here; MySQL will do min(your_limits, MySQL_limits) automatically.

		@details
		There is no need to implement ..._key_... methods if your engine doesn't
		support indexes.
	*/
	// uint max_supported_key_length()    const { return 256; }

	/** @brief
		Called in test_quick_select to determine if indexes should be used.
	*/
	// virtual double scan_time() { return (double) (stats.records+stats.deleted) / 20.0+10; }

	/** @brief
		This method will never be called if you do not implement indexes.
	*/
	// virtual double read_time(uint, uint, ha_rows rows) { return (double) rows /  20.0+1; }

public:

	int open(const char *name, int mode, uint test_if_locked);    // required
	int close(void);                                              // required

	// not required, but implement these and explicitly disallow writes
	// int write_row(uchar *buf);
	// int update_row(const uchar *old_data, uchar *new_data);
	// int delete_row(const uchar *buf);

	// index stuff
	// int index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map, enum ha_rkey_function find_flag);
	// int index_next(uchar *buf);
	// int index_prev(uchar *buf);
	// int index_first(uchar *buf);
	// int index_last(uchar *buf);

	/** @brief
		Unlike index_init(), rnd_init() can be called two consecutive times
		without rnd_end() in between (it only makes sense if scan=1). In this
		case, the second call should prepare for the new table scan (e.g if
		rnd_init() allocates the cursor, the second call should position the
		cursor to the start of the table; no need to deallocate and allocate
		it again. This is a required method.
	*/
	int rnd_init(bool scan);                                      //required
	int rnd_end();
	int rnd_next(uchar *buf);                                     ///< required
	int rnd_pos(uchar *buf, uchar *pos);                          ///< required
	void position(const uchar *record);                           ///< required

	int info(uint);                                               ///< required
	int extra(enum ha_extra_function operation);
	// int external_lock(THD *thd, int lock_type);                   ///< required
	// int delete_all_rows(void);
	// int truncate();
	// ha_rows records_in_range(uint inx, key_range *min_key, key_range *max_key);

	int delete_table(const char *from);
	int rename_table(const char * from, const char * to);
	int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info);                     ///< required

	THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type);     ///< required
};

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__HANDLER_H_

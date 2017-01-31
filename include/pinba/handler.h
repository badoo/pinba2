#ifndef PINBA__HANDLER_H_
#define PINBA__HANDLER_H_

#include <mysql/plugin.h>
#include <sql/handler.h>


class ha_pinba : public handler
{
	THR_LOCK_DATA lock;      ///< MySQL lock
	// Example_share *share;    ///< Shared lock info
	// Example_share *get_share(); ///< Get the share

public:

	ha_pinba(handlerton *hton, TABLE_SHARE *table_arg);
	~ha_pinba();

	/** @brief
		The name that will be used for display purposes.
	*/
	const char *table_type() const { return "PINBA"; }

	/** @brief
		The name of the index type that will be used for display.
		Don't implement this method unless you really have indexes.
	*/
	const char *index_type(uint inx) { return "HASH"; }

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

		// also unsure why HA_BINLOG_ROW_CAPABLE is being set ?

		return ( HA_NO_AUTO_INCREMENT | HA_BINLOG_ROW_CAPABLE | HA_NO_TRANSACTIONS | HA_STATS_RECORDS_IS_EXACT);
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
		return HA_READ_NEXT | HA_READ_PREV | HA_READ_ORDER | HA_ONLY_WHOLE_INDEX;;
	}

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
	  uint max_supported_keys()          const { return 2; /* FIXME: add a constant for this */ }

	/** @brief
		unireg.cc will call this to make sure that the storage engine can handle
		the data it is about to send. Return *real* limits of your storage engine
		here; MySQL will do min(your_limits, MySQL_limits) automatically.

	    @details
		There is no need to implement ..._key_... methods if your engine doesn't
		support indexes.
	*/
	uint max_supported_key_parts()     const { return 1; }

	/** @brief
		unireg.cc will call this to make sure that the storage engine can handle
		the data it is about to send. Return *real* limits of your storage engine
		here; MySQL will do min(your_limits, MySQL_limits) automatically.

		@details
		There is no need to implement ..._key_... methods if your engine doesn't
		support indexes.
	*/
	uint max_supported_key_length()    const { return 256; }

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

  // not required, but implement these and explicitly disallow reads
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
  int external_lock(THD *thd, int lock_type);                   ///< required
  int delete_all_rows(void);
  int truncate();
  ha_rows records_in_range(uint inx, key_range *min_key, key_range *max_key);

  int delete_table(const char *from);
  int rename_table(const char * from, const char * to);
  int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info);                     ///< required

  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type);     ///< required
};

#endif // PINBA__HANDLER_H_

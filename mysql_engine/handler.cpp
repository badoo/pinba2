#include <sql/field.h>
#include <sql/structs.h>
#include <sql/handler.h>
#include <my_pthread.h>

#include <meow/str_ref_algo.hpp>
#include <meow/convert/number_from_string.hpp>

#include "pinba/globals.h"
#include "pinba/packet.h"
#include "pinba/report_by_request.h"
#include "mysql_engine/handler.h"

////////////////////////////////////////////////////////////////////////////////////////////////

pinba_mysql_ctx_t *pinba_MYSQL = NULL; // extern instance

////////////////////////////////////////////////////////////////////////////////////////////////

pinba_share_t::pinba_share_t(std::string const& table_name)
	: ref_count(0)
	, table_name(table_name)
{
	thr_lock_init(&this->lock);
}

pinba_share_t::~pinba_share_t()
{
	thr_lock_delete(&this->lock);
}

////////////////////////////////////////////////////////////////////////////////////////////////

pinba_handler_t::pinba_handler_t(handlerton *hton, TABLE_SHARE *table_arg)
	: handler(hton, table_arg)
	, log_(pinba_MYSQL->log.get())
	, share_(nullptr)
{
}

pinba_handler_t::~pinba_handler_t()
{
}

pinba_share_t* pinba_handler_t::get_share(char const *table_name, TABLE *table)
{
	LOG_DEBUG(log_, "{0}; table_name: {1}, table: {2}", __func__, table_name, table);

	std::unique_lock<std::mutex> lk_(pinba_MYSQL->lock);

	auto const it = open_tables_.find(table_name);
	if (it != open_tables_.end())
		return it->second.get();

	auto share = meow::make_unique<pinba_share_t>(table_name);
	auto *share_p = share.get(); // save raw pointer for return

	open_tables_.emplace(table_name, move(share));

	++share_p->ref_count;
	return share_p;
}

void pinba_handler_t::free_share(pinba_share_t *share)
{
	LOG_DEBUG(log_, "{0}; share: {1}, share->table_name: {2}", __func__, share, (share) ? share->table_name : "");

	std::unique_lock<std::mutex> lk_(pinba_MYSQL->lock);

	if (--share->ref_count == 0)
	{
		size_t const n_erased = open_tables_.erase(share->table_name); // share pointer is invalid from now on
		assert(n_erased == 1);
	}
}

int pinba_handler_t::create(const char *table_name, TABLE *table_arg, HA_CREATE_INFO *create_info)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	try
	{
		if (!table_arg->s)
			throw std::runtime_error("pinba table must have a comment, please see docs");

		// table comment describes the report we're going to create
		// comment structure is as follows (this should look like EBNF, but i cba being strict about this):
		//  v2/<report_type>[/<agg_window>/<key_spec>/<histogram_spec>/<filters>]
		//  report_type = [ request, timer, active, stats ]
		//
		//  -- examples --
		//  request/60,60/~host,~script,~status,+app_type/hv_range=0.0-1.5,p99,p100/min_time=0.0,max_time=1.5,~server=www1.mlan
		//  timer/60,60/~script,+app_type,group,server/hv_range=0.0-5.0,p99,p100/min_time=0.0,max_time=10.0
		//  timer/60,120/~script,group,server/p99,p100/min_time=0.0,max_time=10.0
		//
		//  -- basic stuff --
		//  key_name = <request_field> | <request_tag> | <timer_tag>
		//  request_field = '~' ( host | script | server | schema | status )
		//  request_tag   = '+' [a-zA-Z_-]{1,64}
		//  timer_tag     =     [a-zA-Z_-]{1,64}
		//  seconds_spec  = [0-9]{1,5} ( '.' [0-9]{1,6})?
		//
		//  -- histogram/percentiles --
		//  hv_spec          = 'hv_range=' <hv_min_time> '-' <hv_max_time> [ <hv_res_spec> ]
		//  hv_res_spec      = 'hv_resolution=' <hv_res_time>
		//  hv_res_time      = <seconds_spec>     // histogram resolution (aka percentile calculation precision)
		//  hv_min_time      = <seconds_spec>     // min time to record in time histogram
		//  hv_max_time      = <seconds_spec>     // max time to record in time histogram
		//  percentile_spec  = 'p' [0-9]{1,3}     // percentile to calculate for this report
		//  percentile_multi = <percentile_spec>[,<percentile_spec>[...]]
		//  histogram_spec   = 'no_percentiles' | [<hv_spec>,]<percentile_multi>
		//
		//  key_spec = <key_name>[,<key_name>[...]]
		//

		str_ref const comment = { table_arg->s->comment.str, size_t(table_arg->s->comment.length) };
		auto const parts = meow::split_ex(comment, "/");

		LOG_DEBUG(log_, "{0}; got comment: {1}", __func__, comment);

		for (unsigned i = 0; i < parts.size(); i++)
			LOG_DEBUG(log_, "{0}; parts[{1}] = '{2}'", __func__, i, parts[i]);

		if (parts.size() < 2 || parts[0] != "v2")
			throw std::runtime_error("comment should have at least 'v2/<report_type>");

		auto const report_type = parts[1];
		if (report_type == "request")
		{
			if (parts.size() != 6)
				throw std::runtime_error("'request' report options are: <aggregation_spec>/<key_spec>/<histogram_spec>/<filters>");

			auto const aggregation_spec = parts[2];
			auto const key_spec         = parts[3];
			auto const histogram_spec   = parts[4];
			auto const filters_spec     = parts[5];

			report_conf___by_request_t conf = {         // some reasonable defaults here
				.name            = table_name,
				.time_window     = 60 * d_second,       // 60s aggregation window, 1second interval
				.ts_count        = 60,
				.hv_bucket_count = 10 * 1000,           // 100us resolution for times < 1second, infinity otherwise
				.hv_bucket_d     = 100 * d_microsecond,
				.filters = {},
				.keys = {},
			};

			// aggregation window
			{
				auto const time_window_v = meow::split_ex(aggregation_spec, ",");
				if (time_window_v.size() == 0)
					throw std::runtime_error("aggregation_spec/time_window must be set");

				auto const time_window_s = time_window_v[0];
				{
					uint32_t time_window;
					if (!meow::number_from_string(&time_window, time_window_s))
					{
						throw std::runtime_error(ff::fmt_str(
							"bad seconds_spec for aggregation_spec/time_window '{0}', expected int number of seconds",
							time_window_s));
					}

					conf.time_window = time_window * d_second;
					conf.ts_count    = time_window; // i.e. ticks are always 1second wide
				}
			}

			// keys
			{
				auto const keys_v = meow::split_ex(key_spec, ",");

				for (auto const& key_s : keys_v)
				{
					if (key_s.size() == 0)
						continue; // skip empty key

					auto const kd = [&]()
					{
						if (key_s == "~host")
							return report_conf___by_request_t::key_descriptor_by_request_field("host", &packet_t::host_id);
						if (key_s == "~script")
							return report_conf___by_request_t::key_descriptor_by_request_field("script", &packet_t::script_id);
						if (key_s == "~server")
							return report_conf___by_request_t::key_descriptor_by_request_field("server", &packet_t::server_id);
						if (key_s == "~schema")
							return report_conf___by_request_t::key_descriptor_by_request_field("schema", &packet_t::schema_id);
						if (key_s == "~status")
							return report_conf___by_request_t::key_descriptor_by_request_field("status", &packet_t::status);

						if (key_s[0] == '+') // request_tag
						{
							auto const tag_name = meow::sub_str_ref(key_s, 1, key_s.size() - 1);
							auto const tag_id = pinba_MYSQL->globals->dictionary()->get_or_add(tag_name);

							return report_conf___by_request_t::key_descriptor_by_request_tag(tag_name, tag_id);
						}

						// no support for timer tags, this is request based report

						throw std::runtime_error(ff::fmt_str("key_spec/{0} not known (a timer tag in 'request' report?)", key_s));
					}();

					conf.keys.push_back(kd);
				}

				if (conf.keys.size() == 0)
					throw std::runtime_error("key_spec must be non-empty");
			}
		} // request options parsing

	}
	catch (std::exception const& e)
	{
		// FIXME: need to figure out, how to add our own error codes!

		LOG_WARN(log_, "{0}; table: {1}, error: {2}", __func__, table_name, e.what());
		DBUG_RETURN(HA_WRONG_CREATE_OPTION);
	}

	DBUG_RETURN(0);
}

/**
	@brief
	Used for opening tables. The name will be the name of the file.

	@details
	A table is opened when it needs to be opened; e.g. when a request comes in
	for a SELECT on the table (tables are not open and closed for each request,
	they are cached).

	Called from handler.cc by handler::ha_open(). The server opens all tables by
	calling ha_open() which then calls the handler specific open().

	@see
	handler::ha_open() in handler.cc
*/
int pinba_handler_t::open(const char *name, int mode, uint test_if_locked)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	this->share_ = this->get_share(name, table);
	LOG_DEBUG(log_, "{0}; got share: {1}", __func__, share_);

	thr_lock_data_init(&share_->lock, &this->lock_data, (void*)this);

	DBUG_RETURN(0);
}

/**
	@brief
	Closes a table.

	@details
	Called from sql_base.cc, sql_select.cc, and table.cc. In sql_select.cc it is
	only used to close up temporary tables or during the process where a
	temporary table is converted over to being a myisam table.

	For sql_base.cc look at close_data_tables().

	@see
	sql_base.cc, sql_select.cc and table.cc
*/
int pinba_handler_t::close(void)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	free_share(share_);

	DBUG_RETURN(0);
}

static int rows_returned_count = 0;

/**
	@brief
	rnd_init() is called when the system wants the storage engine to do a table
	scan. See the example in the introduction at the top of this file to see when
	rnd_init() is called.

	@details
	Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
	and sql_update.cc.

	@see
	filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and sql_update.cc
*/
int pinba_handler_t::rnd_init(bool scan)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	rows_returned_count = 0;

	DBUG_RETURN(0);
}

int pinba_handler_t::rnd_end()
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	DBUG_RETURN(0);
}


/**
	@brief
	This is called for each row of the table scan. When you run out of records
	you should return HA_ERR_END_OF_FILE. Fill buff up with the row information.
	The Field structure for the table is the key to getting data into buf
	in a manner that will allow the server to understand it.

	@details
	Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
	and sql_update.cc.

	@see
	filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and sql_update.cc
*/
int pinba_handler_t::rnd_next(uchar *buf)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	if (rows_returned_count++ > 1)
	{
		DBUG_RETURN(HA_ERR_END_OF_FILE);
	}

	// my_bitmap_map *old_map = dbug_tmp_use_all_columns(table, table->write_set);
	my_bitmap_map *old_map = tmp_use_all_columns(table, table->write_set);

	for (Field **field = table->field; *field; field++)
	{
		auto const field_index = (*field)->field_index;

		if (!bitmap_is_set(table->read_set, field_index))
			continue;

		// mark field as writeable to avoid assert() in ::store() calls
		// got no idea how to do this properly anyway
		bitmap_set_bit(table->write_set, field_index);

		switch(field_index)
		{
			case 0:
			{
				// std::string const data = "test-value";
				char const *data = strdup("test-value");
				(*field)->set_notnull();
				// (*field)->store(data.data(), data.size(), &my_charset_bin);
				(*field)->store(data, strlen(data), &my_charset_bin);
			}
			break;

			// case 1:
			// {
			// 	(*field)->set_notnull();
			// 	(*field)->store(10);
			// }
			// break;

		default:
			break;
		}
	}

	// dbug_tmp_restore_column_map(table->write_set, old_map);
	tmp_restore_column_map(table->write_set, old_map);

	DBUG_RETURN(0);
}

/**
	@brief
	This is like rnd_next, but you are given a position to use
	to determine the row. The position will be of the type that you stored in
	ref. You can use ha_get_ptr(pos,ref_length) to retrieve whatever key
	or position you saved when position() was called.

	@details
	Called from filesort.cc, records.cc, sql_insert.cc, sql_select.cc, and sql_update.cc.

	@see
	filesort.cc, records.cc, sql_insert.cc, sql_select.cc and sql_update.cc
*/
int pinba_handler_t::rnd_pos(uchar *buf, uchar *pos)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	DBUG_RETURN(0);
}

/**
	@brief
	position() is called after each call to rnd_next() if the data needs
	to be ordered. You can do something like the following to store
	the position:
	@code
	my_store_ptr(ref, ref_length, current_position);
	@endcode

	@details
	The server uses ref to store data. ref_length in the above case is
	the size needed to store current_position. ref is just a byte array
	that the server will maintain. If you are using offsets to mark rows, then
	current_position should be the offset. If it is a primary key like in
	BDB, then it needs to be a primary key.

	Called from filesort.cc, sql_select.cc, sql_delete.cc, and sql_update.cc.

	@see
	filesort.cc, sql_select.cc, sql_delete.cc and sql_update.cc
*/
void pinba_handler_t::position(const uchar *record)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	DBUG_VOID_RETURN;
}

/**
	@brief
	::info() is used to return information to the optimizer. See my_base.h for
	the complete description.

	@details
	Currently this table handler doesn't implement most of the fields really needed.
	SHOW also makes use of this data.

	You will probably want to have the following in your code:
	@code
	if (records < 2)
		records = 2;
	@endcode
	The reason is that the server will optimize for cases of only a single
	record. If, in a table scan, you don't know the number of records, it
	will probably be better to set records to two so you can return as many
	records as you need. Along with records, a few more variables you may wish
	to set are:
		records
		deleted
		data_file_length
		index_file_length
		delete_length
		check_time
	Take a look at the public variables in handler.h for more information.

	Called in filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
	sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
	sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_show.cc,
	sql_table.cc, sql_union.cc, and sql_update.cc.

	@see
	filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc, sql_delete.cc,
	sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc, sql_select.cc,
	sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_table.cc,
	sql_union.cc and sql_update.cc
*/
int pinba_handler_t::info(uint flag)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	DBUG_RETURN(0);
}


/*
  Grab bag of flags that are sent to the able handler every so often.
  HA_EXTRA_RESET and HA_EXTRA_RESET_STATE are the most frequently called.
  You are not required to implement any of these.
*/
int pinba_handler_t::extra(enum ha_extra_function operation)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	DBUG_RETURN(0);
}

/*
  Called by the database to lock the table. Keep in mind that this
  is an internal lock.
*/
THR_LOCK_DATA **pinba_handler_t::store_lock(THD *thd, THR_LOCK_DATA **to, enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && lock_data.type == TL_UNLOCK)
    lock_data.type = lock_type;

  *to++ = &lock_data;
  return to;
}

int pinba_handler_t::rename_table(const char *from, const char *to)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	DBUG_RETURN(0);
}

int pinba_handler_t::delete_table(const char *table)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);
	DBUG_RETURN(0);
}

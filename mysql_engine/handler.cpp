#include <meow/stopwatch.hpp>
#include <meow/str_ref_algo.hpp>
#include <meow/convert/number_from_string.hpp>

#include "pinba/globals.h"
#include "pinba/packet.h"
#include "pinba/report_by_request.h"
#include "pinba/report_by_timer.h"

#include "mysql_engine/handler.h" // make sure - that this is the first mysql-related include

#include <sql/field.h>
#include <sql/structs.h>
#include <sql/handler.h>
#include <my_pthread.h>
#include <include/mysqld_error.h>

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

struct pinba_table_kind
{
	enum type : uint32_t
	{
		stats                  = 0,
		active_reports         = 1,
		report_by_request_data = 2,
		report_by_timer_data   = 3,
	};
};
typedef pinba_table_kind::type pinba_table_kind_t;

struct pinba_table_conf_t
{
	pinba_table_kind_t         kind;

	report_conf___by_request_t by_request_conf;
	report_conf___by_timer_t   by_timer_conf;
};

static pinba_table_conf_t do_pinba_parse_table_conf(str_ref table_name, str_ref conf_string)
{
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

	logger_t *log_ = P_CTX_(logger).get();
	pinba_table_conf_t result = {};

	auto const parts = meow::split_ex(conf_string, "/");

	LOG_DEBUG(log_, "{0}; got comment: {1}", __func__, conf_string);

	for (unsigned i = 0; i < parts.size(); i++)
		LOG_DEBUG(log_, "{0}; parts[{1}] = '{2}'", __func__, i, parts[i]);

	if (parts.size() < 2 || parts[0] != "v2")
		throw std::runtime_error("comment should have at least 'v2/<report_type>");

	auto const report_type = parts[1];

	if (report_type == "stats")
	{
		result.kind = pinba_table_kind::stats;
		return result;
	}

	if (report_type == "active")
	{
		result.kind = pinba_table_kind::active_reports;
		return result;
	}

	if (report_type == "request")
	{
		if (parts.size() != 6)
			throw std::runtime_error("'request' report options are: <aggregation_spec>/<key_spec>/<histogram_spec>/<filters>");

		auto const aggregation_spec = parts[2];
		auto const key_spec         = parts[3];
		auto const histogram_spec   = parts[4];
		auto const filters_spec     = parts[5];

		result.kind = pinba_table_kind::report_by_request_data;

		auto *conf = &result.by_request_conf;
		conf->name = table_name.str();

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

				conf->time_window = time_window * d_second;
				conf->ts_count    = time_window; // i.e. ticks are always 1 second wide
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
						auto const tag_id = P_G_->dictionary()->get_or_add(tag_name);

						return report_conf___by_request_t::key_descriptor_by_request_tag(tag_name, tag_id);
					}

					// no support for timer tags, this is request based report

					throw std::runtime_error(ff::fmt_str("key_spec/{0} not known (a timer tag in 'request' report?)", key_s));
				}();

				conf->keys.push_back(kd);
			}

			if (conf->keys.size() == 0)
				throw std::runtime_error("key_spec must be non-empty");
		}

		// TODO: percentiles + filters

		return result;
	} // request options parsing

	if (report_type == "timer")
	{
		if (parts.size() != 6)
			throw std::runtime_error("'timer' report options are: <aggregation_spec>/<key_spec>/<histogram_spec>/<filters>");

		auto const aggregation_spec = parts[2];
		auto const key_spec         = parts[3];
		auto const histogram_spec   = parts[4];
		auto const filters_spec     = parts[5];

		result.kind = pinba_table_kind::report_by_timer_data;

		auto *conf = &result.by_timer_conf;
		conf->name = table_name.str();

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

				conf->time_window = time_window * d_second;
				conf->ts_count    = time_window; // i.e. ticks are always 1 second wide
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
						return report_conf___by_timer_t::key_descriptor_by_request_field("host", &packet_t::host_id);
					if (key_s == "~script")
						return report_conf___by_timer_t::key_descriptor_by_request_field("script", &packet_t::script_id);
					if (key_s == "~server")
						return report_conf___by_timer_t::key_descriptor_by_request_field("server", &packet_t::server_id);
					if (key_s == "~schema")
						return report_conf___by_timer_t::key_descriptor_by_request_field("schema", &packet_t::schema_id);
					if (key_s == "~status")
						return report_conf___by_timer_t::key_descriptor_by_request_field("status", &packet_t::status);

					if (key_s[0] == '+') // request_tag
					{
						auto const tag_name = meow::sub_str_ref(key_s, 1, key_s.size() - 1);
						auto const tag_id = P_G_->dictionary()->get_or_add(tag_name);

						return report_conf___by_timer_t::key_descriptor_by_request_tag(tag_name, tag_id);
					}

					// timer_tag
					{
						auto const tag_name = key_s;
						auto const tag_id = P_G_->dictionary()->get_or_add(tag_name);

						return report_conf___by_timer_t::key_descriptor_by_timer_tag(tag_name, tag_id);
					}

					throw std::runtime_error(ff::fmt_str("key_spec/{0} not known", key_s));
				}();

				conf->keys.push_back(kd);
			}

			if (conf->keys.size() == 0)
				throw std::runtime_error("key_spec must be non-empty");
		}

		// TODO: percentiles + filters

		return result;
	} // timer options parsing

	throw std::runtime_error(ff::fmt_str("{0}; unknown v2/<table_type> {1}", __func__, report_type));
};

static pinba_table_conf_t pinba_parse_table_conf(str_ref table_name, str_ref conf_string)
{
	auto const result = do_pinba_parse_table_conf(table_name, conf_string);
	LOG_DEBUG(P_L_, "{0}; result: {{ kind: {1} }", __func__, result.kind);
	return result;
}

struct pinba_table___base_t : public pinba_table_t
{
	virtual int  rnd_init(pinba_handler_t*, bool scan) override
	{
		return 0;
	}

	virtual int  rnd_end(pinba_handler_t*) override
	{
		return 0;
	}

	virtual int  rnd_next(pinba_handler_t*, uchar *buf) override
	{
		return HA_ERR_END_OF_FILE;
	}

	virtual int  rnd_pos(pinba_handler_t*, uchar *buf, uchar *pos) const override
	{
		return 0;
	}

	virtual void position(pinba_handler_t*, const uchar *record) const override
	{
		return;
	}

	virtual int  info(pinba_handler_t*, uint) const override
	{
		return 0;
	}

	virtual int  extra(pinba_handler_t*, enum ha_extra_function operation) const override
	{
		return 0;
	}
};

struct pinba_table___stats_t : public pinba_table___base_t
{
	bool output_done;

	virtual int  rnd_init(pinba_handler_t*, bool scan) override
	{
		output_done = false;
		return 0;
	}

	virtual int  rnd_next(pinba_handler_t *handler, uchar *buf) override
	{
		if (output_done)
			return HA_ERR_END_OF_FILE;

		auto const *stats = P_G_->stats();
		auto *table = handler->current_table();

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
					auto const uptime = os_unix::clock_monotonic_now() - stats->start_tv;
					(*field)->set_notnull();
					(*field)->store(timeval_to_double(uptime));
				}
				break;

				case 1:
					(*field)->set_notnull();
					(*field)->store(stats->udp.recv_total);
				break;

				case 2:
					(*field)->set_notnull();
					(*field)->store(stats->udp.recv_nonblocking);
				break;

				case 3:
					(*field)->set_notnull();
					(*field)->store(stats->udp.recv_eagain);
				break;

				case 4:
					(*field)->set_notnull();
					(*field)->store(stats->udp.packets_received);
				break;

			default:
				break;
			}
		}

		output_done = true;
		return 0;
	}
};


struct pinba_table___active_reports_t : public pinba_table___base_t
{
};

struct pinba_table___report_snapshot_t : public pinba_table___base_t
{
	report_snapshot_ptr            snapshot_;
	report_snapshot_t::position_t  pos_;

	virtual int rnd_init(pinba_handler_t *handler, bool scan) override
	{
		auto share = handler->current_share();

		LOG_DEBUG(P_L_, "{0}; getting snapshot for {1}", __func__, share->table_name);

		try
		{
			snapshot_ = P_E_->get_report_snapshot(share->table_name);
		}
		catch (std::exception const& e)
		{
			LOG_WARN(P_L_, "{0}; internal error: {1}", __func__, e.what());
			my_printf_error(ER_INTERNAL_ERROR, "[pinba] %s", MYF(0), e.what());
			return HA_ERR_INTERNAL_ERROR;
		}

		{
			meow::stopwatch_t sw;
			snapshot_->prepare();

			LOG_DEBUG(P_L_, "{0}; report_snapshot for: {1}, prepare took {2} seconds", __func__, share->table_name, sw.stamp());
		}

		pos_ = snapshot_->pos_first();

		return 0;
	}

	virtual int rnd_end(pinba_handler_t*) override
	{
		snapshot_.reset();
		return 0;
	}

	virtual int rnd_next(pinba_handler_t *handler, uchar *buf) override
	{
		auto *table = handler->current_table();

		assert(snapshot_);

		if (snapshot_->pos_equal(pos_, snapshot_->pos_last()))
			return HA_ERR_END_OF_FILE;

		auto const *rinfo = snapshot_->report_info();
		auto const key = snapshot_->get_key_str(pos_);

		unsigned const key_size = rinfo->n_key_parts;

		for (Field **field = table->field; *field; field++)
		{
			unsigned const field_index = (*field)->field_index;
			unsigned       findex      = field_index;

			if (!bitmap_is_set(table->read_set, field_index))
				continue;

			// mark field as writeable to avoid assert() in ::store() calls
			// got no idea how to do this properly anyway
			bitmap_set_bit(table->write_set, field_index);

			// key comes first
			{
				if (findex < key_size)
				{
					(*field)->set_notnull();
					(*field)->store(key[findex].begin(), key[findex].c_length(), &my_charset_bin);
					continue;
				}
				findex -= key_size;
			}

			// row data comes next
			if (REPORT_KIND__BY_REQUEST_DATA == rinfo->kind)
			{
				static unsigned const n_data_fields = 6;
				if (findex < n_data_fields)
				{
					auto const *row = reinterpret_cast<report_row_data___by_request_t*>(snapshot_->get_data(pos_));

					switch (findex)
					{
					case 0:
						(*field)->set_notnull();
						(*field)->store(row->req_count);
					break;

					case 1:
						(*field)->set_notnull();
						(*field)->store(duration_seconds_as_double(row->time_total));
					break;

					case 2:
						(*field)->set_notnull();
						(*field)->store(duration_seconds_as_double(row->ru_utime));
					break;

					case 3:
						(*field)->set_notnull();
						(*field)->store(duration_seconds_as_double(row->ru_stime));
					break;

					case 4:
						(*field)->set_notnull();
						(*field)->store(row->traffic_kb);
					break;

					case 5:
						(*field)->set_notnull();
						(*field)->store(row->mem_usage);
					break;

					default:
						break;
					}

					findex -= n_data_fields;
				}
			}
			else if (REPORT_KIND__BY_TIMER_DATA == rinfo->kind)
			{
				static unsigned const n_data_fields = 5;
				if (findex < n_data_fields)
				{
					auto const *row = reinterpret_cast<report_row_data___by_timer_t*>(snapshot_->get_data(pos_));

					switch (findex)
					{
					case 0:
						(*field)->set_notnull();
						(*field)->store(row->req_count);
					break;

					case 1:
						(*field)->set_notnull();
						(*field)->store(row->hit_count);
					break;

					case 2:
						(*field)->set_notnull();
						(*field)->store(duration_seconds_as_double(row->time_total));
					break;

					case 3:
						(*field)->set_notnull();
						(*field)->store(duration_seconds_as_double(row->ru_utime));
					break;

					case 4:
						(*field)->set_notnull();
						(*field)->store(duration_seconds_as_double(row->ru_stime));
					break;

					default:
						break;
					}

					findex -= n_data_fields;
				}
			}
			else
			{
				LOG_ERROR(P_L_, "{0}; unknown report snapshot data_kind: {1}", __func__, rinfo->kind);
				// XXX: should we assert here or something?
			}

			// TODO:
			unsigned const n_percentile_fields = 0;
			if (findex < n_percentile_fields)
			{
				findex -= n_percentile_fields;
			}

		} // loop over all fields

		// FIXME: this should be the only return in this function (for now)
		//        should rebuild this with defer-like construct
		pos_ = snapshot_->pos_next(pos_);
		return 0;
	}
};


pinba_table_ptr pinba_table_create(pinba_share_t *share, pinba_table_conf_t const& conf)
{
	switch (conf.kind)
	{
	case pinba_table_kind::stats:
		return meow::make_unique<pinba_table___stats_t>();
	case pinba_table_kind::active_reports:
		return meow::make_unique<pinba_table___active_reports_t>();
	case pinba_table_kind::report_by_request_data:
		return meow::make_unique<pinba_table___report_snapshot_t>();
	case pinba_table_kind::report_by_timer_data:
		return meow::make_unique<pinba_table___report_snapshot_t>();

	default:
		assert(!"must not be reached");
		return {};
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////

pinba_handler_t::pinba_handler_t(handlerton *hton, TABLE_SHARE *table_arg)
	: handler(hton, table_arg)
	, share_(nullptr)
	, log_(P_CTX_(logger).get())
{
}

pinba_handler_t::~pinba_handler_t()
{
}

pinba_share_t* pinba_handler_t::get_share(char const *table_name, TABLE *table)
{
	LOG_DEBUG(log_, "{0}; table_name: {1}, table: {2}", __func__, table_name, table);

	std::unique_lock<std::mutex> lk_(P_CTX_(lock));

	auto& open_shares = P_CTX_(open_shares);

	auto const it = open_shares.find(table_name);
	if (it != open_shares.end())
		return it->second.get();

	// share not found, create new one

	auto share = meow::make_unique<pinba_share_t>(table_name);
	auto *share_p = share.get(); // save raw pointer for return

	share->table_conf = nullptr;
	share->report_active = false;

	open_shares.emplace(table_name, move(share));

	++share_p->ref_count;
	return share_p;
}

void pinba_handler_t::free_share(pinba_share_t *share)
{
	LOG_DEBUG(log_, "{0}; share: {1}, share->table_name: {2}", __func__, share, (share) ? share->table_name : "");

	std::unique_lock<std::mutex> lk_(P_CTX_(lock));

	auto& open_shares = P_CTX_(open_shares);

	if (--share->ref_count == 0)
	{
		delete share->table_conf; // FIXME

		size_t const n_erased = open_shares.erase(share->table_name); // share pointer is invalid from now on
		assert(n_erased == 1);
	}
}

TABLE* pinba_handler_t::current_table() const
{
	return this->table;
}

pinba_share_t* pinba_handler_t::current_share() const
{
	return share_;
}

int pinba_handler_t::create(const char *table_name, TABLE *table_arg, HA_CREATE_INFO *create_info)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	try
	{
		if (!table_arg->s)
			throw std::runtime_error("pinba table must have a comment, please see docs");

		str_ref const comment = { table_arg->s->comment.str, size_t(table_arg->s->comment.length) };
		auto const table_conf = pinba_parse_table_conf(table_name, comment); // throws

		// since we're in create mode, not going to start report just yet
		// but will just save it, and start when first ::open() comes in (or maybe even select aka. rnd_init()?)
		// not checking uniqueness here, since mysql does it for us
		auto share = this->get_share(table_name, table_arg);

		std::unique_lock<std::mutex> lk_(P_CTX_(lock));

		share->table_conf = new pinba_table_conf_t(table_conf); // FIXME
		share->report_active = false;

		pinba_table_ = pinba_table_create(share, table_conf);
	}
	catch (std::exception const& e)
	{
		my_printf_error(ER_CANT_CREATE_TABLE, "[pinba] %s", MYF(0), e.what());
		DBUG_RETURN(HA_WRONG_CREATE_OPTION);
	}

	// DBUG_RETURN(HA_ERR_UNSUPPORTED); // FIXME: change this to 0 when all code is implemented
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
int pinba_handler_t::open(const char *table_name, int mode, uint test_if_locked)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	// open will always come for either
	//  - already active report (we've got 'share' open and stored in 'open_shares')
	//  - table just created and not yet active, i.e. got parsed config and created report
	//    but pinba engine is not aware of that report yet

	this->share_ = this->get_share(table_name, table);
	LOG_DEBUG(log_, "{0}; got share: {1}", __func__, share_);

	// lock to use share, too coarse, but whatever
	std::unique_lock<std::mutex> lk_(P_CTX_(lock));

	try
	{
		// config NOT parsed yet (i.e. existing table after having been closed)
		if (!share_->table_conf)
		{
			if (!current_table()->s)
				throw std::runtime_error("pinba table must have a comment, please see docs");

			str_ref const comment = { current_table()->s->comment.str, size_t(current_table()->s->comment.length) };
			auto const table_conf = pinba_parse_table_conf(table_name, comment); // throws

			share_->table_conf = new pinba_table_conf_t(table_conf); // FIXME
		}

		// got config, but report has not been activated yet (i.e. right after create table)
		// going to construct relevant report here, but still not going to activate (rnd_init() shall do that)
		if (!share_->report_active)
		{
			if (share_->table_conf->kind == pinba_table_kind::report_by_request_data)
			{
				share_->report = meow::make_unique<report___by_request_t>(P_G_, share_->table_conf->by_request_conf);
			}
			else if (share_->table_conf->kind == pinba_table_kind::report_by_timer_data)
			{
				share_->report = meow::make_unique<report___by_timer_t>(P_G_, share_->table_conf->by_timer_conf);
			}
		}

		pinba_table_ = pinba_table_create(share_, *share_->table_conf);
	}
	catch (std::exception const& e)
	{
		// this MUST not happen in fact, since all tables should have been created
		// and we do parse comments on create()
		// might happen when pinba versions are upgraded or something, i guess?

		LOG_ERROR(log_, "{0}; report {1}, error: {2}", __func__, table_name, e.what());

		my_printf_error(ER_CANT_CREATE_TABLE, "[pinba] THIS IS A BUG, report! %s", MYF(0), e.what());
		DBUG_RETURN(HA_WRONG_CREATE_OPTION);
	}

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
	pinba_table_.reset();

	DBUG_RETURN(0);
}

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

	std::unique_lock<std::mutex> lk_(P_CTX_(lock));

	try
	{
		// report not active - might need to activate
		if (!share_->report_active && share_->report)
		{
			pinba_error_t err = P_E_->add_report(move(share_->report));
			if (err)
				throw std::runtime_error(ff::fmt_str("can't activate report: {0}", err.what()));

			share_->report_active = true;
		}
	}
	catch (std::exception const& e)
	{
		LOG_ERROR(log_, "{0}; report {1}, error: {2}", __func__, share_->table_name, e.what());

		my_printf_error(ER_CANT_CREATE_TABLE, "[pinba] THIS IS A BUG, report! %s", MYF(0), e.what());
		DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
	}

	int const r = pinba_table_->rnd_init(this, scan);

	DBUG_RETURN(r);
}

int pinba_handler_t::rnd_end()
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	int const r = pinba_table_->rnd_end(this);

	DBUG_RETURN(r);
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

	int const r = pinba_table_->rnd_next(this, buf);

	DBUG_RETURN(r);
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

	int const r = pinba_table_->rnd_pos(this, buf, pos);

	DBUG_RETURN(r);
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

	pinba_table_->position(this, record);

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

	int const r = pinba_table_->info(this, flag);

	DBUG_RETURN(r);
}


/*
  Grab bag of flags that are sent to the able handler every so often.
  HA_EXTRA_RESET and HA_EXTRA_RESET_STATE are the most frequently called.
  You are not required to implement any of these.
*/
int pinba_handler_t::extra(enum ha_extra_function operation)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	int const r = pinba_table_->extra(this, operation);

	DBUG_RETURN(r);
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

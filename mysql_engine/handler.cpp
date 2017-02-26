#include <meow/stopwatch.hpp>
#include <meow/str_ref_algo.hpp>
#include <meow/convert/number_from_string.hpp>

#include "pinba/globals.h"
#include "pinba/packet.h"
#include "pinba/report_by_request.h"
#include "pinba/report_by_timer.h"

#include "mysql_engine/handler.h" // make sure - that this is the first mysql-related include

#ifdef PINBA_USE_MYSQL_SOURCE
#include <sql/field.h> // <mysql/private/field.h>
#include <sql/handler.h> // <mysql/private/handler.h>
#include <include/mysqld_error.h> // <mysql/mysqld_error.h>
#else
#include <mysql/private/field.h>
#include <mysql/private/handler.h>
#include <mysql/mysqld_error.h>
#endif // PINBA_USE_MYSQL_SOURCE

////////////////////////////////////////////////////////////////////////////////////////////////

pinba_share_t::pinba_share_t(std::string const& table_name)
	: mysql_name(table_name)
	, report_active(false)
	, report_needs_engine(false)
{
	thr_lock_init(&this->lock);
}

pinba_share_t::~pinba_share_t()
{
	thr_lock_delete(&this->lock);
}

pinba_share_ptr pinba_share_get_or_create(char const *table_name)
{
	LOG_DEBUG(P_L_, "{0}; table_name: {1}", __func__, table_name);

	std::unique_lock<std::mutex> lk_(P_CTX_->lock);

	auto& open_shares = P_CTX_->open_shares;

	auto const it = open_shares.find(table_name);
	if (it != open_shares.end())
		return it->second;

	// share not found, create new one

	auto share = meow::make_intrusive<pinba_share_t>(table_name);
	open_shares.emplace(share->mysql_name, share);

	return share;
}

////////////////////////////////////////////////////////////////////////////////////////////////

struct pinba_view_conf_t
{
	pinba_view_kind_t kind;
	report_conf___by_request_t by_request_conf;
	report_conf___by_timer_t   by_timer_conf;

	std::vector<double>        percentiles;
};

static pinba_view_conf_ptr do_pinba_parse_view_conf(str_ref table_name, str_ref conf_string)
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

	logger_t *log_ = P_L_;
	auto result = meow::make_unique<pinba_view_conf_t>();
	// memset(result.get(), 0, sizeof(*result)); // FIXME: uh-oh

	auto const parts = meow::split_ex(conf_string, "/");

	LOG_DEBUG(log_, "{0}; got comment: {1}", __func__, conf_string);

	for (unsigned i = 0; i < parts.size(); i++)
		LOG_DEBUG(log_, "{0}; parts[{1}] = '{2}'", __func__, i, parts[i]);

	if (parts.size() < 2 || parts[0] != "v2")
		throw std::runtime_error("comment should have at least 'v2/<report_type>");

	auto const report_type = parts[1];

	if (report_type == "stats")
	{
		result->kind = pinba_view_kind::stats;
		return move(result);
	}

	if (report_type == "active")
	{
		result->kind = pinba_view_kind::active_reports;
		return move(result);
	}

	if (report_type == "request")
	{
		if (parts.size() != 6)
			throw std::runtime_error("'request' report options are: <aggregation_spec>/<key_spec>/<histogram_spec>/<filters>");

		auto const aggregation_spec = parts[2];
		auto const key_spec         = parts[3];
		auto const histogram_spec   = parts[4];
		auto const filters_spec     = parts[5];

		result->kind = pinba_view_kind::report_by_request_data;

		auto *conf = &result->by_request_conf;
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
						auto const tag_name = meow::sub_str_ref(key_s, 1, key_s.size());
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

		// percentiles
		[&]()
		{
			if (histogram_spec == "no_percentiles")
				return;

			auto const pct_v = meow::split_ex(histogram_spec, ",");

			for (auto const& pct_s : pct_v)
			{
				if (meow::prefix_compare(pct_s, "hv=")) // 3 chars
				{
					auto const hv_range_s = meow::sub_str_ref(pct_s, 3, pct_s.size());
					auto const hv_values_v = meow::split_ex(hv_range_s, ":");

					if (hv_values_v.size() != 3)
						break; // break the loop, will immediately stumble on error

					LOG_DEBUG(P_L_, "hv_values_v = {{ {0}, {1}, {2} }", hv_values_v[0], hv_values_v[1], hv_values_v[2]);

					// hv=<hv_lower_time_ms>:<hv_upper_time_ms>:<hv_bucket_count>
					uint32_t hv_lower_ms;
					if (!meow::number_from_string(&hv_lower_ms, hv_values_v[0]))
						throw std::runtime_error(ff::fmt_str("histogram_spec: can't parse hv_lower_ms from '{0}'", pct_s));

					uint32_t hv_upper_ms;
					if (!meow::number_from_string(&hv_upper_ms, hv_values_v[1]))
						throw std::runtime_error(ff::fmt_str("histogram_spec: can't parse hv_upper_ms from '{0}'", pct_s));

					uint32_t hv_bucket_count;
					if (!meow::number_from_string(&hv_bucket_count, hv_values_v[2]))
						throw std::runtime_error(ff::fmt_str("histogram_spec: can't parse hv_bucket_count from '{0}'", pct_s));

					if (hv_upper_ms <= hv_lower_ms)
						throw std::runtime_error(ff::fmt_str("histogram_spec: hv_upper_ms must be >= hv_lower_ms, in '{0}'", pct_s));

					if (hv_bucket_count == 0)
						throw std::runtime_error(ff::fmt_str("histogram_spec: hv_bucket_count must be >= 0, in '{0}'", pct_s));

					conf->hv_bucket_count = hv_bucket_count;
					conf->hv_bucket_d     = (hv_upper_ms - hv_lower_ms) * d_millisecond / hv_bucket_count;
				}
				else if (meow::prefix_compare(pct_s, "p"))
				{
					auto const percentile_s = meow::sub_str_ref(pct_s, 1, pct_s.size());

					uint32_t pv;
					if (!meow::number_from_string(&pv, percentile_s))
						throw std::runtime_error(ff::fmt_str("histogram_spec: can't parse integer from '{0}'", pct_s));

					result->percentiles.push_back(pv);

					LOG_DEBUG(P_L_, "percentile added: {0}, total: {1}", pv, result->percentiles.size());
				}
				else
				{
					throw std::runtime_error(ff::fmt_str("histogram_spec: unknown part '{0}'", pct_s));
				}
			}

			if (!conf->hv_bucket_count || !conf->hv_bucket_d.nsec)
				throw std::runtime_error(ff::fmt_str("histogram_spec: needs to have hv=<time_lower>:<time_upper>:<n_buckets>, got '{0}'", histogram_spec));
		}();

		// TODO: percentiles + filters

		return move(result);
	} // request options parsing

	if (report_type == "timer")
	{
		if (parts.size() != 6)
			throw std::runtime_error("'timer' report options are: <aggregation_spec>/<key_spec>/<histogram_spec>/<filters>");

		auto const aggregation_spec = parts[2];
		auto const key_spec         = parts[3];
		auto const histogram_spec   = parts[4];
		auto const filters_spec     = parts[5];

		result->kind = pinba_view_kind::report_by_timer_data;

		auto *conf = &result->by_timer_conf;
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
						auto const tag_name = meow::sub_str_ref(key_s, 1, key_s.size());
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

		return move(result);
	} // timer options parsing

	throw std::runtime_error(ff::fmt_str("{0}; unknown v2/<table_type> {1}", __func__, report_type));
};

struct pinba_view___base_t : public pinba_view_t
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

struct pinba_view___stats_t : public pinba_view___base_t
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

				case 5:
					(*field)->set_notnull();
					(*field)->store(stats->udp.packet_decode_err);
				break;

				case 6:
					(*field)->set_notnull();
					(*field)->store(stats->udp.batch_send_total);
				break;

				case 7:
					(*field)->set_notnull();
					(*field)->store(stats->udp.batch_send_err);
				break;

			default:
				break;
			}
		}

		output_done = true;
		return 0;
	}
};


struct pinba_view___active_reports_t : public pinba_view___base_t
{
	pinba_open_shares_t           shares_;
	pinba_open_shares_t::iterator pos_;

	virtual int rnd_init(pinba_handler_t *handler, bool scan) override
	{
		// TODO:
		// mysql code comments say that this fuction might get called twice in a row
		// i have no idea why or when
		// so just hack around the fact for now
		if (!shares_.empty())
			return 0;

		std::lock_guard<std::mutex> lk_(P_CTX_->lock);

		// take hashtable copy (but individual shares can still change! and need locking)
		shares_ = P_CTX_->open_shares;
		pos_    = shares_.begin();

		return 0;
	}

	virtual int rnd_end(pinba_handler_t*) override
	{
		shares_.clear();
		return 0;
	}

	virtual int rnd_next(pinba_handler_t *handler, uchar *buf) override
	{
		if (pos_ == shares_.end())
			return HA_ERR_END_OF_FILE;

		auto const *share = pos_->second.get();
		auto const *table = handler->current_table();

		// remember to lock, might be too coarse, but whatever
		std::lock_guard<std::mutex> lk_(P_CTX_->lock);

		for (Field **field = table->field; *field; field++)
		{
			unsigned const field_index = (*field)->field_index;

			if (!bitmap_is_set(table->read_set, field_index))
				continue;

			// mark field as writeable to avoid assert() in ::store() calls
			// got no idea how to do this properly anyway
			bitmap_set_bit(table->write_set, field_index);

			switch (field_index)
			{
				case 0:
					(*field)->set_notnull();
					(*field)->store(share->mysql_name.c_str(), share->mysql_name.length(), &my_charset_bin);
				break;

				case 1:
					(*field)->set_notnull();
					(*field)->store(share->report_name.c_str(), share->report_name.length(), &my_charset_bin);
				break;

				case 2:
					(*field)->set_notnull();
					(*field)->store(share->view_conf->kind);
				break;

				case 3:
					(*field)->set_notnull();
					(*field)->store(share->report_needs_engine);
				break;

				case 4:
					(*field)->set_notnull();
					(*field)->store(share->report_active);
				break;

				// TODO, packet rate, stats, etc.
			}
		} // field for

		pos_ = std::next(pos_);

		return 0;
	}
};

struct pinba_view___report_snapshot_t : public pinba_view___base_t
{
	report_snapshot_ptr            snapshot_;
	report_snapshot_t::position_t  pos_;

	virtual int rnd_init(pinba_handler_t *handler, bool scan) override
	{
		auto share = handler->current_share();

		LOG_DEBUG(P_L_, "{0}; getting snapshot for t: {1}, r: {2}", __func__, share->mysql_name, share->report_name);

		try
		{
			snapshot_ = P_E_->get_report_snapshot(share->report_name);
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

			LOG_DEBUG(P_L_, "{0}; report_snapshot for: {1}, prepare took {2} seconds", __func__, share->mysql_name, sw.stamp());
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
		auto share  = handler->current_share();
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

					continue;
				}
				findex -= n_data_fields;
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

					continue;
				}
				findex -= n_data_fields;
			}
			else
			{
				LOG_ERROR(P_L_, "{0}; unknown report snapshot data_kind: {1}", __func__, rinfo->kind);
				// XXX: should we assert here or something?
			}

			// TODO:
			auto const& pct = share->view_conf->percentiles;
			unsigned const n_percentile_fields = pct.size();
			if (findex < n_percentile_fields)
			{
				auto const *hv = snapshot_->get_histogram(pos_);
				assert(hv != NULL);

				auto const pct_d = get_percentile(*hv, { rinfo->hv_bucket_count, rinfo->hv_bucket_d }, pct[findex]);

				(*field)->set_notnull();
				(*field)->store(duration_seconds_as_double(pct_d));

				continue;
			}
			findex -= n_percentile_fields;

		} // loop over all fields

		// FIXME: this should be the only return in this function (for now)
		//        should rebuild this with defer-like construct
		pos_ = snapshot_->pos_next(pos_);
		return 0;
	}
};


pinba_view_conf_ptr pinba_parse_view_conf(str_ref table_name, str_ref conf_string)
{
	auto result = do_pinba_parse_view_conf(table_name, conf_string);
	LOG_DEBUG(P_L_, "{0}; result: {{ kind: {1} }", __func__, result->kind);
	return move(result);
}


pinba_view_ptr pinba_view_create(pinba_view_conf_t const& conf)
{
	switch (conf.kind)
	{
		case pinba_view_kind::stats:
			return meow::make_unique<pinba_view___stats_t>();
		case pinba_view_kind::active_reports:
			return meow::make_unique<pinba_view___active_reports_t>();
		case pinba_view_kind::report_by_request_data:
			return meow::make_unique<pinba_view___report_snapshot_t>();
		case pinba_view_kind::report_by_timer_data:
			return meow::make_unique<pinba_view___report_snapshot_t>();

		default:
			assert(!"must not be reached");
			return {};
	}
}

pinba_report_ptr pinba_report_create(pinba_view_conf_t const& conf)
{
	switch (conf.kind)
	{
		case pinba_view_kind::stats:
			return {};
		case pinba_view_kind::active_reports:
			return {};
		case pinba_view_kind::report_by_request_data:
			return meow::make_unique<report___by_request_t>(P_G_, conf.by_request_conf);
		case pinba_view_kind::report_by_timer_data:
			return meow::make_unique<report___by_timer_t>(P_G_, conf.by_timer_conf);

		default:
			assert(!"must not be reached");
			return {};
	}
}

void share_init_with_view_comment_locked(pinba_share_ptr& share, str_ref table_comment)
{
	assert(!share->view_conf);
	assert(!share->report);
	assert(!share->report_active);

	share->view_conf   = pinba_parse_view_conf(share->mysql_name, table_comment); // throws
	share->report      = pinba_report_create(*share->view_conf);

	if (share->report)
	{
		share->report_name         = share->mysql_name;
		share->report_active       = false;
		share->report_needs_engine = true;
	}
	else
	{
		// log display purposes only
		share->report_name = ff::fmt_str("<virtual table: {0}>", share->view_conf->kind);
		share->report_needs_engine = false;
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////

pinba_handler_t::pinba_handler_t(handlerton *hton, TABLE_SHARE *table_arg)
	: handler(hton, table_arg)
	, share_(nullptr)
	, log_(P_L_)
{
}

pinba_handler_t::~pinba_handler_t()
{
}

TABLE* pinba_handler_t::current_table() const
{
	return this->table;
}

pinba_share_ptr pinba_handler_t::current_share() const
{
	return this->share_;
}

int pinba_handler_t::create(const char *table_name, TABLE *table_arg, HA_CREATE_INFO *create_info)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	try
	{
		if (!table_arg->s)
			throw std::runtime_error("pinba table must have a comment, please see docs");

		auto share = pinba_share_get_or_create(table_name);

		std::unique_lock<std::mutex> lk_(P_CTX_->lock);

		str_ref const comment = { table_arg->s->comment.str, size_t(table_arg->s->comment.length) };
		share_init_with_view_comment_locked(share, comment);
	}
	catch (std::exception const& e)
	{
		my_printf_error(ER_CANT_CREATE_TABLE, "[pinba] %s", MYF(0), e.what());
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
int pinba_handler_t::open(const char *table_name, int mode, uint test_if_locked)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	// open will always come for either
	//  - already active report (we've got 'share' open and stored in 'open_shares')
	//  - table just created and not yet active, i.e. got parsed config and created report
	//    but pinba engine is not aware of that report yet

	this->share_ = pinba_share_get_or_create(table_name);

	// lock to use share, too coarse, but whatever
	std::unique_lock<std::mutex> lk_(P_CTX_->lock);

	try
	{
		// config NOT parsed yet (i.e. existing table after restart)
		if (!share_->view_conf)
		{
			if (!current_table()->s)
				throw std::runtime_error("pinba table must have a comment, please see docs");

			str_ref const comment = { current_table()->s->comment.str, size_t(current_table()->s->comment.length) };
			share_init_with_view_comment_locked(share_, comment);
		}

		pinba_view_ = pinba_view_create(*share_->view_conf);
	}
	catch (std::exception const& e)
	{
		// this MUST not happen in fact, since all tables should have been created
		// and we do parse comments on create()
		// might happen when pinba versions are upgraded or something, i guess?

		LOG_ERROR(log_, "{0}; table: {1}, error: {2}", __func__, share_->mysql_name, e.what());

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

	share_.reset();
	pinba_view_.reset();

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

	{
		std::unique_lock<std::mutex> lk_(P_CTX_->lock);

		try
		{
			// report not active - might need to activate
			if (share_->report_needs_engine && !share_->report_active)
			{
				assert(share_->report);

				pinba_error_t err = P_E_->add_report(move(share_->report));
				if (err)
					throw std::runtime_error(ff::fmt_str("can't activate report: {0}", err.what()));

				share_->report_active = true;
			}
		}
		catch (std::exception const& e)
		{
			LOG_ERROR(log_, "{0}; table: {1}, error: {2}", __func__, share_->mysql_name, e.what());

			my_printf_error(ER_CANT_CREATE_TABLE, "[pinba] THIS IS A BUG, report! %s", MYF(0), e.what());
			DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
		}
	} // P_CTX_->lock released here

	int const r = pinba_view_->rnd_init(this, scan);

	DBUG_RETURN(r);
}

int pinba_handler_t::rnd_end()
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	int const r = pinba_view_->rnd_end(this);

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

	int const r = pinba_view_->rnd_next(this, buf);

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

	int const r = pinba_view_->rnd_pos(this, buf, pos);

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

	pinba_view_->position(this, record);

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

	int const r = pinba_view_->info(this, flag);

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

	int const r = pinba_view_->extra(this, operation);

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

	std::lock_guard<std::mutex> lk_(P_CTX_->lock);

	auto& open_shares = P_CTX_->open_shares;

	auto it = open_shares.find(from);
	if (it == open_shares.end())
	{
		// no idea why this might happen, just try and be nice
		LOG_ERROR(log_, "{0}; can't find table to rename from: '{1}' (weird mysql shenanigans?)", __func__, from);
		DBUG_RETURN(0);
	}

	auto share = it->second;
	open_shares.erase(it);

	share->mysql_name = to;
	open_shares.emplace(share->mysql_name, share);

	LOG_DEBUG(log_, "{0}; renamed mysql table '{1}' -> '{2}', internal report_name: '{3}'",
		__func__, from, to, share->report_name);

	DBUG_RETURN(0);
}

int pinba_handler_t::delete_table(const char *table_name)
{
	DBUG_ENTER(__PRETTY_FUNCTION__);

	std::lock_guard<std::mutex> lk_(P_CTX_->lock);

	auto& open_shares = P_CTX_->open_shares;

	auto const it = open_shares.find(table_name);
	if (it == open_shares.end())
	{
		// no idea why this might happen, just try and be nice
		LOG_ERROR(log_, "{0}; can't find table to delete: '{1}' (weird mysql shenanigans?)", __func__, table_name);
		DBUG_RETURN(0);
	}

	auto share = it->second;
	open_shares.erase(it);

	// skip if it's just a virtual table or report that hasn't been activated yet
	if (share->report_needs_engine && share->report_active)
	{
		auto const err = P_E_->delete_report(share->report_name);
		if (err)
		{
			LOG_ERROR(log_, "{0}; table: '{1}', report: '{2}'; error: {3}",
				__func__, share->mysql_name, share->report_name, err.what());

			DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
		}
	}

	LOG_DEBUG(log_, "{0}; dropped table '{1}', report '{2}'", __func__, share->mysql_name, share->report_name);

	DBUG_RETURN(0);
}

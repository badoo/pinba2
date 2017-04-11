#include "mysql_engine/handler.h"
#include "mysql_engine/plugin.h"

#include "pinba/globals.h"
#include "pinba/histogram.h"
// #include "pinba/packet.h"
#include "pinba/report_by_request.h"
#include "pinba/report_by_timer.h"
#include "pinba/report_by_packet.h"

// FIXME: some of these headers were moved to pinba_view, reassess,
//        or maybe move vews back here! since they're sql related anyway
#ifdef PINBA_USE_MYSQL_SOURCE
#include <sql/field.h> // <mysql/private/field.h>
#include <sql/handler.h> // <mysql/private/handler.h>
#include <include/mysqld_error.h> // <mysql/mysqld_error.h>
#else
#include <mysql/private/field.h>
#include <mysql/private/handler.h>
#include <mysql/mysqld_error.h>
#endif // PINBA_USE_MYSQL_SOURCE

#include <meow/defer.hpp>
#include <meow/stopwatch.hpp>

////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////

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

		auto const repacker_threads_tmp = [&]()
		{
			std::lock_guard<std::mutex> lk_(stats->mtx);
			return stats->repacker_threads;
		}();

		repacker_stats_t repacker_stats = [&]()
		{
			repacker_stats_t result = {};
			for (auto const& rst : repacker_threads_tmp)
			{
				result.ru_utime += rst.ru_utime;
				result.ru_stime += rst.ru_stime;
			}
			return result;
		}();

		// mark all fields as writeable to avoid assert() in ::store() calls
		// got no idea how to do this properly anyway
		auto *old_map = tmp_use_all_columns(table, table->write_set);
		MEOW_DEFER(
			tmp_restore_column_map(table->write_set, old_map);
		);

		for (Field **field = table->field; *field; field++)
		{
			auto const field_index = (*field)->field_index;

			if (!bitmap_is_set(table->read_set, field_index))
				continue;

			// mark field as writeable to avoid assert() in ::store() calls
			// got no idea how to do this properly anyway
			// bitmap_set_bit(table->write_set, field_index);

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
					(*field)->store(stats->udp.poll_total);
				break;

				case 2:
					(*field)->set_notnull();
					(*field)->store(stats->udp.recv_total);
				break;

				case 3:
					(*field)->set_notnull();
					(*field)->store(stats->udp.recv_eagain);
				break;

				case 4:
					(*field)->set_notnull();
					(*field)->store(stats->udp.recv_bytes);
				break;

				case 5:
					(*field)->set_notnull();
					(*field)->store(stats->udp.recv_packets);
				break;

				case 6:
					(*field)->set_notnull();
					(*field)->store(stats->udp.packet_decode_err);
				break;

				case 7:
					(*field)->set_notnull();
					(*field)->store(stats->udp.batch_send_total);
				break;

				case 8:
					(*field)->set_notnull();
					(*field)->store(stats->udp.batch_send_err);
				break;

				case 9:
					(*field)->set_notnull();
					(*field)->store(timeval_to_double(repacker_stats.ru_utime));
				break;

				case 10:
					(*field)->set_notnull();
					(*field)->store(timeval_to_double(repacker_stats.ru_stime));
				break;

			default:
				break;
			}
		}

		output_done = true;
		return 0;
	}
};

////////////////////////////////////////////////////////////////////////////////////////////////

struct pinba_view___active_reports_t : public pinba_view___base_t
{
	struct view_row_t
	{
		pinba_share_ptr   share;
		std::string       report_name; // copied from share, to reduce locking
		report_state_ptr  report_state;
	};
	// using view_row_ptr = std::unique_ptr<view_row_t>;
	using view_t       = std::vector<view_row_t>;

	view_t            data_;
	view_t::iterator  pos_;

	// pinba_open_shares_t           shares_;
	// pinba_open_shares_t::iterator pos_;

	virtual int rnd_init(pinba_handler_t *handler, bool scan) override
	{
		// TODO:
		// mysql code comments say that this fuction might get called twice in a row
		// i have no idea why or when
		// so just hack around the fact for now
		if (!data_.empty())
			return 0;

		// copy whatever we need from share and release the lock
		view_t tmp_data = []()
		{
			view_t tmp_data;

			std::lock_guard<std::mutex> lk_(P_CTX_->lock);

			for (auto const& share_pair : P_CTX_->open_shares)
			{
				auto const share = share_pair.second;

				// use only shares that are actually supposed to be backed by pinba report
				if (!share->report_needs_engine)
					continue;

				// and shares that have a report active
				if (!share->report_active)
					continue;

				tmp_data.emplace_back();
				auto& row = tmp_data.back();

				row.share       = share;
				row.report_name = share->report_name;
			}

			return tmp_data;
		}();

		// get reports state, no lock needed here, might be slow
		// but report that we've taken share for might have been deleted meanwhile
		for (auto& row : tmp_data)
		{
			try
			{
				auto rstate = P_E_->get_report_state(row.report_name);

				assert(rstate); // the above function should throw if rstate is to be returned empty

				row.report_state = move(rstate);
				data_.emplace_back(std::move(row));
			}
			catch (std::exception const& e)
			{
				LOG_DEBUG(P_L_, "get_report_state for {0} failed (skipping), err: {1}", row.report_name, e.what());
				continue;
			}
		}

		pos_ = data_.begin();

		return 0;
	}

	virtual int rnd_end(pinba_handler_t*) override
	{
		data_.clear();
		return 0;
	}

	virtual int rnd_next(pinba_handler_t *handler, uchar *buf) override
	{
		if (pos_ == data_.end())
			return HA_ERR_END_OF_FILE;

		MEOW_DEFER(
			pos_ = std::next(pos_);
		);

		auto const *row   = &(*pos_);
		auto const *share = row->share.get();
		auto       *table = handler->current_table();

		report_info_t const      *rinfo      = row->report_state->info;
		report_stats_t const     *rstats     = row->report_state->stats;
		report_estimates_t const *restimates = &row->report_state->estimates;

		// remember to lock this row stats data, since it might be changed by report host thread
		// FIXME: this probably IS too coarse!
		std::lock_guard<std::mutex> stats_lk_(rstats->lock);

		// remember to lock share to read, might be too coarse, but whatever
		std::lock_guard<std::mutex> lk_(P_CTX_->lock);

		// mark all fields as writeable to avoid assert() in ::store() calls
		// got no idea how to do this properly anyway
		auto *old_map = tmp_use_all_columns(table, table->write_set);
		MEOW_DEFER(
			tmp_restore_column_map(table->write_set, old_map);
		);

		for (Field **field = table->field; *field; field++)
		{
			unsigned const field_index = (*field)->field_index;

			if (!bitmap_is_set(table->read_set, field_index))
				continue;

			// mark field as writeable to avoid assert() in ::store() calls
			// got no idea how to do this properly anyway
			// bitmap_set_bit(table->write_set, field_index);

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
				{
					str_ref const kind_name = [&share]()
					{
						return (share->view_conf)
								? pinba_view_kind::enum_as_str_ref(share->view_conf->kind)
								: meow::ref_lit("!! <table comment parse error (select from it, to see the error)>");
					}();

					(*field)->set_notnull();
					(*field)->store(kind_name.data(), kind_name.c_length(), &my_charset_bin);
				}
				break;

				case 3:  (*field)->set_notnull(); (*field)->store(duration_seconds_as_double(rinfo->time_window)); break;
				case 4:  (*field)->set_notnull(); (*field)->store(rinfo->tick_count); break;
				case 5:  (*field)->set_notnull(); (*field)->store(restimates->row_count); break;
				case 6:  (*field)->set_notnull(); (*field)->store(restimates->mem_used); break;
				case 7:  (*field)->set_notnull(); (*field)->store(rstats->packets_recv_total); break;
				case 8:  (*field)->set_notnull(); (*field)->store(rstats->packets_send_err); break;
				case 9:  (*field)->set_notnull(); (*field)->store(timeval_to_double(rstats->ru_utime)); break;
				case 10: (*field)->set_notnull(); (*field)->store(timeval_to_double(rstats->ru_stime)); break;
				case 11: (*field)->set_notnull(); (*field)->store(timeval_to_double(rstats->last_tick_tv)); break;
				case 12: (*field)->set_notnull(); (*field)->store(duration_seconds_as_double(rstats->last_tick_prepare_d)); break;
				case 13: (*field)->set_notnull(); (*field)->store(duration_seconds_as_double(rstats->last_snapshot_merge_d)); break;
			}
		} // field for

		return 0;
	}
};

////////////////////////////////////////////////////////////////////////////////////////////////

struct pinba_view___report_snapshot_t : public pinba_view___base_t
{
	report_snapshot_ptr            snapshot_;
	report_snapshot_t::position_t  pos_;

	// copied from share
	std::string          mysql_name_;
	std::string          report_name_;
	std::vector<double>  percentiles_;

public:

	virtual int rnd_init(pinba_handler_t *handler, bool scan) override
	{
		{
			// FIXME: this share can be changed by other threads, need to lock!
			//        currently no fields this view uses are changed ever, but fuck it, let's be safe!
			auto share = handler->current_share();

			std::lock_guard<std::mutex> lk_(P_CTX_->lock);
			mysql_name_  = share->mysql_name;
			report_name_ = share->report_name;
			percentiles_ = share->view_conf->percentiles;
		}

		LOG_DEBUG(P_L_, "{0}; getting snapshot for t: {1}, r: {2}", __func__, mysql_name_, report_name_);

		// auto const err = [&]() -> pinba_error_t
		// {
		// 	std::weak_ptr rhost_weak_p;
		// 	auto const err = P_E_->get_report_host(&rhost_weak_p, share->report_name);
		// 	if (err)
		// 		return err;

		// 	auto rhost = rhost_weak_p.lock()
		// 	if (!rhost)
		// 		return ff::fmt_err("report '{0}' deleted during query", mysql_name_);

		// 	snapshot_ = rhost->get_report_snapshot();
		// 	return {};
		// }();

		try
		{
			snapshot_ = P_E_->get_report_snapshot(report_name_);
		}
		catch (std::exception const& e)
		{
			LOG_WARN(P_L_, "{0}; internal error: {1}", __func__, e.what());
			my_printf_error(ER_INTERNAL_ERROR, "[pinba] %s", MYF(0), e.what());
			return HA_ERR_INTERNAL_ERROR;
		}

		// TODO: check if percentile fields are being requested
		//       and do not merge histograms if not

		{
			meow::stopwatch_t sw;
			snapshot_->prepare();

			LOG_DEBUG(P_L_, "{0}; report_snapshot for: {1}, prepare took {2} seconds ({3} rows)",
				__func__, mysql_name_, sw.stamp(), snapshot_->row_count());
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

		MEOW_DEFER(
			pos_ = snapshot_->pos_next(pos_);
		);

		auto const *rinfo = snapshot_->report_info();
		auto const key = snapshot_->get_key_str(pos_);

		unsigned const n_key_fields = rinfo->n_key_parts;

		// mark all fields as writeable to avoid assert() in ::store() calls
		// got no idea how to do this properly anyway
		auto *old_map = tmp_use_all_columns(table, table->write_set);
		MEOW_DEFER(
			tmp_restore_column_map(table->write_set, old_map);
		);

		for (Field **field = table->field; *field; field++)
		{
			unsigned const field_index = (*field)->field_index;
			unsigned       findex      = field_index;

			if (!bitmap_is_set(table->read_set, field_index))
				continue;

			// mark field as writeable to avoid assert() in ::store() calls
			// got no idea how to do this properly anyway
			// bitmap_set_bit(table->write_set, field_index);

			// key comes first
			{
				if (findex < n_key_fields)
				{
					(*field)->set_notnull();
					(*field)->store(key[findex].begin(), key[findex].c_length(), &my_charset_bin);
					continue;
				}
				findex -= n_key_fields;
			}

			// row data comes next
			if (REPORT_KIND__BY_REQUEST_DATA == rinfo->kind)
			{
				static unsigned const n_data_fields = 11;
				if (findex < n_data_fields)
				{
					auto const *row = reinterpret_cast<report_row_data___by_request_t*>(snapshot_->get_data(pos_));

					switch (findex)
					{
					case 0: // req_count
						(*field)->set_notnull();
						(*field)->store(row->req_count);
					break;

					case 1: // req_per_sec
						(*field)->set_notnull();
						(*field)->store(double(row->req_count) / duration_seconds_as_double(rinfo->time_window));
					break;

					case 2: // time_total
						(*field)->set_notnull();
						(*field)->store(duration_seconds_as_double(row->time_total));
					break;

					case 3: // time_per_sec
						(*field)->set_notnull();
						(*field)->store(duration_seconds_as_double(row->time_total) / duration_seconds_as_double(rinfo->time_window));
					break;

					case 4: // ru_utime
						(*field)->set_notnull();
						(*field)->store(duration_seconds_as_double(row->ru_utime));
					break;

					case 5: // ru_utime_per_sec
						(*field)->set_notnull();
						(*field)->store(duration_seconds_as_double(row->ru_utime) / duration_seconds_as_double(rinfo->time_window));
					break;

					case 6: // ru_stime
						(*field)->set_notnull();
						(*field)->store(duration_seconds_as_double(row->ru_stime));
					break;

					case 7: // ru_stime_per_sec
						(*field)->set_notnull();
						(*field)->store(duration_seconds_as_double(row->ru_stime) / duration_seconds_as_double(rinfo->time_window));
					break;

					case 8: // traffic_total
						(*field)->set_notnull();
						(*field)->store(row->traffic_kb);
					break;

					case 9: // traffic_per_sec
						(*field)->set_notnull();
						(*field)->store(double(row->traffic_kb) / duration_seconds_as_double(rinfo->time_window));
					break;

					case 10: // memory_footprint
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
				static unsigned const n_data_fields = 10;
				if (findex < n_data_fields)
				{
					auto const *row = reinterpret_cast<report_row_data___by_timer_t*>(snapshot_->get_data(pos_));

					switch (findex)
					{
					case 0: // req_count
						(*field)->set_notnull();
						(*field)->store(row->req_count);
					break;

					case 1: // req_per_sec
						(*field)->set_notnull();
						(*field)->store(double(row->req_count) / duration_seconds_as_double(rinfo->time_window));
					break;

					case 2: // hit_count
						(*field)->set_notnull();
						(*field)->store(row->hit_count);
					break;

					case 3: // hit_per_sec
						(*field)->set_notnull();
						(*field)->store(double(row->hit_count) / duration_seconds_as_double(rinfo->time_window));
					break;

					case 4: // time_total
						(*field)->set_notnull();
						(*field)->store(duration_seconds_as_double(row->time_total));
					break;

					case 5: // time_per_sec
						(*field)->set_notnull();
						(*field)->store(duration_seconds_as_double(row->time_total) / duration_seconds_as_double(rinfo->time_window));
					break;

					case 6: // ru_utime
						(*field)->set_notnull();
						(*field)->store(duration_seconds_as_double(row->ru_utime));
					break;

					case 7: // ru_utime_per_sec
						(*field)->set_notnull();
						(*field)->store(duration_seconds_as_double(row->ru_utime) / duration_seconds_as_double(rinfo->time_window));
					break;

					case 8: // ru_stime
						(*field)->set_notnull();
						(*field)->store(duration_seconds_as_double(row->ru_stime));
					break;

					case 9: // ru_stime_per_sec
						(*field)->set_notnull();
						(*field)->store(duration_seconds_as_double(row->ru_stime) / duration_seconds_as_double(rinfo->time_window));
					break;

					default:
						break;
					}

					continue;
				}
				findex -= n_data_fields;
			}
			else if (REPORT_KIND__BY_PACKET_DATA == rinfo->kind)
			{
				static unsigned const n_data_fields = 7;
				if (findex < n_data_fields)
				{
					auto const *row = reinterpret_cast<report_row_data___by_packet_t*>(snapshot_->get_data(pos_));

					switch (findex)
					{
					case 0:
						(*field)->set_notnull();
						(*field)->store(row->req_count);
					break;

					case 1:
						(*field)->set_notnull();
						(*field)->store(row->timer_count);
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

					case 5:
						(*field)->set_notnull();
						(*field)->store(row->traffic_kb);
					break;

					case 6:
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
			else
			{
				LOG_ERROR(P_L_, "{0}; unknown report snapshot data_kind: {1}", __func__, rinfo->kind);
				// XXX: should we assert here or something?
			}

			// percentiles
			// TODO: calculate all required percentiles in one go
			unsigned const n_percentile_fields = percentiles_.size();
			if (findex < n_percentile_fields)
			{
				auto const *histogram = snapshot_->get_histogram(pos_);
				assert(histogram != nullptr);

				auto const percentile_d = [&]() -> duration_t
				{
					if (HISTOGRAM_KIND__HASHTABLE == rinfo->hv_kind)
					{
						auto const *hv = static_cast<histogram_t const*>(histogram);
						return get_percentile(*hv, { rinfo->hv_bucket_count, rinfo->hv_bucket_d }, percentiles_[findex]);
					}
					else if (HISTOGRAM_KIND__FLAT == rinfo->hv_kind)
					{
						auto const *hv = static_cast<flat_histogram_t const*>(histogram);
						return get_percentile(*hv, { rinfo->hv_bucket_count, rinfo->hv_bucket_d }, percentiles_[findex]);
					}

					assert(!"must not be reached");
					return {0};
				}();

				(*field)->set_notnull();
				(*field)->store(duration_seconds_as_double(percentile_d));

				continue;
			}
			findex -= n_percentile_fields;

		} // loop over all fields

		return 0;
	}
};

pinba_view_ptr pinba_view_create(pinba_view_conf_t const& vcf)
{
	switch (vcf.kind)
	{
		case pinba_view_kind::stats:
			return meow::make_unique<pinba_view___stats_t>();

		case pinba_view_kind::active_reports:
			return meow::make_unique<pinba_view___active_reports_t>();

		case pinba_view_kind::report_by_request_data:
		case pinba_view_kind::report_by_timer_data:
		case pinba_view_kind::report_by_packet_data:
			return meow::make_unique<pinba_view___report_snapshot_t>();

		default:
			assert(!"must not be reached");
			return {};
	}
}


pinba_report_ptr pinba_view_report_create(pinba_view_conf_t const& vcf)
{
	switch (vcf.kind)
	{
		case pinba_view_kind::stats:
		case pinba_view_kind::active_reports:
			return {};

		case pinba_view_kind::report_by_packet_data:
			return create_report_by_packet(P_G_, *pinba_view_conf_get___by_packet(vcf));

		case pinba_view_kind::report_by_request_data:
			return create_report_by_request(P_G_, *pinba_view_conf_get___by_request(vcf));

		case pinba_view_kind::report_by_timer_data:
			return create_report_by_timer(P_G_, *pinba_view_conf_get___by_timer(vcf));

		default:
			assert(!"must not be reached");
			return {};
	}
}

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

static pinba_share_ptr pinba_share_get_or_create(char const *table_name)
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


static void share_init_with_table_comment_locked(pinba_share_ptr& share, str_ref table_comment)
{
	assert(!share->view_conf);
	assert(!share->report);
	assert(!share->report_active);

	share->view_conf   = pinba_view_conf_parse(share->mysql_name, table_comment);
	share->report      = pinba_view_report_create(*share->view_conf);

	if (share->report)
	{
		share->report_name         = share->mysql_name;
		share->report_active       = false;
		share->report_needs_engine = true;
	}
	else
	{
		share->report_name = ff::fmt_str("<virtual table: {0}>", pinba_view_kind::enum_as_str_ref(share->view_conf->kind));
		share->report_active       = true;
		share->report_needs_engine = false;
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////

pinba_handler_t::pinba_handler_t(handlerton *hton, TABLE_SHARE *table_arg)
	: handler(hton, table_arg)
	, share_(nullptr)
{
	ff::fmt(stderr, "");
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
		share_init_with_table_comment_locked(share, comment);
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

	// FIXME: need to get A share here, and assign to this->share_ after init completion (but still need to lock!)
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
			share_init_with_table_comment_locked(share_, comment);
		}

		pinba_view_ = pinba_view_create(*share_->view_conf);
	}
	catch (std::exception const& e)
	{
		// this MUST not happen in fact, since all tables should have been created
		// and we do parse comments on create()
		// might happen when pinba versions are upgraded or something, i guess?

		LOG_ERROR(P_L_, "{0}; table: {1}, error: {2}", __func__, share_->mysql_name, e.what());

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

				// FIXME: if report activation fails, we will have lost the report (and assert abouve would fire)
				//        should be fixed with re-creating report again, or just make it ref counted
				pinba_error_t err = P_E_->add_report(share_->report);
				if (err)
					throw std::runtime_error(ff::fmt_str("can't activate report: {0}", err.what()));

				share_->report.reset(); // do not hold onto the report after activation
				share_->report_active = true;
			}
		}
		catch (std::exception const& e)
		{
			LOG_ERROR(P_L_, "{0}; table: {1}, error: {2}", __func__, share_->mysql_name, e.what());

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
		LOG_ERROR(P_L_, "{0}; can't find table to rename from: '{1}' (weird mysql shenanigans?)", __func__, from);
		DBUG_RETURN(0);
	}

	auto share = it->second;
	open_shares.erase(it);

	share->mysql_name = to;
	open_shares.emplace(share->mysql_name, share);

	LOG_DEBUG(P_L_, "{0}; renamed mysql table '{1}' -> '{2}', internal report_name: '{3}'",
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
		LOG_ERROR(P_L_, "{0}; can't find table to delete: '{1}' (weird mysql shenanigans?)", __func__, table_name);
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
			LOG_ERROR(P_L_, "{0}; table: '{1}', report: '{2}'; error: {3}",
				__func__, share->mysql_name, share->report_name, err.what());

			DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
		}
	}

	LOG_DEBUG(P_L_, "{0}; dropped table '{1}', report '{2}'", __func__, share->mysql_name, share->report_name);

	DBUG_RETURN(0);
}

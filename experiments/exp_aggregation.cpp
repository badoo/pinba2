#include <meow/defer.hpp>

#include "pinba/globals.h"
#include "pinba/report.h"
#include "pinba/histogram.h"
#include "pinba/packet.h"
#include "pinba/repacker.h"
#include "pinba/report.h"
#include "pinba/report_util.h"
#include "pinba/report_by_packet.h"

////////////////////////////////////////////////////////////////////////////////////////////////

struct report_tick_t : public meow::ref_counted_t
{
	repacker_state_ptr repacker_state;

	virtual ~report_tick_t() {}
};
using report_tick_ptr = boost::intrusive_ptr<report_tick_t>;

struct repacker_state_test_t : public repacker_state_t
{
	std::string dummy;

	virtual void merge_other(repacker_state_t& other_ref) override
	{
		auto const& other = static_cast<repacker_state_test_t const&>(other_ref);
		dummy += std::move(other.dummy);
	}

	repacker_state_test_t()
	{
		ff::fmt(dummy, "{0} ", this);
	}

	~repacker_state_test_t()
	{
		ff::fmt(stdout, "{0}; dummy: {1}\n", __func__, dummy);
	}
};

struct report_agg_t : private boost::noncopyable
{
	virtual ~report_agg_t() {}

	virtual void stats_init(report_stats_t *stats) = 0;

	virtual void add(packet_t*) = 0;
	virtual void add_multi(packet_t**, uint32_t) = 0;

	virtual report_tick_ptr     tick_now(timeval_t curr_tv) = 0;
	virtual report_estimates_t  get_estimates() = 0;
};
using report_agg_ptr = std::shared_ptr<report_agg_t>;

struct report_history_t : private boost::noncopyable
{
	virtual ~report_history_t() {}

	virtual void stats_init(report_stats_t *stats) = 0;
	virtual void merge_tick(report_tick_ptr) = 0;

	virtual report_snapshot_ptr get_snapshot() = 0;
};
using report_history_ptr = std::shared_ptr<report_history_t>;

struct new_report_t : private boost::noncopyable
{
	virtual ~new_report_t() {}

	virtual str_ref name() const = 0;
	virtual report_info_t const* info() const = 0;

	virtual report_agg_ptr      create_aggregator() = 0;
	virtual report_history_ptr  create_history() = 0;
};
using new_report_ptr = std::shared_ptr<new_report_t>;

////////////////////////////////////////////////////////////////////////////////////////////////
// history impls

struct report_history_ringbuffer_t : private boost::noncopyable
{
	struct item_t
	{
		report_tick_ptr data;
	};

	using ringbuffer_t = std::vector<item_t>;

public:

	report_history_ringbuffer_t(uint32_t max_ticks)
		: max_ticks_(max_ticks)
	{
	}

	report_tick_ptr append(report_tick_ptr tick)
	{
		report_tick_ptr result = {};

		ringbuffer_.emplace_back(item_t{std::move(tick)});
		if (ringbuffer_.size() > max_ticks_)
		{
			result = std::move(ringbuffer_.begin()->data);
			ringbuffer_.erase(ringbuffer_.begin()); // XXX: O(n)
		}

		return result;
	}

	ringbuffer_t const& get_ringbuffer() const
	{
		return ringbuffer_;
	}

private:
	uint32_t      max_ticks_;
	ringbuffer_t  ringbuffer_;
};

////////////////////////////////////////////////////////////////////////////////////////////////
// packet specific

struct report_row___by_packet_t
{
	report_row_data___by_packet_t  data;
	histogram_t                    hv;
};

struct report_tick___by_packet_t
	: public report_tick_t
	, public report_row___by_packet_t
{
};

struct report_agg___by_packet_t : public report_agg_t
{
	using tick_t   = report_tick___by_packet_t;
	using tick_ptr = boost::intrusive_ptr<tick_t>;

	void tick___data_increment(tick_t *tick, packet_t *packet)
	{
		tick->data.req_count   += 1;
		tick->data.timer_count += packet->timer_count;
		tick->data.time_total  += packet->request_time;
		tick->data.ru_utime    += packet->ru_utime;
		tick->data.ru_stime    += packet->ru_stime;
		tick->data.traffic     += packet->traffic;
		tick->data.mem_used    += packet->mem_used;
	}

	void tick___hv_increment(tick_t *tick, packet_t *packet, uint32_t hv_bucket_count, duration_t hv_bucket_d)
	{
		tick->hv.increment({hv_bucket_count, hv_bucket_d}, packet->request_time);
	}

public:

	report_agg___by_packet_t(pinba_globals_t *globals, report_conf___by_packet_t const& conf)
		: globals_(globals)
		, stats_(nullptr)
		, conf_(conf)
		, tick_(meow::make_intrusive<tick_t>())
	{
	}

	virtual void stats_init(report_stats_t *stats) override
	{
		stats_ = stats;
	}

	virtual void add(packet_t *packet) override
	{
		// run all filters and check if packet is 'interesting to us'
		for (size_t i = 0, i_end = conf_.filters.size(); i < i_end; ++i)
		{
			auto const& filter = conf_.filters[i];
			if (!filter.func(packet))
			{
				stats_->packets_dropped_by_filters++;
				return;
			}
		}

		// apply packet data
		tick___data_increment(tick_.get(), packet);

		if (conf_.hv_bucket_count > 0)
		{
			tick___hv_increment(tick_.get(), packet, conf_.hv_bucket_count, conf_.hv_bucket_d);
		}

		stats_->packets_aggregated++;
	}

	virtual void add_multi(packet_t **packets, uint32_t packet_count) override
	{
		for (uint32_t i = 0; i < packet_count; ++i)
			this->add(packets[i]);
	}

	virtual report_tick_ptr tick_now(timeval_t curr_tv) override
	{
		tick_ptr result = std::move(tick_);
		tick_ = meow::make_intrusive<tick_t>();
		return std::move(result);
	}

	virtual report_estimates_t get_estimates() override
	{
		report_estimates_t result = {};

		result.row_count = 1; // always a single row (no aggregation)

		result.mem_used += sizeof(*tick_);
		result.mem_used += tick_->hv.map_cref().bucket_count() * sizeof(*tick_->hv.map_cref().begin());

		return result;
	}

private:
	pinba_globals_t            *globals_;
	report_stats_t             *stats_;
	report_conf___by_packet_t  conf_;

	tick_ptr                   tick_;
};

struct report_history___by_packet_t : public report_history_t
{
	using ring_t       = report_history_ringbuffer_t;
	using ringbuffer_t = ring_t::ringbuffer_t;

public:

	report_history___by_packet_t(pinba_globals_t *globals, report_info_t const& rinfo)
		: globals_(globals)
		, stats_(nullptr)
		, rinfo_(rinfo)
		, ring_(rinfo.tick_count)
	{
	}

	virtual void stats_init(report_stats_t *stats) override
	{
		stats_ = stats;
	}

	virtual void merge_tick(report_tick_ptr tick)
	{
		ring_.append(std::move(tick));
	}

	virtual report_snapshot_ptr get_snapshot() override
	{
		struct snapshot_traits
		{
			using src_ticks_t = ringbuffer_t;
			using hashtable_t = std::array<report_row___by_packet_t, 1>; // array to get iterators 'for free'

			static report_key_t key_at_position(hashtable_t const&, hashtable_t::iterator const& it)    { return {}; }
			static void*        value_at_position(hashtable_t const&, hashtable_t::iterator const& it)  { return (void*)it; }
			static void*        hv_at_position(hashtable_t const&, hashtable_t::iterator const& it)     { return &it->hv; }

			// merge from src ringbuffer to snapshot data
			static void merge_ticks_into_data(
				  pinba_globals_t *globals
				, report_info_t& rinfo
				, src_ticks_t& ticks
				, hashtable_t& to
				, report_snapshot_t::prepare_type_t ptype)
			{
				MEOW_DEFER(
					ticks.clear();
				);

				for (auto const& tick : ticks)
				{
					if (!tick.data)
						continue;

					auto const& src = static_cast<report_tick___by_packet_t const&>(*tick.data);
					auto      & dst = to[0];

					dst.data.req_count   += src.data.req_count;
					dst.data.timer_count += src.data.timer_count;
					dst.data.time_total  += src.data.time_total;
					dst.data.ru_utime    += src.data.ru_utime;
					dst.data.ru_stime    += src.data.ru_stime;
					dst.data.traffic     += src.data.traffic;
					dst.data.mem_used    += src.data.mem_used;

					if (rinfo.hv_enabled)
					{
						dst.hv.merge_other(src.hv);
					}
				}
			}
		};

		return meow::make_unique<report_snapshot__impl_t<snapshot_traits>>(globals_, ring_.get_ringbuffer(), rinfo_);
	}

private:
	pinba_globals_t              *globals_;
	report_stats_t               *stats_;
	report_info_t                rinfo_;

	report_history_ringbuffer_t  ring_;
};


struct report_history___by_packet_windowed_t : public report_history_t
{
	using ring_t       = report_history_ringbuffer_t;
	using ringbuffer_t = ring_t::ringbuffer_t;

public:

	report_history___by_packet_windowed_t(pinba_globals_t *globals, report_info_t const& rinfo)
		: globals_(globals)
		, stats_(nullptr)
		, rinfo_(rinfo)
		, ring_(rinfo.tick_count)
	{
	}

	virtual void stats_init(report_stats_t *stats) override
	{
		stats_ = stats;
	}

	virtual void merge_tick(report_tick_ptr tick)
	{
		report_tick_ptr old_tick = ring_.append(tick);

		// merge incoming tick into data
		{
			auto const& src = static_cast<report_tick___by_packet_t const&>(*tick);

			merged_data_.req_count   += src.data.req_count;
			merged_data_.timer_count += src.data.timer_count;
			merged_data_.time_total  += src.data.time_total;
			merged_data_.ru_utime    += src.data.ru_utime;
			merged_data_.ru_stime    += src.data.ru_stime;
			merged_data_.traffic     += src.data.traffic;
			merged_data_.mem_used    += src.data.mem_used;
		}

		// un-merge old tick
		if (old_tick)
		{
			auto const& src = static_cast<report_tick___by_packet_t const&>(*old_tick);

			merged_data_.req_count   -= src.data.req_count;
			merged_data_.timer_count -= src.data.timer_count;
			merged_data_.time_total  -= src.data.time_total;
			merged_data_.ru_utime    -= src.data.ru_utime;
			merged_data_.ru_stime    -= src.data.ru_stime;
			merged_data_.traffic     -= src.data.traffic;
			merged_data_.mem_used    -= src.data.mem_used;
		}
	}

	virtual report_snapshot_ptr get_snapshot() override
	{
		struct snapshot_traits
		{
			using src_ticks_t = ringbuffer_t;
			using hashtable_t = std::array<report_row___by_packet_t, 1>; // array to get iterators 'for free'

			static report_key_t key_at_position(hashtable_t const&, hashtable_t::iterator const& it)    { return {}; }
			static void*        value_at_position(hashtable_t const&, hashtable_t::iterator const& it)  { return (void*)it; }
			static void*        hv_at_position(hashtable_t const&, hashtable_t::iterator const& it)     { return &it->hv; }

			// merge from src ringbuffer to snapshot data
			static void merge_ticks_into_data(
				  pinba_globals_t *globals
				, report_info_t& rinfo
				, src_ticks_t& ticks
				, hashtable_t& to
				, report_snapshot_t::prepare_type_t ptype)
			{
			}
		};

		auto snapshot = meow::make_unique<report_snapshot__impl_t<snapshot_traits>>(globals_, rinfo_);

		// merge everything now
		// no need to copy ringbuffer, since we're inside our own thread
		{
			report_row___by_packet_t& dst = snapshot->data_[0];

			dst.data = merged_data_;

			for (auto const& ring_elt : ring_.get_ringbuffer())
			{
				auto const *src_tick = static_cast<report_tick___by_packet_t const*>(ring_elt.data.get());

				if (src_tick->repacker_state)
				{
					if (!snapshot->repacker_state)
						snapshot->repacker_state = src_tick->repacker_state;
					else
						snapshot->repacker_state->merge_other(*src_tick->repacker_state);
				}

				if (rinfo_.hv_enabled)
					dst.hv.merge_other(src_tick->hv);
			}
		}

		return std::move(snapshot);
	}

private:
	pinba_globals_t              *globals_;
	report_stats_t               *stats_;
	report_info_t                rinfo_;

	report_history_ringbuffer_t  ring_;

// merged
	report_row_data___by_packet_t merged_data_;
};


struct new_report___by_packet_t : public new_report_t
{
	new_report___by_packet_t(pinba_globals_t *globals, report_conf___by_packet_t const& conf)
		: globals_(globals)
		, conf_(conf)
	{
		rinfo_ = report_info_t {
			.name            = conf_.name,
			.kind            = REPORT_KIND__BY_PACKET_DATA,
			.time_window     = conf_.time_window,
			.tick_count      = conf_.tick_count,
			.n_key_parts     = 0,
			.hv_enabled      = (conf_.hv_bucket_count > 0),
			.hv_kind         = HISTOGRAM_KIND__HASHTABLE,
			.hv_bucket_count = conf_.hv_bucket_count,
			.hv_bucket_d     = conf_.hv_bucket_d,
		};
	}

	virtual str_ref name() const override
	{
		return rinfo_.name;
	}

	virtual report_info_t const* info() const override
	{
		return &rinfo_;
	}

	virtual report_agg_ptr create_aggregator() override
	{
		return std::make_shared<report_agg___by_packet_t>(globals_, conf_);
	}

	virtual report_history_ptr create_history() override
	{
		// return std::make_shared<report_history___by_packet_t>(globals_, rinfo_);
		return std::make_shared<report_history___by_packet_windowed_t>(globals_, rinfo_);
	}

private:
	pinba_globals_t            *globals_;
	report_info_t              rinfo_;
	report_conf___by_packet_t  conf_;
};

////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char const *argv[])
{
	pinba_options_t options = {};
	pinba_globals_t *globals = pinba_globals_init(&options);

	packet_t packet_data = {
		.host_id       = 1,
		.server_id     = 0,
		.script_id     = 7,
		.schema_id     = 0,
		.status        = 0,
		.traffic       = 9999,
		.mem_used      = 1,
		.tag_count     = 0,
		.timer_count   = 0,
		.request_time  = duration_t{ 15 * msec_in_sec },
		.ru_utime      = duration_t{ 3 * msec_in_sec },
		.ru_stime      = duration_t{ 1 * msec_in_sec },
		.tag_name_ids  = NULL,
		.tag_value_ids = NULL,
		.timers        = NULL,
	};

	packet_t *packet = &packet_data;

	if (argc >= 2)
	{
		FILE *f = fopen(argv[1], "r");
		uint8_t buf[16 * 1024];
		size_t n = fread(buf, 1, sizeof(buf), f);

		auto request = pinba__request__unpack(NULL, n, buf);
		if (!request)
			throw std::runtime_error("request unpack error");

		struct nmpa_s nmpa;
		nmpa_init(&nmpa, 1024);

		packet = pinba_request_to_packet(request, globals->dictionary(), &nmpa);

		debug_dump_packet(stdout, packet, globals->dictionary());
	}

	//

	report_conf___by_packet_t const r_conf = {
		.name            = "test",
		.time_window     = 60 * d_second,
		.tick_count      = 60,
		.hv_bucket_count = 1000000,
		.hv_bucket_d     = 1 * d_microsecond,
		.filters         = {},
	};

	report_info_t r_info = {
		.name            = r_conf.name,
		.kind            = REPORT_KIND__BY_PACKET_DATA,
		.time_window     = r_conf.time_window,
		.tick_count      = r_conf.tick_count,
		.n_key_parts     = 0,
		.hv_enabled      = (r_conf.hv_bucket_count > 0),
		.hv_kind         = HISTOGRAM_KIND__HASHTABLE,
		.hv_bucket_count = r_conf.hv_bucket_count,
		.hv_bucket_d     = r_conf.hv_bucket_d,
	};

	report_stats_t r_stats = {};

	new_report_ptr report = std::make_shared<new_report___by_packet_t>(globals, r_conf);

	auto r_agg = report->create_aggregator();
	r_agg->stats_init(&r_stats);

	auto r_history = report->create_history();
	r_history->stats_init(&r_stats);

	auto const finish_and_print_tick = [&]()
	{
		report_tick_ptr tick_base = r_agg->tick_now(os_unix::clock_monotonic_now());
		tick_base->repacker_state = std::make_shared<repacker_state_test_t>();

		auto const *tick = static_cast<report_tick___by_packet_t const*>(tick_base.get());

		auto const *data = &tick->data;

		ff::fmt(stdout, "{{ {0}, {1}, {2}, {3}, {4}, {5} }\n",
			data->req_count, data->time_total, data->ru_utime, data->ru_stime,
			data->traffic, data->mem_used);

		r_history->merge_tick(std::move(tick_base));
	};

	r_agg->add(packet);
	r_agg->add(packet);
	r_agg->add(packet);
	r_agg->add(packet);

	finish_and_print_tick();

	r_agg->add(packet);
	r_agg->add(packet);
	r_agg->add(packet);

	finish_and_print_tick();

	r_agg->add(packet);
	r_agg->add(packet);

	finish_and_print_tick();

	// history is smart and can snapshot itself variant
	{
		report_snapshot_ptr snapshot = r_history->get_snapshot();

		snapshot->prepare();
		debug_dump_report_snapshot(stdout, snapshot.get(), snapshot->report_info()->name);
	}

	return 0;
}
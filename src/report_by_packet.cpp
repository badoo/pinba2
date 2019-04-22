#include <array>
#include <utility>

#include <meow/defer.hpp>
#include <boost/noncopyable.hpp>

#include "pinba/globals.h"
#include "pinba/histogram.h"
#include "pinba/packet.h"
#include "pinba/report.h"
#include "pinba/report_util.h"
#include "pinba/report_by_packet.h"

////////////////////////////////////////////////////////////////////////////////////////////////
namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

	// this is the data we return from report___by_packet_t snapshot
	struct report_row___by_packet_t
	{
		report_row_data___by_packet_t     data;
		std::unique_ptr<hdr_histogram_t>  hv;
	};

	struct report_tick___by_packet_t
		: public report_tick_t
		, public report_row___by_packet_t
	{
		struct nmpa_s nmpa;

	public:

		report_tick___by_packet_t(histogram_conf_t const& hv_conf)
		{
			nmpa_init(&nmpa, 1024);
			this->hv = meow::make_unique<hdr_histogram_t>(&nmpa, hv_conf);
		}

		~report_tick___by_packet_t()
		{
			nmpa_free(&nmpa);
		}
	};

////////////////////////////////////////////////////////////////////////////////////////////////

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

		void tick___hv_increment(tick_t *tick, packet_t *packet, histogram_conf_t const& hv_conf)
		{
			tick->hv->increment(hv_conf, packet->request_time);
		}

	public:

		report_agg___by_packet_t(pinba_globals_t *globals, report_conf___by_packet_t const& conf, report_info_t const& rinfo)
			: globals_(globals)
			, stats_(nullptr)
			, conf_(conf)
			, hv_conf_(histogram___configure_with_rinfo(rinfo))
		{
			this->tick_now({});
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
				tick___hv_increment(tick_.get(), packet, hv_conf_);
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
			tick_ = meow::make_intrusive<tick_t>(hv_conf_);
			return std::move(result);
		}

		virtual report_estimates_t get_estimates() override
		{
			report_estimates_t result = {};

			result.row_count = 1; // always a single row (no aggregation)

			result.mem_used += sizeof(*tick_);
			result.mem_used += nmpa_mem_used(&tick_->nmpa);

			return result;
		}

	private:
		pinba_globals_t            *globals_;
		report_stats_t             *stats_;
		report_conf___by_packet_t  conf_;
		histogram_conf_t           hv_conf_;

		tick_ptr                   tick_;
	};

////////////////////////////////////////////////////////////////////////////////////////////////

	struct report_history___by_packet_t : public report_history_t
	{
		using ring_t       = report_history_ringbuffer_t;
		using ringbuffer_t = ring_t::ringbuffer_t;

	public:

		report_history___by_packet_t(pinba_globals_t *globals, report_info_t const& rinfo)
			: globals_(globals)
			, stats_(nullptr)
			, rinfo_(rinfo)
			, hv_conf_(histogram___configure_with_rinfo(rinfo))
			, ring_(rinfo.tick_count)
		{
		}

		virtual void stats_init(report_stats_t *stats) override
		{
			stats_ = stats;
		}

		virtual void merge_tick(report_tick_ptr tick) override
		{
			ring_.append(std::move(tick));
		}

		virtual report_estimates_t get_estimates() override
		{
			return {};
		}

		virtual report_snapshot_ptr get_snapshot() override
		{
			struct snapshot_traits
			{
				using totals_t    = report_row_data___by_packet_t;
				using src_ticks_t = ringbuffer_t;
				using hashtable_t = std::array<report_row___by_packet_t, 1>; // array to get iterators 'for free'

				static report_key_t key_at_position(hashtable_t const&, hashtable_t::iterator const& it)    { return {}; }
				static void*        value_at_position(hashtable_t const&, hashtable_t::iterator const& it)  { return (void*)it; }
				static void*        hv_at_position(hashtable_t const&, hashtable_t::iterator const& it)     { return it->hv.get(); }

				static void calculate_raw_stats(report_snapshot_ctx_t *snapshot_ctx, src_ticks_t const& ticks, report_raw_stats_t *stats)
				{
					for (auto const& tick_base : ticks)
					{
						if (!tick_base)
							continue;

						stats->row_count += 1;
					}
				}

				static void calculate_totals(report_snapshot_ctx_t *snapshot_ctx, hashtable_t const& data, totals_t *totals)
				{
					auto const& row = data[0].data;

					totals->req_count   += row.req_count;
					totals->timer_count += row.timer_count;
					totals->time_total  += row.time_total;
					totals->ru_utime    += row.ru_utime;
					totals->ru_stime    += row.ru_stime;
					totals->traffic     += row.traffic;
					totals->mem_used    += row.mem_used;
				}

				// merge from src ringbuffer to snapshot data
				static void merge_ticks_into_data(
					  report_snapshot_ctx_t *snapshot_ctx
					, src_ticks_t& ticks
					, hashtable_t& to
					, report_snapshot_t::merge_flags_t flags)
				{
					bool const need_histograms = (snapshot_ctx->rinfo.hv_enabled && (flags & report_snapshot_t::merge_flags::with_histograms));

					for (auto const& tick : ticks)
					{
						if (!tick)
							continue;

						auto const& src = static_cast<report_tick___by_packet_t const&>(*tick);
						auto      & dst = to[0];

						dst.data.req_count   += src.data.req_count;
						dst.data.timer_count += src.data.timer_count;
						dst.data.time_total  += src.data.time_total;
						dst.data.ru_utime    += src.data.ru_utime;
						dst.data.ru_stime    += src.data.ru_stime;
						dst.data.traffic     += src.data.traffic;
						dst.data.mem_used    += src.data.mem_used;

						if (need_histograms)
						{
							if (!dst.hv)
							{
								dst.hv = meow::make_unique<hdr_histogram_t>(&snapshot_ctx->nmpa, snapshot_ctx->hv_conf);
							}

							dst.hv->merge_other_with_same_conf(*src.hv, snapshot_ctx->hv_conf);
						}
					}

					// can clear ticks here, as we've merged hvs with hdr already
					ticks.clear();
				}
			};
			using snapshot_t = report_snapshot__impl_t<snapshot_traits>;

			report_snapshot_ctx_t const sctx = {
				.globals        = globals_,
				.stats          = stats_,
				.rinfo          = rinfo_,
				.estimates      = this->get_estimates(),
				.hv_conf        = hv_conf_,
				.nmpa           = nmpa_autofree_t(64 * 1024),
			};

			return meow::make_unique<snapshot_t>(sctx, ring_.get_ringbuffer());
		}

	private:
		pinba_globals_t              *globals_;
		report_stats_t               *stats_;
		report_info_t                rinfo_;
		histogram_conf_t             hv_conf_;

		report_history_ringbuffer_t  ring_;
	};

////////////////////////////////////////////////////////////////////////////////////////////////

	struct report___by_packet_t : public report_t
	{
		report___by_packet_t(pinba_globals_t *globals, report_conf___by_packet_t const& conf)
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
				.hv_kind         = HISTOGRAM_KIND__HDR,
				.hv_bucket_count = conf_.hv_bucket_count,
				.hv_bucket_d     = conf_.hv_bucket_d,
				.hv_min_value    = conf_.hv_min_value,
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
			return std::make_shared<report_agg___by_packet_t>(globals_, conf_, rinfo_);
		}

		virtual report_history_ptr create_history() override
		{
			return std::make_shared<report_history___by_packet_t>(globals_, rinfo_);
		}

	private:
		pinba_globals_t            *globals_;
		report_info_t              rinfo_;
		report_conf___by_packet_t  conf_;
	};

////////////////////////////////////////////////////////////////////////////////////////////////
}} // namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

report_ptr create_report_by_packet(pinba_globals_t *globals, report_conf___by_packet_t const& conf)
{
	return std::make_shared<aux::report___by_packet_t>(globals, conf);
}

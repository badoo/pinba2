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
		report_row_data___by_packet_t  data;
		histogram_t                    hv;
	};

	struct report_tick___by_packet_t
		: public report_tick_t
		, public report_row___by_packet_t
	{
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
				using src_ticks_t = ringbuffer_t;
				using hashtable_t = std::array<report_row___by_packet_t, 1>; // array to get iterators 'for free'

				static report_key_t key_at_position(hashtable_t const&, hashtable_t::iterator const& it)    { return {}; }
				static void*        value_at_position(hashtable_t const&, hashtable_t::iterator const& it)  { return (void*)it; }
				static void*        hv_at_position(hashtable_t const&, hashtable_t::iterator const& it)     { return &it->hv; }

				// merge from src ringbuffer to snapshot data
				static void merge_ticks_into_data(
					  report_snapshot_ctx_t *snapshot_ctx
					, src_ticks_t& ticks
					, hashtable_t& to
					, report_snapshot_t::prepare_type_t ptype)
				{
					MEOW_DEFER(
						ticks.clear();
					);

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

						if (snapshot_ctx->rinfo.hv_enabled)
						{
							dst.hv.merge_other(src.hv);
						}
					}
				}
			};
			using snapshot_t = report_snapshot__impl_t<snapshot_traits>;

			return meow::make_unique<snapshot_t>(report_snapshot_ctx_t{globals_, stats_, rinfo_}, ring_.get_ringbuffer());
		}

	private:
		pinba_globals_t              *globals_;
		report_stats_t               *stats_;
		report_info_t                rinfo_;

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
			return std::make_shared<report_history___by_packet_t>(globals_, rinfo_);
		}

	private:
		pinba_globals_t            *globals_;
		report_info_t              rinfo_;
		report_conf___by_packet_t  conf_;
	};

////////////////////////////////////////////////////////////////////////////////////////////////
#if 0
	struct report___by_packet_t : public report_old_t
	{
		// typedef report_key_t                   key_t;
		typedef report_row_data___by_packet_t  data_t;

		struct item_t
			// : private boost::noncopyable // bring this back, when we remove copy ctor
		{
			data_t      data;
			histogram_t hv;

			// XXX: only used by dense_hash_map to default construct the object to be filled
			item_t()
				: data()
				, hv()
			{
			}

			// FIXME: only used by dense_hash_map for set_empty_key()
			//        should not be called often with huge histograms, so expect it to be ok :(
			//        sparsehash with c++11 support (https://github.com/sparsehash/sparsehash-c11) fixes this
			//        but gcc 4.9.4 doesn't support the type_traits it requires
			//        so live this is for now, but probably - move to gcc6 or something
			item_t(item_t const& other)
				: data(other.data)
				, hv(other.hv)
			{
			}

			item_t(item_t&& other)
				: item_t()
			{
				*this = std::move(other); // operator=()
			}

			void operator=(item_t&& other)
			{
				data = other.data;          // a copy
				hv   = std::move(other.hv); // real move
			}

			void data_increment(packet_t *packet)
			{
				data.req_count   += 1;
				data.timer_count += packet->timer_count;
				data.time_total  += packet->request_time;
				data.ru_utime    += packet->ru_utime;
				data.ru_stime    += packet->ru_stime;
				data.traffic     += packet->traffic;
				data.mem_used    += packet->mem_used;
			}

			void hv_increment(packet_t *packet, uint32_t hv_bucket_count, duration_t hv_bucket_d)
			{
				hv.increment({hv_bucket_count, hv_bucket_d}, packet->request_time);
			}
		};

	public: // ticks

		using ticks_t       = ticks_ringbuffer_t<item_t>;
		using tick_t        = ticks_t::tick_t;
		using ticks_list_t  = ticks_t::ringbuffer_t;

	public: // snapshot

		struct snapshot_traits
		{
			using src_ticks_t = ticks_list_t;
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
					if (!tick)
						continue;

					auto const& src = tick->data;
					auto      & dst = to[0];

					dst.data.req_count   += src.data.req_count;
					dst.data.timer_count += src.data.timer_count;
					dst.data.time_total  += src.data.time_total;
					dst.data.ru_utime    += src.data.ru_utime;
					dst.data.ru_stime    += src.data.ru_stime;
					dst.data.traffic  += src.data.traffic;
					dst.data.mem_used    += src.data.mem_used;

					if (rinfo.hv_enabled)
					{
						dst.hv.merge_other(src.hv);
					}
				}
			}
		};

		using snapshot_t = report_snapshot__impl_t<snapshot_traits>;

	public:

		report___by_packet_t(pinba_globals_t *globals, report_conf___by_packet_t const& conf)
			: globals_(globals)
			, stats_(nullptr)
			, conf_(conf)
			, ticks_(conf.tick_count)
		{
			info_ = report_info_t {
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
			return info_.name;
		}

		virtual report_info_t const* info() const override
		{
			return &info_;
		}

		virtual void stats_init(report_stats_t *stats) override
		{
			stats_ = stats;
		}

	public:

		virtual void ticks_init(timeval_t curr_tv) override
		{
			ticks_.init(curr_tv);
		}

		virtual void tick_now(timeval_t curr_tv) override
		{
			ticks_.tick(curr_tv);
		}

		virtual report_estimates_t get_estimates() override
		{
			report_estimates_t result = {};

			result.row_count = 1; // always a single row (no aggregation)

			result.mem_used += sizeof(ticks_.current().data);

			for (auto const& tick : ticks_.get_internal_buffer())
			{
				if (!tick)
					continue;

				item_t *item = &tick->data;
				result.mem_used += sizeof(*item);
				result.mem_used += item->hv.map_cref().bucket_count() * sizeof(*item->hv.map_cref().begin());
			}

			return result;
		}

		virtual report_snapshot_ptr get_snapshot() override
		{
			return meow::make_unique<snapshot_t>(globals_, ticks_.get_internal_buffer(), info_);
		}

	public:

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
			item_t& item = ticks_.current().data;
			item.data_increment(packet);

			if (info_.hv_enabled)
			{
				item.hv_increment(packet, conf_.hv_bucket_count, conf_.hv_bucket_d);
			}

			stats_->packets_aggregated++;
		}

		virtual void add_multi(packet_t **packets, uint32_t packet_count) override
		{
			for (uint32_t i = 0; i < packet_count; ++i)
				this->add(packets[i]);
		}

	// private:
	protected:
		pinba_globals_t             *globals_;
		report_stats_t              *stats_;
		report_conf___by_packet_t   conf_;

		report_info_t               info_;

		ticks_t                     ticks_;
	};
#endif
////////////////////////////////////////////////////////////////////////////////////////////////
}} // namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

report_ptr create_report_by_packet(pinba_globals_t *globals, report_conf___by_packet_t const& conf)
{
	return std::make_shared<aux::report___by_packet_t>(globals, conf);
}

#include <meow/defer.hpp>

#include <boost/noncopyable.hpp>
#include <boost/preprocessor/arithmetic/add.hpp>
#include <boost/preprocessor/repetition/repeat.hpp>

#include <sparsehash/dense_hash_map>

#include "pinba/globals.h"
#include "pinba/histogram.h"
#include "pinba/packet.h"
#include "pinba/repacker.h"
#include "pinba/report.h"
#include "pinba/report_util.h"
#include "pinba/report_by_request.h"

////////////////////////////////////////////////////////////////////////////////////////////////
namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

	template<size_t NKeys>
	struct report___by_request_t : public report_t
	{
		using key_t   = report_key_impl_t<NKeys>;
		using data_t  = report_row_data___by_request_t;

		using this_report_t       = report___by_request_t;
		using this_report_conf_t  = report_conf___by_request_t;

	public: // ticks + aggregator

		struct tick_item_t
			// : private boost::noncopyable // bring this back, when we remove copy ctor
		{
			data_t       data;
			histogram_t  hv;

			void data_increment(packet_t *packet)
			{
				data.req_count  += 1;
				data.time_total += packet->request_time;
				data.ru_utime   += packet->ru_utime;
				data.ru_stime   += packet->ru_stime;
				data.traffic    += packet->traffic;
				data.mem_used   += packet->mem_used;
			}

			void hv_increment(packet_t *packet, histogram_conf_t const& hv_conf)
			{
				hv.increment(hv_conf, packet->request_time);
			}
		};

		struct tick_t : public report_tick_t
		{
			struct hashtable_t
				: public google::dense_hash_map<key_t, tick_item_t, report_key_impl___hasher_t, report_key_impl___equal_t>
			{
				hashtable_t()
				{
					this->set_empty_key(report_key_impl___make_empty<NKeys>());
				}
			};

			hashtable_t ht;
		};

	public: // aggregator

		struct aggregator_t : public report_agg_t
		{
			aggregator_t(pinba_globals_t *globals, report_conf___by_request_t const& conf)
				: globals_(globals)
				, stats_(nullptr)
				, conf_(conf)
				, tick_(meow::make_intrusive<tick_t>())
			{
				hv_conf_ = histogram_conf_t {
					.bucket_count = conf.hv_bucket_count,
					.bucket_d     = conf.hv_bucket_d,
					.min_value    = conf.hv_min_value,
				};
			}

			virtual void stats_init(report_stats_t *stats) override
			{
				stats_ = stats;
			}

			virtual report_tick_ptr tick_now(timeval_t curr_tv) override
			{
				report_tick_ptr result = std::move(tick_);
				tick_ = meow::make_intrusive<tick_t>();
				return result;
			}

			virtual report_estimates_t get_estimates() override
			{
				report_estimates_t result = {};

				result.row_count = tick_->ht.size();
				result.mem_used += sizeof(*tick_);
				result.mem_used += tick_->ht.bucket_count() * sizeof(*tick_->ht.begin());

				return result;
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

				// construct a key, by runinng all key fetchers
				key_t k;

				for (size_t i = 0, i_end = conf_.keys.size(); i < i_end; ++i)
				{
					auto const& key_descriptor = conf_.keys[i];

					report_conf___by_request_t::key_fetch_result_t const r = key_descriptor.fetcher(packet);
					if (!r.found)
					{
						stats_->packets_dropped_by_rtag++;
						return;
					}

					k[i] = r.key_value;
				}

				// finally - find and update item
				tick_item_t& item = tick_->ht[k];
				item.data_increment(packet);

				if (conf_.hv_bucket_count > 0)
				{
					item.hv_increment(packet, hv_conf_);
				}

				stats_->packets_aggregated++;
			}

			virtual void add_multi(packet_t **packets, uint32_t packet_count) override
			{
				for (uint32_t i = 0; i < packet_count; ++i)
					this->add(packets[i]);
			}

		private:
			pinba_globals_t              *globals_;
			report_stats_t               *stats_;
			report_conf___by_request_t   conf_;

			histogram_conf_t             hv_conf_;
			boost::intrusive_ptr<tick_t> tick_;
		};

	public: // history

		struct history_t : public report_history_t
		{
			using ring_t       = report_history_ringbuffer_t;
			using ringbuffer_t = ring_t::ringbuffer_t;

		public:

			history_t(pinba_globals_t *globals, report_info_t const& rinfo)
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
				report_estimates_t result = {};

				if (!ring_.get_ringbuffer().empty())
				{
					auto const& tick = static_cast<tick_t const&>(*ring_.get_ringbuffer().back());
					result.row_count = tick.ht.size();
				}

				result.mem_used += sizeof(*this);
				for (auto const& tick_base : ring_.get_ringbuffer())
				{
					auto const& tick = static_cast<tick_t const&>(*tick_base);
					result.mem_used += sizeof(tick);
					result.mem_used += tick.ht.bucket_count() * sizeof(*tick.ht.begin());
					for (auto const& ht_pair : tick.ht)
					{
						histogram_t const& hv = ht_pair.second.hv;
						result.mem_used += hv.map_cref().bucket_count() * sizeof(*hv.map_cref().begin());
					}
				}

				return result;
			}

		public: // snapshot

			struct snapshot_traits
			{
				using src_ticks_t = ringbuffer_t;
				using totals_t    = report_row_data___by_request_t;

				struct row_t
				{
					data_t            data;
					histogram_t       tmp_hv; // temporary, used while merging only
					flat_histogram_t  hv;
				};

				struct hashtable_t
					: public google::dense_hash_map<key_t, row_t, report_key_impl___hasher_t, report_key_impl___equal_t>
				{
					hashtable_t() { this->set_empty_key(report_key_impl___make_empty<NKeys>()); }
				};

			public:

				static report_key_t key_at_position(hashtable_t const&, typename hashtable_t::iterator const& it)
				{
					return report_key_t { it->first };
				}

				static void* value_at_position(hashtable_t const&, typename hashtable_t::iterator const& it)
				{
					return (void*)&it->second;
				}

				static void* hv_at_position(hashtable_t const&, typename hashtable_t::iterator const& it)
				{
					return &it->second.hv;
				}

				static void calculate_totals(report_snapshot_ctx_t *snapshot_ctx, totals_t *totals, hashtable_t const& data)
				{
					for (auto const& data_pair : data)
					{
						auto const& row = data_pair.second.data;

						totals->req_count  += row.req_count;
						totals->time_total += row.time_total;
						totals->ru_utime   += row.ru_utime;
						totals->ru_stime   += row.ru_stime;
						totals->traffic    += row.traffic;
						totals->mem_used   += row.mem_used;
					}
				}

				// merge from src ringbuffer to snapshot data
				static void merge_ticks_into_data(
					  report_snapshot_ctx_t *snapshot_ctx
					, src_ticks_t& ticks
					, hashtable_t& to
					, report_snapshot_t::merge_flags_t flags)
				{
					bool const need_histograms = (snapshot_ctx->rinfo.hv_enabled && (flags & report_snapshot_t::merge_flags::no_histograms));

					uint64_t key_lookups = 0;
					uint64_t hv_lookups = 0;

					for (auto const& tick_base : ticks)
					{
						if (!tick_base)
							continue;

						tick_t const& tick = static_cast<tick_t const&>(*tick_base);

						for (auto const& tick_pair : tick.ht)
						{
							tick_item_t const& src = tick_pair.second;
							row_t            & dst = to[tick_pair.first];

							dst.data.req_count  += src.data.req_count;
							dst.data.time_total += src.data.time_total;
							dst.data.ru_utime   += src.data.ru_utime;
							dst.data.ru_stime   += src.data.ru_stime;
							dst.data.traffic    += src.data.traffic;
							dst.data.mem_used   += src.data.mem_used;

							if (need_histograms)
							{
								dst.tmp_hv.merge_other(src.hv);
								hv_lookups += src.hv.map_cref().size();
							}
						}

						repacker_state___merge_to_from(snapshot_ctx->repacker_state, tick.repacker_state);

						key_lookups += tick.ht.size();
					}

					// compact histograms from hashtable to flat
					for (auto& ht_pair : to)
					{
						histogram_t&      tmp_hv = ht_pair.second.tmp_hv;
						flat_histogram_t& hv     = ht_pair.second.hv;

						histogram___convert_ht_to_flat(tmp_hv, &hv);

						tmp_hv.clear();
					}

					LOG_DEBUG(snapshot_ctx->logger(), "prepare '{0}'; n_ticks: {1}, key_lookups: {2}, hv_lookups: {3}",
						snapshot_ctx->rinfo.name, ticks.size(), key_lookups, hv_lookups);

					// can clean ticks only if histograms were not merged
					// if (!need_histograms) // yes we can, since merge goes through a temporary ht
					{
						ticks.clear();
						ticks.shrink_to_fit();
					}
				}
			};

			virtual report_snapshot_ptr get_snapshot() override
			{
				using snapshot_t = report_snapshot__impl_t<snapshot_traits>;
				return meow::make_unique<snapshot_t>(report_snapshot_ctx_t{globals_, stats_, rinfo_}, ring_.get_ringbuffer());
			}

		private:
			pinba_globals_t              *globals_;
			report_stats_t               *stats_;
			report_info_t                rinfo_;

			report_history_ringbuffer_t  ring_;
		};

	public: // report_t

		report___by_request_t(pinba_globals_t *globals, report_conf___by_request_t const& conf)
			: globals_(globals)
			, stats_(nullptr)
			, conf_(conf)
		{
			rinfo_ = report_info_t {
				.name            = conf_.name,
				.kind            = REPORT_KIND__BY_REQUEST_DATA,
				.time_window     = conf_.time_window,
				.tick_count      = conf_.tick_count,
				.n_key_parts     = (uint32_t)conf_.keys.size(),
				.hv_enabled      = (conf_.hv_bucket_count > 0),
				.hv_kind         = HISTOGRAM_KIND__FLAT,
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
			return std::make_shared<aggregator_t>(globals_, conf_);
		}

		virtual report_history_ptr create_history() override
		{
			return std::make_shared<history_t>(globals_, rinfo_);
		}

	private:
		pinba_globals_t              *globals_;
		report_stats_t               *stats_;
		report_info_t                rinfo_;

		report_conf___by_request_t   conf_;
	};

////////////////////////////////////////////////////////////////////////////////////////////////
}} // namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

report_ptr create_report_by_request(pinba_globals_t *globals, report_conf___by_request_t const& conf)
{
	constexpr size_t max_keys = PINBA_LIMIT___MAX_KEY_PARTS;
	size_t const n_keys = conf.keys.size();

	switch (n_keys)
	{
		case 0:
			throw std::logic_error(ff::fmt_str("report_by_request doesn't support 0 keys aggregation"));
		default:
			throw std::logic_error(ff::fmt_str("report_by_request supports up to {0} keys, {1} given", max_keys, n_keys));

	#define CASE(z, N, unused) \
		case N: return std::make_shared<aux::report___by_request_t<N>>(globals, conf); \
	/**/

	BOOST_PP_REPEAT_FROM_TO(1, BOOST_PP_ADD(PINBA_LIMIT___MAX_KEY_PARTS, 1), CASE, 0);

	#undef CASE
	}
}

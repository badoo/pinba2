#include <boost/noncopyable.hpp>
#include <boost/preprocessor/arithmetic/add.hpp>
#include <boost/preprocessor/repetition/repeat.hpp>

#include <meow/defer.hpp>
#include <meow/utility/offsetof.hpp> // MEOW_SELF_FROM_MEMBER

#include "pinba/globals.h"
#include "pinba/histogram.h"
#include "pinba/multi_merge.h"
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

	public:

		struct tick_item_t
		{
			uint64_t key_hash;
			key_t    key;
			data_t   data;


			tick_item_t() = default;

		private: // not movable or copyable
			tick_item_t(tick_item_t const&)            = delete;
			tick_item_t(tick_item_t&&)                 = delete;
			tick_item_t& operator=(tick_item_t const&) = delete;
			tick_item_t& operator=(tick_item_t&&)      = delete;
		};

		struct tick_t : public report_tick_t
		{
			std::deque<tick_item_t>      items;
			std::deque<hdr_histogram_t>  hvs;

			struct nmpa_s                hv_nmpa;

			static constexpr size_t hv_nmpa_default_chunk_size   = 128 * 1024;

		public:

			tick_t()
			{
				nmpa_init(&hv_nmpa, hv_nmpa_default_chunk_size);
			}

			~tick_t()
			{
				nmpa_free(&hv_nmpa);
			}

		private: // not movable or copyable
			tick_t(tick_t const&)            = delete;
			tick_t(tick_t&&)                 = delete;
			tick_t& operator=(tick_t const&) = delete;
			tick_t& operator=(tick_t&&)      = delete;
		};

	public: // aggregator

		struct aggregator_t : public report_agg_t
		{
			// map: key -> offset in tick->items and tick->hvs
			struct hashtable_t
				: public tsl::robin_map<
								  key_t
								, uint32_t
								, report_key_impl___hasher_t
								, report_key_impl___equal_t
								, std::allocator<std::pair<key_t, uint32_t>>
								, /*StoreHash=*/ true>
			{
			};

			uint32_t raw_item_offset_get(key_t const& k)
			{
				uint64_t const key_hash = report_key_impl___hasher_t()(k);

				auto inserted_pair = tick_ht_.emplace_hash(key_hash, k, UINT_MAX);
				uint32_t& off = inserted_pair.first.value();

				// fastpath: mapping exists, item exists, just return
				if (!inserted_pair.second)
					return off;

				// slowpath: create item and maybe hvs

				tick_->items.emplace_back();

				tick_item_t& item = tick_->items.back();
				item.key      = k;
				item.key_hash = key_hash;

				if (conf_.hv_bucket_count > 0)
					tick_->hvs.emplace_back(&tick_->hv_nmpa, hv_conf_);

				assert(tick_->items.size() < size_t(INT_MAX));
				uint32_t const new_off = static_cast<uint32_t>(tick_->items.size() - 1);

				off = new_off;
				return new_off;
			}

			void raw_item_increment(key_t const& k, packet_t const *packet)
			{
				uint32_t const offset = this->raw_item_offset_get(k);

				tick_item_t& item = tick_->items[offset];

				item.data.req_count  += 1;
				item.data.time_total += packet->request_time;
				item.data.ru_utime   += packet->ru_utime;
				item.data.ru_stime   += packet->ru_stime;
				item.data.traffic    += packet->traffic;
				item.data.mem_used   += packet->mem_used;

				if (conf_.hv_bucket_count > 0)
				{
					auto& hv = tick_->hvs[offset];
					hv.increment(hv_conf_, packet->request_time);
				}
			}

		public:

			aggregator_t(pinba_globals_t *globals, report_conf___by_request_t const& conf, report_info_t const& rinfo)
				: globals_(globals)
				, stats_(nullptr)
				, conf_(conf)
				, hv_conf_(histogram___configure_with_rinfo(rinfo))
				, tick_(meow::make_intrusive<tick_t>())
			{
			}

			virtual void stats_init(report_stats_t *stats) override
			{
				stats_ = stats;
			}

			virtual report_tick_ptr tick_now(timeval_t curr_tv) override
			{
				report_tick_ptr result = std::move(tick_);
				tick_ = meow::make_intrusive<tick_t>();

				tick_ht_.clear();
				tick_ht_.rehash(0); // shrinks the hashtable

				return result;
			}

			virtual report_estimates_t get_estimates() override
			{
				report_estimates_t result = {};

				result.row_count = tick_->items.size();

				// tick
				result.mem_used += sizeof(*tick_);

				// tick ht
				result.mem_used += sizeof(tick_ht_);
				result.mem_used += tick_ht_.bucket_count() * sizeof(*tick_ht_.begin());

				// items
				result.mem_used += tick_->items.size() * sizeof(*tick_->items.begin());

				// hvs
				result.mem_used += tick_->hvs.size() * sizeof(*tick_->hvs.begin());
				result.mem_used += nmpa_mem_used(&tick_->hv_nmpa);

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
				this->raw_item_increment(k, packet);

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
			hashtable_t                  tick_ht_;
		};

	public: // history

		struct history_t : public report_history_t
		{
			using ring_t       = report_history_ringbuffer_t;
			using ringbuffer_t = ring_t::ringbuffer_t;

			struct history_tick_t : public report_tick_t // inheric to get compatibility with history ring for free
			{
				// precalculated mem usage, to avoid expensive computation in get_estimates()
				uint64_t                       mem_used = 0;

				std::deque<tick_item_t>        items; // should be the same as aggregator tick items, to move data
				std::vector<flat_histogram_t>  hvs;   // keep this as vector, as we can preallocate (and need to copy anyway)
			};

		public:

			history_t(pinba_globals_t *globals, report_info_t const& rinfo)
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

			virtual void merge_tick(report_tick_ptr tick_base) override
			{
				// re-process tick data, for more compact storage
				auto *agg_tick = static_cast<tick_t*>(tick_base.get());  // src (non-const to move from, see below)
				auto h_tick    = meow::make_intrusive<history_tick_t>(); // dst

				// remember to grab repacker_state
				h_tick->repacker_state = std::move(agg_tick->repacker_state);

				// can MOVE items, since the format is intentionally the same
				h_tick->items = std::move(agg_tick->items);
				h_tick->mem_used += h_tick->items.size() * sizeof(*h_tick->items.begin());

				// migrate histograms, converting them from hashtable to flat
				if (rinfo_.hv_enabled)
				{
					h_tick->hvs.reserve(agg_tick->hvs.size()); // we know the size in advance, mon
					h_tick->mem_used += h_tick->hvs.capacity() * sizeof(*h_tick->hvs.begin());

					for (auto const& src_hv : agg_tick->hvs)
					{
						h_tick->hvs.emplace_back(std::move(histogram___convert_hdr_to_flat(src_hv, hv_conf_)));

						h_tick->mem_used += h_tick->hvs.back().values.capacity() * sizeof(*h_tick->hvs.back().values.begin());
					}

					// sanity
					assert(h_tick->items.size() == agg_tick->hvs.size());
				}

				ring_.append(std::move(h_tick));
			}

			virtual report_estimates_t get_estimates() override
			{
				report_estimates_t result = {};

				result.row_count = [&]() -> uint32_t
				{
					auto const& ringbuf = ring_.get_ringbuffer();
					if (ringbuf.empty())
						return 0;

					uint32_t non_unique_rows = 0;

					for (auto const& tick_base : ringbuf)
					{
						auto const& tick = static_cast<history_tick_t const&>(*tick_base);
						non_unique_rows += tick.items.size();
					}

					// got stats from snapshot merge with exact values, adjust based uniq to total rows ratio
					if (stats_->last_snapshot_src_rows && stats_->last_snapshot_uniq_rows)
					{
						double const uniq_to_raw_fraction = (double)stats_->last_snapshot_uniq_rows / stats_->last_snapshot_src_rows;
						return non_unique_rows * uniq_to_raw_fraction;
					}

					// no stats from snapshot merge yet,
					// use average tick size (aka lean low and assume, all values repeat every tick)
					return (uint32_t)std::ceil((double)non_unique_rows / ringbuf.size());
				}();

				result.mem_used += sizeof(*this);

				for (auto const& tick_base : ring_.get_ringbuffer())
				{
					auto const& tick = static_cast<history_tick_t const&>(*tick_base);

					// tick
					result.mem_used += sizeof(tick);
					result.mem_used += tick.mem_used;
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
					data_t       data;

					// list of saved hvs, we merge only when requested (i.e. in hv_at_position)
					// please note that we're also saving pointers to flat_histogram_t::values
					// and will restore full structs on merge
					// this a 'limitation' of multi_merge() function
					std::vector<histogram_values_t const*>  saved_hv;
					flat_histogram_t                        merged_hv;
				};

				struct hashtable_t
					: public tsl::robin_map<
									  key_t
									, row_t
									, report_key_impl___hasher_t
									, report_key_impl___equal_t
									, std::allocator<std::pair<key_t, row_t>>
									, /*StoreHash=*/ true>
				{
				};

			public:

				static report_key_t key_at_position(hashtable_t const&, typename hashtable_t::iterator const& it)
				{
					return report_key_t { it->first };
				}

				static void* value_at_position(hashtable_t const&, typename hashtable_t::iterator const& it)
				{
					return (void*)&it->second.data;
				}

				static void* hv_at_position(hashtable_t const&, typename hashtable_t::iterator const& it)
				{
					row_t *row = const_cast<row_t*>(&it->second);

					if (row->saved_hv.empty()) // already merged
						return &row->merged_hv;

					struct merger_t
					{
						flat_histogram_t *to;

						inline bool compare(histogram_value_t const& l, histogram_value_t const& r) const
						{
							return l.bucket_id < r.bucket_id;
						}

						inline bool equal(histogram_value_t const& l, histogram_value_t const& r) const
						{
							return l.bucket_id == r.bucket_id;
						}

						inline void reserve(size_t const sz)
						{
							to->values.reserve(sz);
						}

						inline void push_back(histogram_values_t const *seq, histogram_value_t const& v)
						{
							flat_histogram_t *src = MEOW_SELF_FROM_MEMBER(flat_histogram_t, values, seq);

							bool const should_insert = [&]()
							{
								if (to->values.empty())
									return true;

								return !equal(to->values.back(), v);
							}();

							if (should_insert)
							{
								to->values.emplace_back(v);
							}
							else
							{
								to->values.back().value += v.value;
							}
						}
					};

					// merge histogram values
					merger_t merger = { .to = &row->merged_hv };
					pinba::multi_merge(&merger, row->saved_hv.begin(), row->saved_hv.end());

					// merge histogram totals
					for (auto const *src_hv_values : row->saved_hv)
					{
						flat_histogram_t *src = MEOW_SELF_FROM_MEMBER(flat_histogram_t, values, src_hv_values);
						row->merged_hv.total_count  += src->total_count;
						row->merged_hv.negative_inf += src->negative_inf;
						row->merged_hv.positive_inf += src->positive_inf;
					}

					// clear source
					row->saved_hv.clear();

					return &row->merged_hv;
				}

				static void calculate_raw_stats(report_snapshot_ctx_t *snapshot_ctx, src_ticks_t const& ticks, report_raw_stats_t *stats)
				{
					for (auto const& tick_base : ticks)
					{
						if (!tick_base)
							continue;

						auto const& tick = static_cast<history_tick_t const&>(*tick_base);

						stats->row_count += tick.items.size();
					}
				}

				static void calculate_totals(report_snapshot_ctx_t *snapshot_ctx, hashtable_t const& data, totals_t *totals)
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
					bool const need_histograms = (snapshot_ctx->rinfo.hv_enabled && (flags & report_snapshot_t::merge_flags::with_histograms));

					// if we have result count estimate - try reserve the space in resulting hashtable
					// this might get ugly, if we under-guess just slightly
					// and face a giant rehash at the end of the merge
					// but should save us some significant time on initial few rehashes
					if (snapshot_ctx->estimates.row_count > 0)
					{
						to.reserve(snapshot_ctx->estimates.row_count);
					}

					uint64_t n_ticks = 0;
					uint64_t key_lookups = 0;
					uint64_t hv_appends = 0;

					for (auto const& tick_base : ticks)
					{
						if (!tick_base)
							continue;

						auto const& tick = static_cast<history_tick_t const&>(*tick_base);

						n_ticks++;

						for (size_t i = 0; i < tick.items.size(); i++)
						{
							tick_item_t const& src = tick.items[i];

							auto inserted_pair = to.emplace_hash(src.key_hash, src.key, row_t{});
							row_t&         dst = inserted_pair.first.value();

							dst.data.req_count  += src.data.req_count;
							dst.data.time_total += src.data.time_total;
							dst.data.ru_utime   += src.data.ru_utime;
							dst.data.ru_stime   += src.data.ru_stime;
							dst.data.traffic    += src.data.traffic;
							dst.data.mem_used   += src.data.mem_used;

							if (need_histograms)
							{
								flat_histogram_t const& src_hv = tick.hvs[i];

								// try preallocate
								if (dst.saved_hv.empty())
									dst.saved_hv.reserve(ticks.size());

								dst.saved_hv.push_back(&src_hv.values);
							}
						}

						key_lookups += tick.items.size();

						if (need_histograms)
							hv_appends  += tick.hvs.size();
					}

					LOG_DEBUG(snapshot_ctx->logger(), "prepare '{0}'; n_ticks: {1}, key_lookups: {2}, hv_appends: {3}",
						snapshot_ctx->rinfo.name, n_ticks, key_lookups, hv_appends);

					// can clean ticks only if histograms were not merged
					// since histogram merger uses raw pointers to tick_data_t::hvs[]::values
					// and those need to be alive while this snapshot is alive
					if (!need_histograms)
					{
						ticks.clear();
						ticks.shrink_to_fit();
					}
				}
			};

			virtual report_snapshot_ptr get_snapshot() override
			{
				report_snapshot_ctx_t const sctx = {
					.globals        = globals_,
					.stats          = stats_,
					.rinfo          = rinfo_,
					.estimates      = this->get_estimates(),
					.hv_conf        = hv_conf_,
				};

				using snapshot_t = report_snapshot__impl_t<snapshot_traits>;
				return meow::make_unique<snapshot_t>(sctx, ring_.get_ringbuffer());
			}

		private:
			pinba_globals_t              *globals_;
			report_stats_t               *stats_;
			report_info_t                rinfo_;
			histogram_conf_t             hv_conf_;

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
			return std::make_shared<aggregator_t>(globals_, conf_, rinfo_);
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

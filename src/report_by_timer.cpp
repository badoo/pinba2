// #include <wchar.h> // wmemcmp

#include <functional>
#include <utility>

#include <boost/preprocessor/arithmetic/add.hpp>
#include <boost/preprocessor/repetition/repeat.hpp>

#include <meow/utility/offsetof.hpp> // MEOW_SELF_FROM_MEMBER

#include <tsl/robin_map.h>

#include "misc/nmpa.h"

#include "pinba/globals.h"
#include "pinba/bloom.h"
#include "pinba/histogram.h"
#include "pinba/multi_merge.h"
#include "pinba/packet.h"
#include "pinba/report.h"
#include "pinba/report_util.h"
#include "pinba/report_by_timer.h"

////////////////////////////////////////////////////////////////////////////////////////////////
namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

	template<size_t NKeys>
	struct report___by_timer_t : public report_t
	{
		typedef report_key_impl_t<NKeys>      key_t;
		typedef report_row_data___by_timer_t  data_t;

	public: // key extraction and transformation

		typedef meow::string_ref<report_key_t::value_type> key_subrange_t;

		struct key_info_t
		{
			template<class T>
			using chunk_t = meow::chunk<T, NKeys, uint32_t>;

			using key_descriptor_t = report_conf___by_timer_t::key_descriptor_t;

			struct descriptor_t
			{
				key_descriptor_t  d;
				uint32_t          remap_from;  // offset in split_key_d
				uint32_t          remap_to;    // offset in conf.key_d
			};

			typedef chunk_t<descriptor_t>          rkd_chunk_t;
			typedef meow::string_ref<descriptor_t> rkd_range_t;

			// key descriptors grouped by kind
			rkd_chunk_t split_key_d;

			// these are ranges, describing which keys are where in split_key_d
			rkd_range_t request_tag_r;
			rkd_range_t request_field_r;
			rkd_range_t timer_tag_r;

			void from_config(report_conf___by_timer_t const& conf)
			{
				request_tag_r   = split_descriptors_by_kind(conf, RKD_REQUEST_TAG);
				request_field_r = split_descriptors_by_kind(conf, RKD_REQUEST_FIELD);
				timer_tag_r     = split_descriptors_by_kind(conf, RKD_TIMER_TAG);
			}

			// copy all descriptors with given kind to split_key_d
			// and return range pointing to where they are now
			// also updates remap_key_d with data to reverse the mapping
			rkd_range_t split_descriptors_by_kind(report_conf___by_timer_t const& conf, int kind)
			{
				auto const& key_d = conf.keys;
				uint32_t const size_before = split_key_d.size();

				for (uint32_t i = 0; i < key_d.size(); ++i)
				{
					if (key_d[i].kind == kind)
					{
						descriptor_t const d = {
							.d          = key_d[i],
							.remap_from = split_key_d.size(),
							.remap_to   = i,
						};

						split_key_d.push_back(d);
					}
				}

				return rkd_range_t { split_key_d.begin() + size_before, split_key_d.size() - size_before };
			}

			key_subrange_t rtag_key_subrange(key_t& k) const
			{
				return { k.begin() + (request_tag_r.begin() - split_key_d.begin()), request_tag_r.size() };
			}

			key_subrange_t rfield_key_subrange(key_t& k) const
			{
				return { k.begin() + (request_field_r.begin() - split_key_d.begin()), request_field_r.size() };
			}

			key_subrange_t timertag_key_subrange(key_t& k) const
			{
				return { k.begin() + (timer_tag_r.begin() - split_key_d.begin()), timer_tag_r.size() };
			}

			key_t remap_key(key_t const& flat_key) const
			{
				key_t result = {}; // avoid might-be-unitialized warning
				assert(split_key_d.size() == result.size());

				for (auto const& d : split_key_d)
				{
					result[d.remap_to] = flat_key[d.remap_from];
				}

				return result;
			}

			void remap_key_to_from(key_t& result, key_t const& flat_key) const
			{
				assert(split_key_d.size() == result.size());

				// for (uint32_t i = 0; i < result.size(); i++)
				// {
				//     descriptor_t const& descriptor = split_key_d[i];
				//     result[d.remap_to] = flat_key[i];
				// }

				for (auto const& d : split_key_d)
				{
					result[d.remap_to] = flat_key[d.remap_from];
				}
			}
		};

	public: // shared structures for aggregator/history

		// single row of current tick aggregation
		struct tick_item_t
		{
			uint64_t         last_unique;

			uint64_t         key_hash;

			data_t           data;
			hdr_histogram_t  hv;

			tick_item_t(uint64_t kh, struct nmpa_s *nmpa, histogram_conf_t const& hv_conf)
				: last_unique(0)
				, key_hash(kh)
				, data()
				, hv(nmpa, hv_conf)
			{
			}

			~tick_item_t()
			{
				// NOTE: this dtor is never called!
				// since we allocate this object in nmpa and never destroy (only free memory in tick->item_nmpa)
				// therefore this->hv dtor is never called either (memory is freed by tick->hv_nmpa)
			}

		private: // not movable or copyable
			tick_item_t(tick_item_t const&) = delete;
			tick_item_t& operator=(tick_item_t const&) = delete;

			tick_item_t(tick_item_t&&) = delete;
			tick_item_t& operator=(tick_item_t&&) = delete;
		};

		// map: key -> pointer to item allocated in nmpa
		struct agg_hashtable_t
			: public tsl::robin_map<
							  key_t
							, tick_item_t*
							, report_key_impl___hasher_t
							, report_key_impl___equal_t
							, std::allocator<std::pair<key_t, tick_item_t*>>
							, /*StoreHash=*/ true>
		{
		};

		struct tick_t : public report_tick_t
		{
			agg_hashtable_t  ht;
			struct nmpa_s    item_nmpa;
			struct nmpa_s    hv_nmpa;

			static constexpr size_t item_nmpa_default_chunk_size = 128 * 1024;
			static constexpr size_t hv_nmpa_default_chunk_size   = 128 * 1024;

		public:

			tick_t()
			{
				nmpa_init(&item_nmpa, item_nmpa_default_chunk_size);
				nmpa_init(&hv_nmpa, hv_nmpa_default_chunk_size);
			}

			~tick_t()
			{
				nmpa_free(&hv_nmpa);
				nmpa_free(&item_nmpa);
			}

		private: // not movable or copyable
			tick_t(tick_t const&)            = delete;
			tick_t(tick_t&&)                 = delete;
			tick_t& operator=(tick_t const&) = delete;
			tick_t& operator=(tick_t&&)      = delete;
		};

	public: // aggregation

		struct aggregator_t : public report_agg_t
		{
			tick_item_t& raw_item_reference(key_t const& k)
			{
				uint64_t const key_hash = report_key_impl___hasher_t()(k);

				auto inserted_pair = tick_->ht.emplace_hash(key_hash, k, nullptr);
				tick_item_t *& item_ptr = inserted_pair.first.value();

				// mapping exists, item exists, just return
				if (!inserted_pair.second)
					return *item_ptr;

				// slowpath - create item and maybe hvs

				tick_item_t *new_item = (tick_item_t*)nmpa_alloc(&tick_->item_nmpa, sizeof(tick_item_t));
				if (new_item == nullptr)
					throw std::bad_alloc();

				new (new_item) tick_item_t(key_hash, &tick_->hv_nmpa, hv_conf_);

				item_ptr = new_item;
				return *item_ptr;
			}

			void raw_item_increment(key_t const& k, packet_t const *packet, packed_timer_t const *timer)
			{
				tick_item_t& item = this->raw_item_reference(k);

				item.data.hit_count  += timer->hit_count;
				item.data.time_total += timer->value;
				item.data.ru_utime   += timer->ru_utime;
				item.data.ru_stime   += timer->ru_stime;

				if (item.last_unique != packet_unqiue_)
				{
					item.data.req_count += 1;
					item.last_unique    = packet_unqiue_;
				}

				if (conf_.hv_bucket_count > 0)
				{
					hdr_histogram_t& hv = item.hv;

					// optimize common case when hit_count == 1, and there is no need to divide
					if (__builtin_expect(timer->hit_count == 1, 1))
					{
						hv.increment(hv_conf_, timer->value);
					}
					else
					{
						hv.increment(hv_conf_, (timer->value / timer->hit_count), timer->hit_count);
					}
				}
			}

		public:

			aggregator_t(pinba_globals_t *globals, report_conf___by_timer_t const& conf, report_info_t const& rinfo)
				: globals_(globals)
				, stats_(nullptr)
				, conf_(conf)
				, hv_conf_(histogram___configure_with_rinfo(rinfo))
				, packet_unqiue_(1) // init this to 1, so it's different from 0 in default constructed data_t
				, tick_(meow::make_intrusive<tick_t>())
			{
				// key info
				ki_.from_config(conf);

				// bloom
				{
					for (auto const& kd : conf_.keys)
					{
						if (RKD_TIMER_TAG != kd.kind)
							continue;

						packet_bloom_.add(kd.timer_tag);
						timer_bloom_.add(kd.timer_tag);
					}

					for (auto const& ttf : conf_.timertag_filters)
					{
						packet_bloom_.add(ttf.name_id);
						timer_bloom_.add(ttf.name_id);
					}
				}
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

				// tick
				result.mem_used += sizeof(*tick_);

				// tick ht
				result.mem_used += sizeof(tick_->ht);
				result.mem_used += tick_->ht.bucket_count() * sizeof(*tick_->ht.begin());

				// pools
				result.mem_used += nmpa_mem_used(&tick_->item_nmpa);
				result.mem_used += nmpa_mem_used(&tick_->hv_nmpa);

				return result;
			}

			virtual void add(packet_t *packet) override
			{
				// packet-level bloom check
				// FIXME: this probably immediately causes L1/L2 miss, since bloom is at the end of packet struct ?
				if (!packet->bloom.contains(this->packet_bloom_))
				{
					// LOG_DEBUG(globals_->logger(), "packet: {0} !< {1}", packet->timer_bloom->to_string(), packet_bloom_.to_string());
					stats_->packets_dropped_by_bloom++;
					return;
				}

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

				// check if timer is interesting (aka satisfies filters)
				auto const filter_by_timer_tags = [&](packed_timer_t const *t) -> bool
				{
					for (auto const& tfd : conf_.timertag_filters)
					{
						bool tag_exists = false;

						for (uint32_t tag_i = 0; tag_i < t->tag_count; ++tag_i)
						{
							if (t->tag_name_ids[tag_i] != tfd.name_id)
								continue;

							tag_exists = true;

							if (t->tag_value_ids[tag_i] != tfd.value_id)
								return false;
						}

						if (!tag_exists)
							return false;
					}
					return true;
				};

				// put key data into out_range if timer has all the parts
				auto const fetch_by_timer_tags = [&](key_info_t const& ki, key_subrange_t out_range, packed_timer_t const *t) -> bool
				{
					uint32_t const n_tags_required = out_range.size();

					for (uint32_t i = 0; i < n_tags_required; ++i)
					{
						bool tag_found = false;

						for (uint32_t tag_i = 0; tag_i < t->tag_count; ++tag_i)
						{
							if (t->tag_name_ids[tag_i] != ki.timer_tag_r[i].d.timer_tag)
								continue;

							out_range[i] = t->tag_value_ids[tag_i];
							tag_found = true;
							break;
						}

						// each full run of inner loop - must get us a tag
						// so if it didn't -> just return false
						if (!tag_found)
							return false;
					}

					return true;
				};

				auto const find_request_tags = [&](key_info_t const& ki, key_t *out_key) -> bool
				{
					key_subrange_t out_range = ki_.rtag_key_subrange(*out_key);

					uint32_t const n_tags_required = out_range.size();

					for (uint32_t tag_i = 0; tag_i < n_tags_required; ++tag_i)
					{
						bool tag_found = false;

						for (uint16_t i = 0, i_end = packet->tag_count; i < i_end; ++i)
						{
							if (packet->tag_name_ids[i] != ki.request_tag_r[tag_i].d.request_tag)
								continue;

							out_range[tag_i] = packet->tag_value_ids[i];
							tag_found = true;
							break;
						}

						// each full run of inner loop - must get us a tag
						// so if it didn't -> just return false
						if (!tag_found)
							return false;
					}

					return true;
				};

				auto const find_request_fields = [&](key_info_t const& ki, key_t *out_key) -> bool
				{
					key_subrange_t out_range = ki_.rfield_key_subrange(*out_key);

					for (uint32_t i = 0; i < ki.request_field_r.size(); ++i)
					{
						out_range[i] = packet->*ki.request_field_r[i].d.request_field;

						if (out_range[i] == 0)
							return false;
					}

					return true;
				};

				key_t key_inprogress;

				bool const tags_found = find_request_tags(ki_, &key_inprogress);
				if (!tags_found)
				{
					stats_->packets_dropped_by_rtag++;
					return;
				}

				bool const fields_found = find_request_fields(ki_, &key_inprogress);
				if (!fields_found)
				{
					stats_->packets_dropped_by_rfield++;
					return;
				}

				// need to scan all timers, find matching and increment for each one
				// use local counters to save on atomics
				uint32_t timers_scanned            = 0;
				uint32_t timers_aggregated         = 0;
				uint32_t timers_skipped_by_bloom   = 0;
				uint32_t timers_skipped_by_filters = 0;
				uint32_t timers_skipped_by_tags    = 0;
				{
					packet_unqiue_++; // next unique, since this is the new packet add

					key_subrange_t const timer_key_range = ki_.timertag_key_subrange(key_inprogress);

					for (uint16_t i = 0; i < packet->timer_count; ++i)
					{
						timer_bloom_t const *tbloom = &packet->timers_blooms[i];
						packed_timer_t const *timer = &packet->timers[i];

						timers_scanned++;

						if (!tbloom->contains(this->timer_bloom_))
						{
							// LOG_DEBUG(globals_->logger(), "timer[{0}]: bloom {1} !< {2}", i, timer->bloom.to_string(), timer_bloom_.to_string());
							timers_skipped_by_bloom++;
							continue;
						}

						bool const timer_ok = filter_by_timer_tags(timer);
						if (!timer_ok) {
							timers_skipped_by_filters++;
							continue;
						}

						bool const timer_found = fetch_by_timer_tags(ki_, timer_key_range, timer);
						if (!timer_found) {
							timers_skipped_by_tags++;
							continue;
						}

						timers_aggregated++;

						// LOG_DEBUG(globals_->logger(), "found key '{0}'", key_to_string(key_inprogress));

						// key_t const k = ki_.remap_key(key_inprogress);
						key_t k = {};
						ki_.remap_key_to_from(k, key_inprogress);

						// LOG_DEBUG(globals_->logger(), "remapped key '{0}'", key_to_string(k));

						// finally - find and update item
						this->raw_item_increment(k, packet, timer);
					}
				}

				stats_->timers_scanned            += timers_scanned;
				stats_->timers_aggregated         += timers_aggregated;
				stats_->timers_skipped_by_bloom   += timers_skipped_by_bloom;
				stats_->timers_skipped_by_filters += timers_skipped_by_filters;
				stats_->timers_skipped_by_tags    += timers_skipped_by_tags;

				if (!timers_aggregated)
					stats_->packets_dropped_by_timertag++;
				else
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
			report_conf___by_timer_t     conf_;
			histogram_conf_t             hv_conf_;

			uint64_t                     packet_unqiue_;

			key_info_t                   ki_;

			timertag_bloom_t             packet_bloom_;
			timer_bloom_t                timer_bloom_;

			boost::intrusive_ptr<tick_t> tick_;
		};

	public: // history

		struct history_t : public report_history_t
		{
			using ring_t       = report_history_ringbuffer_t;
			using ringbuffer_t = ring_t::ringbuffer_t;

			struct history_row_t
			{
				uint64_t          key_hash;
				key_t             key;
				data_t            data;
				flat_histogram_t  hv;
			};

			struct history_tick_t : public report_tick_t // not required to inherit here, but get history ring for free
			{
				// precalculated mem usage, to avoid expensive computation in get_estimates()
				uint64_t                   mem_used = 0;

				std::vector<history_row_t> rows    = {};
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
				auto *agg_tick = static_cast<tick_t const*>(tick_base.get()); // src (non-const to move from, see below)
				auto    h_tick = meow::make_intrusive<history_tick_t>();      // dst

				// remember to grab repacker_state
				h_tick->repacker_state = std::move(agg_tick->repacker_state);

				// reserve, we know the size
				h_tick->rows.reserve(agg_tick->ht.size());
				h_tick->mem_used += h_tick->rows.capacity() * sizeof(*h_tick->rows.begin());

				for (auto const& ht_pair : agg_tick->ht)
				{
					h_tick->rows.emplace_back();

					key_t       const& src_key  = ht_pair.first;
					tick_item_t const& src_item = *ht_pair.second;
					history_row_t    & dst_row  = h_tick->rows.back();

					dst_row.key_hash = src_item.key_hash;
					dst_row.key      = src_key;
					dst_row.data     = src_item.data;

					// FIXME: only convert if histograms are enabled!
					dst_row.hv       = std::move(histogram___convert_hdr_to_flat(src_item.hv, hv_conf_));
					h_tick->mem_used += dst_row.hv.values.capacity() * sizeof(*dst_row.hv.values.begin());
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
						non_unique_rows += tick.rows.size();
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

					result.mem_used += sizeof(tick);
					result.mem_used += tick.mem_used;
				}

				return result;
			}

		public: // snapshot

			struct snapshot_traits
			{
				using src_ticks_t = ringbuffer_t;
				using totals_t    = report_row_data___by_timer_t;

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
					row_t *row = const_cast<row_t*>(&it->second); // will mutate the row, potentially

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

						stats->row_count += tick.rows.size();
					}
				}

				static void calculate_totals(report_snapshot_ctx_t *snapshot_ctx, hashtable_t const& data, totals_t *totals)
				{
					for (auto const& data_pair : data)
					{
						auto const& row = data_pair.second.data;

						totals->req_count  += row.req_count;
						totals->hit_count  += row.hit_count;
						totals->time_total += row.time_total;
						totals->ru_utime   += row.ru_utime;
						totals->ru_stime   += row.ru_stime;
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

						for (size_t i = 0; i < tick.rows.size(); i++)
						{
							history_row_t const& src = tick.rows[i];

							auto inserted_pair = to.emplace_hash(src.key_hash, src.key, row_t{});
							row_t&         dst = inserted_pair.first.value();

							dst.data.req_count  += src.data.req_count;
							dst.data.hit_count  += src.data.hit_count;
							dst.data.time_total += src.data.time_total;
							dst.data.ru_utime   += src.data.ru_utime;
							dst.data.ru_stime   += src.data.ru_stime;

							if (need_histograms)
							{
								flat_histogram_t const& src_hv = src.hv;

								// try preallocate
								if (dst.saved_hv.empty())
									dst.saved_hv.reserve(ticks.size());

								dst.saved_hv.push_back(&src_hv.values);
							}
						}

						key_lookups += tick.rows.size();

						if (need_histograms)
							hv_appends  += tick.rows.size();
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
					.nmpa           = {} // don't need this at the moment
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

		report___by_timer_t(pinba_globals_t *globals, report_conf___by_timer_t const& conf)
			: globals_(globals)
			, stats_(nullptr)
			, conf_(conf)
		{
			assert(conf_.keys.size() == NKeys);

			rinfo_ = report_info_t {
				.name            = conf_.name,
				.kind            = REPORT_KIND__BY_TIMER_DATA,
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
		pinba_globals_t           *globals_;
		report_stats_t            *stats_;
		report_info_t             rinfo_;

		report_conf___by_timer_t  conf_;
	};

////////////////////////////////////////////////////////////////////////////////////////////////
}} // namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

report_ptr create_report_by_timer(pinba_globals_t *globals, report_conf___by_timer_t const& conf)
{
	constexpr size_t max_keys = PINBA_LIMIT___MAX_KEY_PARTS;
	size_t const n_keys = conf.keys.size();

	switch (n_keys)
	{
		case 0:
			throw std::logic_error(ff::fmt_str("report_by_timer doesn't support 0 keys aggregation"));
		default:
			throw std::logic_error(ff::fmt_str("report_by_timer supports up to {0} keys, {1} given", max_keys, n_keys));

	#define CASE(z, N, unused) \
		case N: return std::make_shared<aux::report___by_timer_t<N>>(globals, conf); \
	/**/

	BOOST_PP_REPEAT_FROM_TO(1, BOOST_PP_ADD(PINBA_LIMIT___MAX_KEY_PARTS, 1), CASE, 0);

	#undef CASE
	}
}

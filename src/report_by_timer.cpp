// #include <wchar.h> // wmemcmp

#include <functional>
#include <utility>

#include <meow/stopwatch.hpp>
#include <meow/utility/offsetof.hpp> // MEOW_SELF_FROM_MEMBER

#include <sparsehash/dense_hash_map>

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

		struct item_t
			// : private boost::noncopyable // bring this back, when we remove copy ctor
		{
			// last unique packet we've incremented data from
			//  this one is used to detect multiple timers being merged from one packet_t
			//  and we need to increment data.req_count only once per add() call
			//
			uint64_t    last_unique;

			data_t      data;
			histogram_t hv;

			item_t()
				: last_unique(0)
				, data()
				, hv()
			{
			}

			// FIXME: only used by dense_hash_map for set_empty_key()
			//        should not be called often with huge histograms, so expect it to be ok :(
			//        sparsehash with c++11 support (https://github.com/sparsehash/sparsehash-c11) fixes this
			//        but gcc 4.9.4 doesn't support the type_traits it requires
			//        so live this is for now, but probably - move to gcc6 or something
			item_t(item_t const& other)
				: last_unique(other.last_unique)
				, data(other.data)
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
				last_unique = other.last_unique;    // a copy
				data        = other.data;           // a copy
				hv          = std::move(other.hv);  // real move
			}

			void data_increment(packed_timer_t const *timer)
			{
				data.hit_count  += timer->hit_count;
				data.time_total += timer->value;
				data.ru_utime   += timer->ru_utime;
				data.ru_stime   += timer->ru_stime;
			}

			void packet_increment(packet_t *packet, uint64_t unique)
			{
				if (unique == last_unique)
					return;

				data.req_count  += 1;

				last_unique     = unique;
			}

			void hv_increment(packed_timer_t const *timer, uint32_t hv_bucket_count, duration_t hv_bucket_d)
			{
				hv.increment({hv_bucket_count, hv_bucket_d}, (timer->value / timer->hit_count));
			}

			void merge_other(item_t const& other)
			{
				// data
				data.req_count  += other.data.req_count;
				data.hit_count  += other.data.hit_count;
				data.time_total += other.data.time_total;
				data.ru_utime   += other.data.ru_utime;
				data.ru_stime   += other.data.ru_stime;

				// hv
				hv.merge_other(other.hv);
			}
		};

	private: // raw data

		struct raw_hashtable_t
			: public google::dense_hash_map<key_t, item_t, report_key_impl___hasher_t, report_key_impl___equal_t>
		{
			raw_hashtable_t()
			{
				this->set_empty_key(report_key_impl___make_empty<NKeys>());
			}
		};

		raw_hashtable_t raw_data_;

	private: // tick data

		struct tick_data_t
		{
			std::vector<key_t>             keys;
			std::vector<data_t>            datas;
			std::vector<flat_histogram_t>  hvs;
		};

		using ticks_t       = ticks_ringbuffer_t<tick_data_t>;
		using tick_t        = typename ticks_t::tick_t;
		using ticks_list_t  = typename ticks_t::ringbuffer_t;

	public: // snapshot

		struct snapshot_traits
		{
			using src_ticks_t = ticks_list_t;

			struct row_t
			{
				data_t       data;

				// list of saved hvs, we merge only when requested (i.e. in hv_at_position)
				// please not that we're also saving pointers to flat_histogram_t::values
				// and will restore full structs on merge
				// this a 'limitation' of multi_merge() function
				std::vector<histogram_values_t const*> saved_hv;
				flat_histogram_t merged_hv;
			};

			struct hashtable_t
				: public google::dense_hash_map<key_t, row_t, report_key_impl___hasher_t, report_key_impl___equal_t>
			{
				hashtable_t()
				{
					this->set_empty_key(report_key_impl___make_empty<NKeys>());
				}
			};

			static report_key_t key_at_position(hashtable_t const&, typename hashtable_t::iterator const& it)
			{
				return report_key_t { it->first };
			}

			static void* value_at_position(hashtable_t const&, typename hashtable_t::iterator const& it)
			{
				return &it->second.data;
			}

			static void* hv_at_position(hashtable_t const&, typename hashtable_t::iterator const& it)
			{
				row_t *row = &it->second;

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
					row->merged_hv.total_value += src->total_value;
					row->merged_hv.inf_value   += src->inf_value;
				}

				// clear source
				row->saved_hv.clear();

				return &row->merged_hv;
			}

			// merge from src ringbuffer to snapshot data
			static void merge_ticks_into_data(
				  pinba_globals_t *globals
				, report_info_t& rinfo
				, src_ticks_t& ticks
				, hashtable_t& to
				, report_snapshot_t::prepare_type_t ptype)
			{
#if 0
				struct merger_t
				{
					hashtable_t     *to;
					report_info_t   *rinfo;
					pinba_globals_t *globals;

					uint64_t n_compare_calls;

					inline bool compare(key_t const& l, key_t const& r)
					{
						// static_assert(sizeof(key_t::value_type) == sizeof(wchar_t), "wmemchr operates on whcar_t, and we pass key data there");

						// assert(l.size() == r.size());

						++n_compare_calls;
						// return wmemcmp((wchar_t*)l.data(), (wchar_t*)r.data(), l.size());
						// return std::lexicographical_compare(l.begin(), l.end(), r.begin(), r.end());
						return l < r;
					}

					inline bool equal(key_t const& l, key_t const& r)
					{
						// assert(l.size() == r.size());
						// return std::equal(l.begin(), l.end(), r.begin());
						return l == r;
					}

					inline void reserve(size_t const sz)
					{
						to->reserve(sz);
					}

					inline void push_back(std::vector<key_t> const *seq, key_t const& v)
					{
						tick_data_t *td = MEOW_SELF_FROM_MEMBER(tick_data_t, keys, seq);
						size_t v_offset = &v - &((*seq)[0]);

						auto const should_insert = [&]()
						{
							if (to->empty())
								return true;

							// return (0 != compare(to->back().key, v));
							return !equal(to->back().key, v);
						}();

						if (should_insert)
						{
							row_t row;
							row.key  = v;
							row.data = td->datas[v_offset];

							if (rinfo->hv_enabled)
							{
								row.hv.merge_other(td->hvs[v_offset]);
							}

							// LOG_DEBUG(globals->logger(), "prev empty, adding {0}", report_key_to_string(v));

							to->push_back(row);

							return;
						}

						row_t& prev_row = to->back();

						data_t      & dst_data = prev_row.data;
						data_t const& src_data = td->datas[v_offset];

						dst_data.req_count  += src_data.req_count;
						dst_data.hit_count  += src_data.hit_count;
						dst_data.time_total += src_data.time_total;
						dst_data.ru_utime   += src_data.ru_utime;
						dst_data.ru_stime   += src_data.ru_stime;

						if (rinfo->hv_enabled)
						{
							prev_row.hv.merge_other(td->hvs[v_offset]);
						}

						// LOG_DEBUG(globals->logger(), "prev is equal, merging {0}", report_key_to_string(v));
					}
				};

				// merge input
				std::vector<key_t> const*  td[ticks.size()];
				std::vector<key_t> const** td_end = td;

				for (auto& tick : ticks)
				{
					if (!tick)
						continue;

					*td_end++ = &tick->data.keys;
				}

				// merge destination
				merger_t merger = {
					.to      = &to,
					.rinfo   = &rinfo,
					.globals = globals,
					.n_compare_calls = 0,
				};

				pinba::multi_merge(&merger, td, td_end);

				LOG_DEBUG(globals->logger(), "{0} done; n_ticks: {1}, n_compare_calls: {2}, result_length: {3}",
					__func__, (td_end - td), merger.n_compare_calls, merger.to->size());
#endif

				bool const need_histograms = (rinfo.hv_enabled && ptype != report_snapshot_t::prepare_type::no_histograms);

				uint64_t n_ticks = 0;
				uint64_t key_lookups = 0;
				uint64_t hv_appends = 0;

				for (auto const& tick : ticks)
				{
					if (!tick)
						continue;

					n_ticks++;

					for (size_t i = 0; i < tick->data.keys.size(); i++)
					{
						row_t       & dst      = to[tick->data.keys[i]];
						data_t const& src_data = tick->data.datas[i];

						dst.data.req_count  += src_data.req_count;
						dst.data.hit_count  += src_data.hit_count;
						dst.data.time_total += src_data.time_total;
						dst.data.ru_utime   += src_data.ru_utime;
						dst.data.ru_stime   += src_data.ru_stime;

						if (need_histograms)
						{
							flat_histogram_t const& src_hv = tick->data.hvs[i];

							// try preallocate
							if (dst.saved_hv.empty())
								dst.saved_hv.reserve(ticks.size());

							hv_appends++;
							dst.saved_hv.push_back(&src_hv.values);
						}
					}

					key_lookups += tick->data.keys.size();
				}

				LOG_DEBUG(globals->logger(), "prepare '{0}'; n_ticks: {1}, key_lookups: {2}, hv_appends: {3}",
					rinfo.name, n_ticks, key_lookups, hv_appends);

				// can clean ticks only if histograms were not merged
				// since histogram merger uses raw pointers to tick_data_t::hvs[]::values
				// and those need to be alive while this snapshot is alive
				if (!need_histograms)
					ticks.clear();
			}
		};

		using snapshot_t = report_snapshot__impl_t<snapshot_traits>;

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
		};

	private:
		pinba_globals_t           *globals_;
		report_stats_t            *stats_;
		report_conf___by_timer_t  conf_;

		report_info_t             info_;
		key_info_t                ki_;

		ticks_t                   ticks_;

		uint64_t                  packet_unqiue_;

		timertag_bloom_t          timer_bloom_;

	public:

		report___by_timer_t(pinba_globals_t *globals, report_conf___by_timer_t const& conf)
			: globals_(globals)
			, stats_(nullptr)
			, conf_(conf)
			, ticks_(conf.tick_count)
			, packet_unqiue_{1} // init this to 1, so it's different from 0 in default constructed data_t
		{
			assert(conf_.keys.size() == NKeys);

			info_ = report_info_t {
				.name            = conf_.name,
				.kind            = REPORT_KIND__BY_TIMER_DATA,
				.time_window     = conf_.time_window,
				.tick_count      = conf_.tick_count,
				.n_key_parts     = (uint32_t)conf_.keys.size(),
				.hv_enabled      = (conf_.hv_bucket_count > 0),
				// .hv_kind         = HISTOGRAM_KIND__HASHTABLE,
				.hv_kind         = HISTOGRAM_KIND__FLAT,
				.hv_bucket_count = conf_.hv_bucket_count,
				.hv_bucket_d     = conf_.hv_bucket_d,
			};

			// bloom
			{
				for (auto const& kd : conf_.keys)
				{
					if (RKD_TIMER_TAG != kd.kind)
						continue;

					timer_bloom_.add(kd.timer_tag);
				}

				for (auto const& ttf : conf_.timertag_filters)
					timer_bloom_.add(ttf.name_id);
			}

			ki_.from_config(conf);
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
			tick_t *tick = &ticks_.current();
			tick_data_t& td = tick->data;

			meow::stopwatch_t sw;

			// NOTE: since we're using hash based merger for main keys, there is no need to sort anything

			// this builds an array of pointers to raw_data_ hashtable values (aka std::pair<key_t, item_t>*)
			// then sorts pointers, according to keys, inside the hash
			// and then we're going to copy full values to destination already sorted
			// struct sort_elt_t
			// {
			// 	typename raw_hashtable_t::value_type const *ptr;
			// };

			// // fill
			// std::vector<sort_elt_t> raw_data_pointers;
			// raw_data_pointers.reserve(raw_data_.size());

			// for (auto const& raw_pair : raw_data_)
			// 	raw_data_pointers.push_back({&raw_pair});

			// assert(raw_data_pointers.size() == raw_data_.size());

			// LOG_DEBUG(globals_->logger(), "{0}/{1} tick sort data prepared, elapsed: {2}", name(), curr_tv, sw.stamp());

			// // sort
			// sw.reset();

			// std::sort(raw_data_pointers.begin(), raw_data_pointers.end(),
			// 	[](sort_elt_t const& l, sort_elt_t const& r) { return l.ptr->first < r.ptr->first; });

			// LOG_DEBUG(globals_->logger(), "{0}/{1} tick data sorted, elapsed: {2}", name(), curr_tv, sw.stamp());

			// can copy now
			sw.reset();

			// reserve memory in advance, since we know the final size
			// this is really fast (as we haven't touched this memory yet), not worth measuring generally
			{
				td.keys.reserve(raw_data_.size());
				td.datas.reserve(raw_data_.size());

				if (info_.hv_enabled)
					td.hvs.reserve(raw_data_.size());
			}

			// copy, according to pointers
			// for (auto const& sort_elt : raw_data_pointers)

			for (auto const& raw_pair : raw_data_)
			{
				td.keys.push_back(raw_pair.first);
				td.datas.push_back(raw_pair.second.data);

				if (info_.hv_enabled)
				{
					histogram_t const& src_hv = raw_pair.second.hv;
					td.hvs.push_back(histogram___convert_ht_to_flat(src_hv));
				}
			}

			// LOG_DEBUG(globals_->logger(), "{0}/{1} tick finished, {2} entries, copy elapsed: {3}", name(), curr_tv, td.keys.size(), sw.stamp());

			raw_data_.clear();

			ticks_.tick(curr_tv);
		}

		virtual report_estimates_t get_estimates() override
		{
			report_estimates_t result = {};

			if (tick_t *tick = ticks_.last())
				result.row_count = tick->data.keys.size();
			else
				result.row_count = raw_data_.size();


			result.mem_used += sizeof(raw_data_);
			result.mem_used += raw_data_.bucket_count() * sizeof(*raw_data_.begin());

			for (auto const& raw_pair : raw_data_)
			{
				auto const& hv_map = raw_pair.second.hv.map_cref();
				result.mem_used += hv_map.bucket_count() * sizeof(*hv_map.begin());
			}

			for (auto const& tick : ticks_.get_internal_buffer())
			{
				if (!tick)
					continue;

				tick_data_t *td = &tick->data;

				result.mem_used += td->keys.size() * sizeof(td->keys[0]);
				result.mem_used += td->datas.size() * sizeof(td->datas[0]);
				result.mem_used += td->hvs.size() * sizeof(td->hvs[0]);

				for (auto const& hvs : td->hvs)
				{
					result.mem_used += hvs.values.size() * sizeof(*hvs.values.begin());
				}
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
			// bloom check
			if (packet->timer_bloom)
			{
				if (!packet->timer_bloom->contains(this->timer_bloom_))
				{
					stats_->packets_dropped_by_bloom++;
					return;
				}
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
				uint32_t       n_tags_found = 0;
				std::fill(out_range.begin(), out_range.end(), 0); // FIXME: this is not needed anymore ?

				for (uint32_t i = 0; i < n_tags_required; ++i)
				{
					for (uint32_t tag_i = 0; tag_i < t->tag_count; ++tag_i)
					{
						if (t->tag_name_ids[tag_i] != ki.timer_tag_r[i].d.timer_tag)
							continue;

						n_tags_found++;
						out_range[i] = t->tag_value_ids[tag_i];

						if (n_tags_found == n_tags_required)
							return true;
					}
				}

				return (n_tags_found == n_tags_required);
			};

			auto const find_request_tags = [&](key_info_t const& ki, key_t *out_key) -> bool
			{
				key_subrange_t out_range = ki_.rtag_key_subrange(*out_key);

				uint32_t const n_tags_required = out_range.size();
				uint32_t       n_tags_found = 0;
				std::fill(out_range.begin(), out_range.end(), 0); // FIXME: this is not needed anymore ?

				for (uint32_t tag_i = 0; tag_i < n_tags_required; ++tag_i)
				{
					for (uint16_t i = 0, i_end = packet->tag_count; i < i_end; ++i)
					{
						if (packet->tag_name_ids[i] != ki.request_tag_r[tag_i].d.request_tag)
							continue;

						n_tags_found++;
						out_range[tag_i] = packet->tag_value_ids[i];

						if (n_tags_found == n_tags_required)
							return true;
					}
				}

				return (n_tags_found == n_tags_required);
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
			key_inprogress.fill(0); // zerofill the key for now

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
			uint32_t timers_skipped_by_filters = 0;
			uint32_t timers_skipped_by_tags    = 0;
			{
				packet_unqiue_++; // next unique, since this is the new packet add

				key_subrange_t timer_key_range = ki_.timertag_key_subrange(key_inprogress);

				for (uint16_t i = 0; i < packet->timer_count; ++i)
				{
					packed_timer_t const *timer = &packet->timers[i];

					timers_scanned++;

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

					key_t const k = ki_.remap_key(key_inprogress);

					// LOG_DEBUG(globals_->logger(), "remapped key '{0}'", key_to_string(k));

					// finally - find and update item
					item_t& item = raw_data_[k];
					item.data_increment(timer);

					item.packet_increment(packet, packet_unqiue_);

					if (info_.hv_enabled)
					{
						item.hv_increment(timer, conf_.hv_bucket_count, conf_.hv_bucket_d);
					}
				}
			}

			stats_->timers_scanned            += timers_scanned;
			stats_->timers_aggregated         += timers_aggregated;
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

	};

////////////////////////////////////////////////////////////////////////////////////////////////
}} // namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

report_ptr create_report_by_timer(pinba_globals_t *globals, report_conf___by_timer_t const& conf)
{
	constexpr size_t max_keys = 8;
	size_t const n_keys = conf.keys.size();

	switch (n_keys)
	{
		case 0:
			throw std::logic_error(ff::fmt_str("report_by_timer doesn't support 0 keys aggregation"));
		default:
			throw std::logic_error(ff::fmt_str("report_by_timer supports up to {0} keys, {1} given", max_keys, n_keys));

	#define CASE(N) \
		case N: return std::make_shared<aux::report___by_timer_t<N>>(globals, conf);

		CASE(1);
		CASE(2);
		CASE(3);
		CASE(4);
		CASE(5);
		CASE(6);
		CASE(7);
		CASE(8);

	#undef CASE
	}
}

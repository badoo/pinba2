// #include <wchar.h> // wmemcmp

#include <functional>
#include <utility>

#include <meow/stopwatch.hpp>
#include <meow/utility/offsetof.hpp> // MEOW_SELF_FROM_MEMBER

#include <sparsehash/dense_hash_map>

#include "pinba/globals.h"
#include "pinba/histogram.h"
// #include "pinba/multi_merge.h"
#include "pinba/packet.h"
#include "pinba/report.h"
#include "pinba/report_util.h"
#include "pinba/report_by_timer.h"

////////////////////////////////////////////////////////////////////////////////////////////////
namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

template<size_t N>
using key_base_t = std::array<uint32_t, N>;

struct key__hasher_t
{
	// TODO(antoxa):
	//    std::hash seems to be ~20% faster on uint32_t/uint64_t keys that t1ha
	//    need better benchmarks here
	//    (but also see key__equal_t below)

	// inline size_t operator()(key_base_t<1> const& key) const
	// {
	// 	static std::hash<uint32_t> hasher;
	// 	return hasher(*reinterpret_cast<uint32_t const*>(key.data()));
	// }

	// inline size_t operator()(key_base_t<2> const& key) const
	// {
	// 	static std::hash<uint64_t> hasher;
	// 	return hasher(*reinterpret_cast<uint64_t const*>(key.data()));
	// }

	template<size_t N>
	inline size_t operator()(key_base_t<N> const& key) const
	{
		return t1ha0(key.data(), key.size() * sizeof(key[0]), 0);
	}
};

struct key__equal_t
{
	// XXX(antoxa):  leaving it here, but do NOT uncomment code below as it causes ~10x slowdown on hash lookups/merges
	// TODO(antoxa): need another experiment, when key_base_t<1> IS uint32_t and key_base_t<2> IS uint64_t

	// inline bool operator()(key_base_t<1> const& l, key_base_t<1> const& r) const
	// {
	// 	auto const lv = *reinterpret_cast<uint32_t const*>(l.data());
	// 	auto const rv = *reinterpret_cast<uint32_t const*>(r.data());
	// 	return lv == rv;
	// }

	// inline bool operator()(key_base_t<2> const& l, key_base_t<2> const& r) const
	// {
	// 	auto const lv = *reinterpret_cast<uint64_t const*>(l.data());
	// 	auto const rv = *reinterpret_cast<uint64_t const*>(r.data());
	// 	return lv == rv;
	// }

	template<size_t N>
	inline bool operator()(key_base_t<N> const& l, key_base_t<N> const& r) const
	{
		return l == r;
	}
};

template<size_t N>
inline key_base_t<N> key__make_empty()
{
	key_base_t<N> result;
	result.fill(0);
	return result;
}

template<size_t N>
std::string key_to_string(key_base_t<N> const& k)
{
	std::string result;

	for (size_t i = 0; i < k.size(); ++i)
	{
		ff::write(result, (i == 0) ? "" : "|", k[i]);
	}

	return result;
}

////////////////////////////////////////////////////////////////////////////////////////////////

struct report_row___by_timer_t
{
	report_row_data___by_timer_t  data;
	histogram_t                   hv;
};

////////////////////////////////////////////////////////////////////////////////////////////////

template<size_t NKeys>
struct report___by_timer_t : public report_t
{
	typedef key_base_t<NKeys>             key_t;
	typedef report_row_data___by_timer_t  data_t;

	struct item_t
		: private boost::noncopyable
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
		: public google::dense_hash_map<key_t, item_t, key__hasher_t, key__equal_t>
	{
		raw_hashtable_t()
		{
			this->set_empty_key(key__make_empty<NKeys>());
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
			histogram_t  hv;
		};

		struct hashtable_t : public google::dense_hash_map<key_t, row_t, key__hasher_t, key__equal_t>
		{
			hashtable_t()
			{
				this->set_empty_key(key__make_empty<NKeys>());
			}
		};

		static report_key_t key_at_position(hashtable_t const&, typename hashtable_t::iterator const& it)
		{
			// FIXME: just patch meow::chunk to initialize this nicely
			report_key_t k;
			for (auto const& kpart : it->first)
				k.push_back(kpart);
			return k;
		}

		static void* value_at_position(hashtable_t const&, typename hashtable_t::iterator const& it)
		{
			return &it->second.data;
		}

		static void* hv_at_position(hashtable_t const&, typename hashtable_t::iterator const& it)
		{
			return &it->second.hv;
		}

		// merge from src ringbuffer to snapshot data
		static void merge_ticks_into_data(pinba_globals_t *globals, report_info_t& rinfo, src_ticks_t const& ticks, hashtable_t& to)
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

			uint64_t n_ticks = 0;
			uint64_t key_lookups = 0;
			uint64_t hv_lookups = 0;

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

					if (rinfo.hv_enabled)
					{
						// dst.hv.merge_other(tick->data.hvs[i]);

						flat_histogram_t const& src_hv = tick->data.hvs[i];

						dst.hv.increment_inf(src_hv.inf_value);

						for (auto const& hv_value : src_hv.values)
						{
							dst.hv.increment_bucket(hv_value.bucket_id, hv_value.value);
						}
						hv_lookups += src_hv.values.size();
					}
				}

				key_lookups += tick->data.keys.size();
			}

			LOG_DEBUG(globals->logger(), "prepare '{0}'; n_ticks: {1}, key_lookups: {2}, hv_lookups: {3}",
				rinfo.name, n_ticks, key_lookups, hv_lookups);
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

			for (auto const& d : split_key_d)
			{
				result[d.remap_to] = flat_key[d.remap_from];
			}

			return result;
		}
	};

private:
	pinba_globals_t           *globals_;
	report_conf___by_timer_t  conf_;

	report_info_t  info_;
	key_info_t     ki_;

	ticks_t        ticks_;

	uint64_t       packet_unqiue_;

public:

	report___by_timer_t(pinba_globals_t *globals, report_conf___by_timer_t const& conf)
		: globals_(globals)
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
			.hv_kind         = HISTOGRAM_KIND__HASHTABLE, // HISTOGRAM_KIND__FLAT,
			.hv_bucket_count = conf_.hv_bucket_count,
			.hv_bucket_d     = conf_.hv_bucket_d,
		};

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

	virtual int kind() const override
	{
		return info_.kind;
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

				flat_histogram_t dst_hv = {
					.values      = {},
					.total_value = src_hv.items_total(),
					.inf_value   = src_hv.inf_value(),
				};

				dst_hv.values.reserve(src_hv.map_cref().size());

				for (auto const& hv_pair : src_hv.map_cref())
				{
					dst_hv.values.push_back(histogram_value_t { .bucket_id = hv_pair.first, .value = hv_pair.second });
				}

				// sort here, as we can't sort on merge, since data must be immutable
				std::sort(dst_hv.values.begin(), dst_hv.values.end());

				td.hvs.push_back(std::move(dst_hv));
			}
		}

		LOG_DEBUG(globals_->logger(), "{0}/{1} tick finished, {2} entries, copy elapsed: {3}", name(), curr_tv, td.keys.size(), sw.stamp());

		raw_data_.clear();

		ticks_.tick(curr_tv);
	}

	virtual report_snapshot_ptr get_snapshot() override
	{
		return meow::make_unique<snapshot_t>(globals_, ticks_.get_internal_buffer(), info_);
	}

public:

	virtual void add(packet_t *packet)
	{
		// run all filters and check if packet is 'interesting to us'
		for (size_t i = 0, i_end = conf_.filters.size(); i < i_end; ++i)
		{
			auto const& filter = conf_.filters[i];
			if (!filter.func(packet))
				return;
		}

		// finds timer with required tags
		auto const fetch_by_timer_tags = [&](key_info_t const& ki, key_subrange_t out_range, packed_timer_t const *t) -> bool
		{
			uint32_t const n_tags_required = out_range.size();
			uint32_t       n_tags_found = 0;
			std::fill(out_range.begin(), out_range.end(), 0);

			for (uint32_t i = 0; i < n_tags_required; ++i)
			{
				for (uint32_t tag_i = 0; tag_i < t->tag_count; ++tag_i)
				{
					if (t->tags[tag_i].name_id != ki.timer_tag_r[i].d.timer_tag)
						continue;

					n_tags_found++;
					out_range[i] = t->tags[tag_i].value_id;

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
			std::fill(out_range.begin(), out_range.end(), 0);

			for (uint32_t tag_i = 0; tag_i < n_tags_required; ++tag_i)
			{
				for (uint16_t i = 0, i_end = packet->tag_count; i < i_end; ++i)
				{
					if (packet->tags[i].name_id != ki.request_tag_r[tag_i].d.request_tag)
						continue;

					n_tags_found++;
					out_range[tag_i] = packet->tags[i].value_id;

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
			return;

		bool const fields_found = find_request_fields(ki_, &key_inprogress);
		if (!fields_found)
			return;

		// need to scan all timers, find matching and increment for each one
		{
			packet_unqiue_++; // next unique, since this is the new packet add

			key_subrange_t timer_key_range = ki_.timertag_key_subrange(key_inprogress);

			for (uint16_t i = 0; i < packet->timer_count; ++i)
			{
				packed_timer_t const *timer = &packet->timers[i];

				bool const timer_found = fetch_by_timer_tags(ki_, timer_key_range, timer);
				if (!timer_found)
					continue;

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
	}

	virtual void add_multi(packet_t **packets, uint32_t packet_count) override
	{
		// TODO: maybe optimize this we can
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
		case N: return meow::make_intrusive<aux::report___by_timer_t<N>>(globals, conf);

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

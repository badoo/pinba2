#include <memory>
#include <algorithm>
#include <vector>
#include <unordered_map>
#include <type_traits>

#include <boost/noncopyable.hpp>

#include <sparsehash/dense_hash_map>

#include <meow/stopwatch.hpp>
#include <meow/hash/hash.hpp>
#include <meow/hash/hash_impl.hpp>
#include <meow/format/format_to_string.hpp>

#include "pinba/globals.h"
#include "pinba/dictionary.h"
#include "pinba/histogram.h"
#include "pinba/packet.h"
#include "pinba/report.h"
#include "pinba/report_util.h"
#include "pinba/report_by_packet.h"
#include "pinba/report_by_request.h"
#include "pinba/multi_merge.h"

////////////////////////////////////////////////////////////////////////////////////////////////

#if 0
namespace { namespace detail {
	template<class I>
	struct merge_heap_item_t
	{
		I value_iter;  // current value iterator
		I end_iter;    // end iterator
	};

	template<class Container>
	inline void maybe_reserve(Container& cont, size_t sz) { /* nothing */ }

	template<class T>
	inline void maybe_reserve(std::vector<T>& cont, size_t sz) { cont.reserve(sz); }
}} // namespace { namespace detail {

// merge a range (defined by 'begin' and 'end') of pointers to *sorted* sequences into 'result'
// 'result' shall also be sorted and should support emplace(iterator, value)
// 'compare_fn' should return <0 for less, ==0 for equal, >0 for greater
// 'merge_fn' is used to merge equal elements
template<class ResultContainer, class Iterator, class Compare, class Merge>
static inline void multi_merge(ResultContainer& result, Iterator begin, Iterator end, Compare const& compare_fn, Merge const& merge_fn)
{
	using SequencePtr = typename std::iterator_traits<Iterator>::value_type;
	static_assert(std::is_pointer<SequencePtr>::value, "expected a range of pointers to sequences");

	using SequenceT    = typename std::remove_pointer<SequencePtr>::type;
	using merge_item_t = detail::merge_heap_item_t<typename SequenceT::const_iterator>;

	auto const item_greater = [&compare_fn](merge_item_t const& l, merge_item_t const& r)
	{
		return compare_fn(*l.value_iter, *r.value_iter) > 0;
	};

	size_t       result_length = 0; // initialized later
	size_t const input_size = std::distance(begin, end);

	merge_item_t tmp[input_size];
	merge_item_t *tmp_end = tmp + input_size;

	{
		size_t offset = 0;
		for (auto i = begin; i != end; i = std::next(i))
		{
			auto const *sequence = *i;

			auto const curr_b = std::begin(*sequence);
			auto const curr_e = std::end(*sequence);

			// FIXME: this is only useful if input sequence's iterator is random access
			// but fixing depends on reserve() optimization
			size_t const seq_size = std::distance(curr_b, curr_e);
			if (seq_size == 0)
				continue;

			result_length += seq_size;
			tmp[offset++] = { curr_b, curr_e };
		}

		tmp_end = tmp + offset;
	}

	detail::maybe_reserve(result, result_length);

	std::make_heap(tmp, tmp_end, item_greater);

	while (tmp != tmp_end)
	{
		// ff::fmt(stdout, "heap: {{ ");
		// for (merge_item_t *i = tmp; i != tmp_end; i++)
		// {
		// 	ff::fmt(stdout, "{0}:{1} ", i->value_iter->bucket_id, i->value_iter->value);
		// }
		// ff::fmt(stdout, "}\n");

		std::pop_heap(tmp, tmp_end, item_greater);
		merge_item_t *last = tmp_end - 1;

		// make room for next item, if it's not there
		if (compare_fn(result.back(), *last->value_iter) != 0)
		{
			result.push_back({}); // emplace not implemented for std::string :(
		}

		// merge current into existing (possibly empty item)
		merge_fn(result.back(), *last->value_iter);

		// advance to next item if exists
		auto const next_it = std::next(last->value_iter);
		if (next_it != last->end_iter)
		{
			last->value_iter = next_it;
			std::push_heap(tmp, tmp_end, item_greater);
		}
		else
		{
			--tmp_end;
		}
	}
}
#endif
////////////////////////////////////////////////////////////////////////////////////////////////

struct report_row_data___by_timer_t
{
	uint32_t    req_count;   // number of requests timer with such tag was present in
	uint32_t    hit_count;   // timer hit X times
	duration_t  time_total;  // sum of all timer values (i.e. total time spent in this timer)
	// timeval_t   time_total;  // sum of all timer values (i.e. total time spent in this timer)
	duration_t  ru_utime;    // same for rusage user
	duration_t  ru_stime;    // same for rusage system

	report_row_data___by_timer_t()
	{
		// FIXME: add a failsafe for memset
		memset(this, 0, sizeof(*this));
	}
};

struct report_row___by_timer_t
{
	report_row_data___by_timer_t  data;
	histogram_t                   hv;
};

////////////////////////////////////////////////////////////////////////////////////////////////

// RKD = Report Key Descriptor
#define RKD_REQUEST_TAG   0
#define RKD_REQUEST_FIELD 1
#define RKD_TIMER_TAG     2

struct report_key_timer_descriptor_t
{
	std::string name;
	int         kind;  // see defines above
	union {
		uint32_t             timer_tag;
		uint32_t             request_tag;
		uint32_t packet_t::* request_field;
	};
};

struct report___by_timer_conf_t
{
	std::string name;

	duration_t  time_window;      // total time window this report covers (report host uses this for ticking)
	uint32_t    ts_count;         // number of timeslices to store

	uint32_t    hv_bucket_count;  // number of histogram buckets, each bucket is hv_bucket_d 'wide'
	duration_t  hv_bucket_d;      // width of each hv_bucket

	using filter_func_t = std::function<bool(packet_t*)>;
	struct filter_descriptor_t
	{
		std::string   name;
		filter_func_t func;
	};

	std::vector<filter_descriptor_t> filters;

	// some builtins
	static inline filter_descriptor_t make_filter___by_min_time(duration_t min_time)
	{
		return filter_descriptor_t {
			.name = ff::fmt_str("by_min_time/>={0}", min_time),
			.func = [=](packet_t *packet)
			{
				return (packet->request_time >= min_time);
			},
		};
	}

	static inline filter_descriptor_t make_filter___by_max_time(duration_t max_time)
	{
		return filter_descriptor_t {
			.name = ff::fmt_str("by_max_time/<{0}", max_time),
			.func = [=](packet_t *packet)
			{
				return (packet->request_time < max_time);
			},
		};
	}

	// this describes how to form the report key
	// must have at least one element with RKD_TIMER_TAG
	std::vector<report_key_timer_descriptor_t> key_d;
};

struct report___by_timer_t : public report_t
{
	typedef report___by_timer_t           self_t;
	typedef report_key_t                  key_t;
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
		//        !!! AND swap() FOR SOME REASON !!!
		//        should not be called often with huge histograms, so expect it to be ok :(
		//        sparsehash with c++11 support (https://github.com/sparsehash/sparsehash-c11) fixes this
		//        but gcc 4.9.4 doesn't support the type_traits it requires
		//        so live this is for now, but probably - move to gcc6 or something
		item_t(item_t const& other)
			: last_unique(0)
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
			last_unique = other.last_unique;
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

public: // ticks

	struct raw_hashtable_t
		: public google::dense_hash_map<key_t, item_t, report_key__hasher_t, report_key__equal_t>
	{
		raw_hashtable_t()
		{
			this->set_empty_key(key_t{});
		}
	};

	raw_hashtable_t raw_data_;

	struct tick_data_t
	{
		struct row_t
		{
			data_t            data;
			flat_histogram_t  hv;
		}; // __attribute__((packed));

		struct hashtable_t
			: public google::dense_hash_map<key_t, row_t, report_key__hasher_t, report_key__equal_t>
		{
			hashtable_t()
			{
				this->set_empty_key(key_t{});
			}
		};

		hashtable_t ht;
	};

	using ticks_t       = ticks_ringbuffer_t<tick_data_t>;
	using tick_t        = ticks_t::tick_t;
	using ticks_list_t  = ticks_t::ringbuffer_t;

public: // snapshot

	struct snapshot_t : public report_snapshot_t
	{
		using parent_t    = report___by_timer_t;

		using src_ticks_t = parent_t::ticks_list_t;
		using hashtable_t = parent_t::tick_data_t::hashtable_t;
		using iterator_t  = typename hashtable_t::iterator;

	public:

		snapshot_t(src_ticks_t const& ticks, report_info_t const& rinfo, dictionary_t *d)
			: data_()
			, ticks_(ticks)
			, rinfo_(rinfo)
			, dictionary_(d)
		{
		}

	private:

		virtual report_info_t const* report_info() const override
		{
			return &rinfo_;
		}

		virtual dictionary_t const* dictionary() const override
		{
			return dictionary_;
		}

		virtual void prepare(prepare_type_t ptype) override
		{
			if (this->is_prepared())
				return;

			for (auto const& from : ticks_)
			{
				if (!from)
					return;

				for (auto const& from_pair : from->data.ht)
				{
					auto const& src = from_pair.second;
					auto      & dst = data_[from_pair.first];

					dst.data.req_count  += src.data.req_count;
					dst.data.hit_count  += src.data.hit_count;
					dst.data.time_total += src.data.time_total;
					dst.data.ru_utime   += src.data.ru_utime;
					dst.data.ru_stime   += src.data.ru_stime;
				}
			}

			if (rinfo_.hv_enabled)
			{
				struct merger_t
				{
					std::vector<histogram_value_t> *to;

					inline int compare(histogram_value_t const& l, histogram_value_t const& r) const
					{
						return (l < r) ? -1 : (r < l) ? 1 : 0;
					}

					inline void reserve(size_t const sz)
					{
						to->reserve(sz);
					}

					inline void push_back(std::vector<histogram_value_t> *seq, histogram_value_t const& v)
					{
						// TODO: try removing this if or put 'unlikely'
						if (to->empty())
						{
							to->push_back(v);
							return;
						}

						auto& back = to->back();

						if (0 == compare(back, v)) // same elt, must merge
							back.value += v.value;
						else                       // new one
							to->push_back(v);
					}
				};

				std::vector<std::vector<histogram_value_t>*> hv_range;

				for (auto& pair : data_)
				{
					auto const& key = pair.first;

					// find hvalues in all ticks for this key
					// and put pointers to them into hv_range
					for (auto const& tick : ticks_)
					{
						if (!tick)
							continue;

						auto const it = tick->data.ht.find(key);
						if (it == tick->data.ht.end())
							continue;

						hv_range.push_back(&it->second.hv.values);
					}

					// merge them all at once into empty destination
					merger_t merger = { .to = &pair.second.hv.values };
					pinba::multi_merge(&merger, hv_range.begin(), hv_range.end());

					// clear hv for next iteration
					hv_range.clear();
				}
			}
		}

		virtual bool is_prepared() const override
		{
			return ticks_.empty();
		}

		virtual size_t row_count() const override
		{
			return data_.size();
		}

	private:

		union real_position_t
		{
			static_assert(sizeof(iterator_t) <= sizeof(position_t), "position_t must be able to hold iterator contents");

			real_position_t(iterator_t i) : it(i) {}
			real_position_t(position_t p) : pos(p) {}

			position_t pos;
			iterator_t it;
		};

		position_t position_from_iterator(iterator_t const& it)
		{
			real_position_t p { it };
			return p.pos;
		}

	private:

		virtual position_t pos_first() override
		{
			return position_from_iterator(data_.begin());
		}

		virtual position_t pos_last() override
		{
			return position_from_iterator(data_.end());
		}

		virtual position_t pos_next(position_t const& pos) override
		{
			auto const& it = reinterpret_cast<iterator_t const&>(pos);
			return position_from_iterator(std::next(it));
		}

		virtual position_t pos_prev(position_t const& pos) override
		{
			auto const& it = reinterpret_cast<iterator_t const&>(pos);
			return position_from_iterator(std::prev(it));
		}

		virtual bool pos_equal(position_t const& l, position_t const& r) const override
		{
			auto const& l_it = reinterpret_cast<iterator_t const&>(l);
			auto const& r_it = reinterpret_cast<iterator_t const&>(r);
			return (l_it == r_it);
		}

		virtual report_key_t get_key(position_t const& pos) const override
		{
			auto const& it = reinterpret_cast<iterator_t const&>(pos);
			return it->first;
			// return Traits::key_at_position(data_, it);
		}

		virtual report_key_str_t get_key_str(position_t const& pos) const override
		{
			report_key_t k = this->get_key(pos);

			report_key_str_t result;
			for (uint32_t i = 0; i < k.size(); ++i)
			{
				result.push_back(dictionary_->get_word(k[i]));
			}
			return result;
		}

		virtual int data_kind() const override
		{
			return rinfo_.kind;
		}

		virtual void* get_data(position_t const& pos) override
		{
			auto const& it = reinterpret_cast<iterator_t const&>(pos);
			return &it->second.data;
			// return Traits::value_at_position(data_, it);
		}

		virtual int histogram_kind() const override
		{
			return rinfo_.hv_kind;
		}

		virtual void* get_histogram(position_t const& pos) override
		{
			if (!rinfo_.hv_enabled)
				return nullptr;

			auto const& it = reinterpret_cast<iterator_t const&>(pos);
			return &it->second.hv;
			// return Traits::hv_at_position(data_, it);
		}

	private:
		hashtable_t    data_;         // real data we iterate over
		src_ticks_t    ticks_;        // ticks we merge our data from (in other thread potentially)
		report_info_t  rinfo_;        // report info, immutable copy taken in ctor
		dictionary_t   *dictionary_;  // dictionary used to transform string ids to their values
	};

public: // key extraction and transformation

	typedef meow::string_ref<report_key_t::value_type> key_subrange_t;

	struct key_info_t
	{
		template<class T>
		using chunk_t = meow::chunk<T, key_t::static_size, uint32_t>;

		struct descriptor_t
		{
			report_key_timer_descriptor_t d;
			uint32_t                remap_from;  // offset in split_key_d
			uint32_t                remap_to;    // offset in conf.key_d
		};

		typedef chunk_t<descriptor_t>          rkd_chunk_t;
		typedef meow::string_ref<descriptor_t> rkd_range_t;

		// key descriptors grouped by kind
		rkd_chunk_t split_key_d;

		// these are ranges, describing which keys are where in split_key_d
		rkd_range_t request_tag_r;
		rkd_range_t request_field_r;
		rkd_range_t timer_tag_r;

		void from_config(report___by_timer_conf_t const& conf)
		{
			request_tag_r   = split_descriptors_by_kind(conf, RKD_REQUEST_TAG);
			request_field_r = split_descriptors_by_kind(conf, RKD_REQUEST_FIELD);
			timer_tag_r     = split_descriptors_by_kind(conf, RKD_TIMER_TAG);
		}

		// copy all descriptors with given kind to split_key_d
		// and return range pointing to where they are now
		// also updates remap_key_d with data to reverse the mapping
		rkd_range_t split_descriptors_by_kind(report___by_timer_conf_t const& conf, int kind)
		{
			auto const& key_d = conf.key_d;
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

					// ff::fmt(stdout, "d: {{ {0}, {1}, {2}, {3} }\n", d.d.kind, d.d.timer_tag, d.remap_from, d.remap_to);

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
			key_t result;

			for (uint32_t i = 0; i < flat_key.size(); i++)
				result.push_back();

			for (auto const& d : split_key_d)
			{
				result[d.remap_to] = flat_key[d.remap_from];
			}

			return result;
		}
	};

private:
	pinba_globals_t           *globals_;
	report___by_timer_conf_t  *conf_;

	report_info_t  info_;
	key_info_t     ki_;

	ticks_t        ticks_;

	uint64_t       packet_unqiue_;

public:

	report___by_timer_t(pinba_globals_t *globals, report___by_timer_conf_t *conf)
		: globals_(globals)
		, conf_(conf)
		, ticks_(conf_->ts_count)
		, packet_unqiue_{1}
	{
		// validate config
		if (conf_->key_d.size() > key_t::static_size)
		{
			throw std::runtime_error(ff::fmt_str(
				"required keys ({0}) > supported keys ({1})", conf_->key_d.size(), key_t::static_size));
		}

		info_ = report_info_t {
			.name        = conf_->name,
			.kind        = REPORT_KIND__BY_TIMER_DATA,
			.time_window = conf_->time_window,
			.tick_count  = conf_->ts_count,
			.n_key_parts = (uint32_t)conf_->key_d.size(),
			.hv_enabled  = (conf_->hv_bucket_count > 0),
		};

		ki_.from_config(*conf);
	}

	virtual str_ref name() const override
	{
		return conf_->name;
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

		for (auto const& raw_pair : raw_data_)
		{
			auto const& src = raw_pair.second;
			auto&       dst = tick->data.ht[raw_pair.first];

			dst.data = src.data;

			if (info_.hv_enabled)
			{
				dst.hv.inf_value = src.hv.inf_value();
				dst.hv.total_value = src.hv.items_total();

				for (auto const& hv_pair : src.hv.map_cref())
				{
					histogram_value_t v;
					v.bucket_id = hv_pair.first;
					v.value = hv_pair.second;
					// ff::fmt(stdout, "{0} <- {{ {1}, {2} }\n", report_key_to_string(raw_pair.first), v.bucket_id, v.value);
					dst.hv.values.push_back(v);
				}

				std::sort(dst.hv.values.begin(), dst.hv.values.end());
			}
		}

		raw_data_.clear();

		ticks_.tick(curr_tv);
	}

	virtual report_estimates_t get_estimates() override
	{
		return {};
	}

	virtual report_snapshot_ptr get_snapshot() override
	{
		return meow::make_unique<snapshot_t>(ticks_.get_internal_buffer(), info_, globals_->dictionary());
	}

public:

	virtual void add(packet_t *packet)
	{
		// run all filters and check if packet is 'interesting to us'
		for (size_t i = 0, i_end = conf_->filters.size(); i < i_end; ++i)
		{
			auto const& filter = conf_->filters[i];
			if (!filter.func(packet))
			{
				ff::fmt(stdout, "packet {0} skipped by filter {1}\n", packet, filter.name);
				return;
			}
		}

		// put key data into out_range if timer has all the parts
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
		for (uint32_t i = 0; i < info_.n_key_parts; ++i) // zerofill the key for now
			key_inprogress.push_back();

		bool const tags_found = find_request_tags(ki_, &key_inprogress);
		if (!tags_found)
		{
			ff::fmt(stdout, "packet rejected, required request tags not found\n");
			return;
		}

		bool const fields_found = find_request_fields(ki_, &key_inprogress);
		if (!fields_found)
		{
			ff::fmt(stdout, "packet rejected, required request fields not found\n");
			return;
		}

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

				// ff::fmt(stdout, "found key: {0}\n", report_key_to_string(key_inprogress, globals_->dictionary()));

				key_t const k = ki_.remap_key(key_inprogress);

				// ff::fmt(stdout, "real key: {0}\n", report_key_to_string(k));
				// ff::fmt(stdout, "real key: {0}\n", report_key_to_string(k, globals_->dictionary()));

				// finally - find and update item
				item_t& item = raw_data_[k];
				item.data_increment(timer);

				item.packet_increment(packet, packet_unqiue_);

				if (info_.hv_enabled)
				{
					item.hv_increment(timer, conf_->hv_bucket_count, conf_->hv_bucket_d);
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

	void serialize(FILE *f, str_ref name = {})
	{
		uint32_t   hit_count_total = 0;
		duration_t hit_time_total  = {0};
		duration_t time_window     = {0};

		auto const& tlist = ticks_.get_internal_buffer();

		ff::fmt(f, ">> {0} ----------------------->\n", name);
		for (unsigned i = 0; i < tlist.size(); i++)
		{
			ff::fmt(f, "tick[{0}]\n", i);

			auto const& tick = tlist[i];

			if (!tick)
				continue;

			time_window += duration_from_timeval(tick->end_tv - tick->start_tv);

			for (auto const& pair : tick->data.ht)
			{
				auto const& key  = pair.first;
				auto const& item = pair.second;
				auto const& data = item.data;

				hit_count_total += data.req_count;
				hit_time_total  += data.time_total;

				ff::fmt(f, "  [{0}] ->  {{ {1}, {2}, {3}, {4}, {5} } [",
					// report_key_to_string(key, globals_->dictionary()),
					report_key_to_string(key),
					data.req_count, data.hit_count, data.time_total, data.ru_utime, data.ru_stime);

				auto const& hvalues = item.hv.values;
				for (auto it = hvalues.begin(), it_end = hvalues.end(); it != it_end; ++it)
				{
					ff::fmt(f, "{0}{1}: {2}", (hvalues.begin() == it)?"":", ", it->bucket_id, it->value);
				}
				ff::fmt(f, "]\n");
			}
		}

		duration_t const avg_hit_time = (hit_count_total) ? hit_time_total / hit_count_total : duration_t{0};
		double const avg_hits_per_sec = ((double)hit_count_total / time_window.nsec) * nsec_in_sec; // gives 'weird' results for time_window < 1second

		ff::fmt(f, "<< avg_hit_time: {0}, tw: {1}, avg_hits_per_sec (expected): {2} -------<\n",
			avg_hit_time , time_window, avg_hits_per_sec);
		ff::fmt(f, "\n");
	}
};

////////////////////////////////////////////////////////////////////////////////////////////////

template<class T>
inline double operator/(T const& value, duration_t d)
{
	return ((double)value / d.nsec) * nsec_in_sec;
}

template<class SinkT>
SinkT& serialize_report_snapshot(SinkT& sink, report_snapshot_t *snapshot, str_ref name = {})
{
	{
		meow::stopwatch_t sw;

		ff::fmt(sink, ">> {0} ----------------------->\n", name);
		snapshot->prepare();
		ff::fmt(sink, ">> merge took {0} --------->\n", sw.stamp());
	}

	auto const write_hv = [&](report_snapshot_t::position_t const& pos)
	{
		ff::fmt(sink, " [");
		auto const *histogram = snapshot->get_histogram(pos);
		if (histogram != nullptr)
		{
			if (HISTOGRAM_KIND__HASHTABLE == snapshot->histogram_kind())
			{
				auto const *hv = static_cast<histogram_t const*>(histogram);

				auto const& hv_map = hv->map_cref();
				for (auto it = hv_map.begin(), it_end = hv_map.end(); it != it_end; ++it)
				{
					ff::fmt(sink, "{0}{1}: {2}", (hv_map.begin() == it)?"":", ", it->first, it->second);
				}
			}
			else if (HISTOGRAM_KIND__FLAT == snapshot->histogram_kind())
			{
				auto const *hv = static_cast<flat_histogram_t const*>(histogram);

				auto const& hvalues = hv->values;
				for (auto it = hvalues.begin(), it_end = hvalues.end(); it != it_end; ++it)
				{
					ff::fmt(sink, "{0}{1}: {2}", (hvalues.begin() == it)?"":", ", it->bucket_id, it->value);
				}
			}
		}

		ff::fmt(sink, "]");
	};

	for (auto pos = snapshot->pos_first(), end = snapshot->pos_last(); !snapshot->pos_equal(pos, end); pos = snapshot->pos_next(pos))
	{
		auto const key = snapshot->get_key(pos);
		// ff::fmt(sink, "[{0}] -> ", report_key_to_string(key, snapshot->dictionary()));
		ff::fmt(sink, "[{0}] -> ", report_key_to_string(key));

		auto const data_kind = snapshot->data_kind();

		switch (data_kind)
		{
		case REPORT_KIND__BY_PACKET_DATA:
		{
			auto const *rinfo = snapshot->report_info();
			auto const *data  = reinterpret_cast<report_row_data___by_packet_t*>(snapshot->get_data(pos));

			ff::fmt(sink, "{{ {0}, {1}, {2}, {3}, {4}, {5}, {6} }",
				data->req_count, data->timer_count, data->time_total, data->ru_utime, data->ru_stime,
				data->traffic_kb, data->mem_usage);

			auto const time_window = rinfo->time_window; // TODO: calculate real time window from snapshot data
			ff::fmt(sink, " {{ rps: {0} }",
				ff::as_printf("%.06lf", data->req_count / time_window));

			write_hv(pos);
		}
		break;

		case REPORT_KIND__BY_REQUEST_DATA:
		{
			auto const *rinfo = snapshot->report_info();
			auto const *data  = reinterpret_cast<report_row_data___by_request_t*>(snapshot->get_data(pos));

			ff::fmt(sink, "{{ {0}, {1}, {2}, {3}, {4}, {5} }",
				data->req_count, data->time_total, data->ru_utime, data->ru_stime,
				data->traffic_kb, data->mem_usage);

			auto const time_window = rinfo->time_window; // TODO: calculate real time window from snapshot data
			ff::fmt(sink, " {{ rps: {0} }",
				ff::as_printf("%.06lf", data->req_count / time_window));

			write_hv(pos);
		}
		break;

		case REPORT_KIND__BY_TIMER_DATA:
		{
			auto const *rinfo = snapshot->report_info();
			auto const *data  = reinterpret_cast<report_row_data___by_timer_t*>(snapshot->get_data(pos));

			ff::fmt(sink, "{{ {0}, {1}, {2}, {3}, {4} }",
				data->req_count, data->hit_count, data->time_total, data->ru_utime, data->ru_stime);

			auto const time_window = rinfo->time_window; // TODO: calculate real time window from snapshot data
			ff::fmt(sink, " {{ rps: {0}, tps: {1} }",
				ff::as_printf("%.06lf", data->req_count / time_window),
				ff::as_printf("%.06lf", data->hit_count / time_window));

			write_hv(pos);
		}
		break;

		default:
			// assert(!"unknown report snapshot data_kind()");
			ff::fmt(sink, "unknown report snapshot data_kind(): {0}", data_kind);
			break;
		}

		ff::fmt(sink, "\n");
	}

	ff::fmt(sink, "<<-----------------------<\n");
	ff::fmt(sink, "\n");

	return sink;
}

////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char const *argv[])
try
{
	pinba_options_t options = {};
	pinba_globals_ptr globals = pinba_globals_init(&options);

	auto packet_data = packet_t {
		.host_id = 1,
		.server_id = 0,
		.script_id = 7,
		.schema_id = 0,
		.status = 0,
		.doc_size = 9999,
		.memory_peak = 1,
		.tag_count = 0,
		.timer_count = 0,
		.request_time = duration_t{ 15 * msec_in_sec },
		.ru_utime = duration_t{ 3 * msec_in_sec },
		.ru_stime = duration_t{ 1 * msec_in_sec },
		.dictionary = NULL,
		.tag_name_ids = NULL,
		.tag_value_ids = NULL,
		.timers = NULL,
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

		debug_dump_packet(stdout, packet);
	}

#if 0
	report_conf_t report_conf = {};
	report_conf.time_window     = 60 * d_second,
	report_conf.ts_count        = 5;
	report_conf.hv_bucket_d     = 1 * d_microsecond;
	report_conf.hv_bucket_count = 1 * 1000 * 1000;

	// report_conf.min_time = 100 * d_millisecond;
	// report_conf.max_time = 300 * d_millisecond;

	report_conf.selectors = {
		// {
		// 	"request_time/>=100ms/<300ms",
		// 	[](packet_t *packet)
		// 	{
		// 		static constexpr duration_t const min_time = 100 * d_millisecond;
		// 		static constexpr duration_t const max_time = 300 * d_millisecond;

		// 		return (packet->request_time >= min_time && packet->request_time < max_time);
		// 	},
		// },
	};

	report_conf.key_fetchers = {
		/*[0] = */{
			.name    = "script_name",
			.fetcher = [](packet_t *packet) -> report_key_fetch_res_t
			{
				return { packet->script_id, true };
			},
			//
			// alternative impl (should be faster, no indirect calls)
			//
			// .request_tag   = 15, // (if > 0)
			// .request_field = &packet_t::script_id, // (if != NULL) fetch this field from packet (as uint32_t ofc)
			// .timer_tag     = 45, // (if > 0) find timer, that has tag with this id, and use it's value as key_part (skip packet if not found)
		},
		/*[1] = */{
			.name    = "hostname",
			.fetcher = [](packet_t *packet) -> report_key_fetch_res_t
			{
				return { packet->host_id, true };
			},
			//
			// alternative impl (should be faster, no indirect calls)
			//
			// .request_tag   = 15, // (if > 0)
			// .request_field = &packet_t::script_id, // (if != NULL) fetch this field from packet (as uint32_t ofc)
			// .timer_tag     = 45, // (if > 0) find timer, that has tag with this id, and use it's value as key_part (skip packet if not found)
		},
		// /*[2] = */{
		// 	.name    = "request_tag/test_tag",
		// 	.fetcher = [](packet_t *packet) -> report_key_fetch_res_t
		// 	{
		// 		static uint32_t const tag_name_id = 0; //dictionary->get_or_add("test_tag");
		// 		for (uint32_t i = 0; i < packet->tag_count; ++i)
		// 		{
		// 			if (packet->tags[i].name_id == tag_name_id)
		// 			{
		// 				return { packet->tags[i].value_id, true };
		// 			}
		// 		}
		// 		return { 0, false };
		// 	},
		// },
	};

	// auto report = meow::make_unique<report___by_request_t>(&report_conf);
#endif

	report___by_timer_conf_t rconf_timer = [&]()
	{
		report___by_timer_conf_t conf = {};
		conf.time_window     = 5 * d_second,
		conf.ts_count        = 5;
		conf.hv_bucket_d     = 1 * d_microsecond;
		conf.hv_bucket_count = 1 * 1000 * 1000;

		// conf.min_time = 100 * d_millisecond;
		// conf.max_time = 300 * d_millisecond;

		conf.filters = {
			// {
			// 	"request_time/>=100ms/<300ms",
			// 	[](packet_t *packet)
			// 	{
			// 		static constexpr duration_t const min_time = 100 * d_millisecond;
			// 		static constexpr duration_t const max_time = 300 * d_millisecond;

			// 		return (packet->request_time >= min_time && packet->request_time < max_time);
			// 	},
			// },
		};

		auto const make_timertag_kd = [&](str_ref tag_name)
		{
			report_key_timer_descriptor_t r;
			r.name = ff::fmt_str("timertag/{0}", tag_name);
			r.kind = RKD_TIMER_TAG;
			r.timer_tag = globals->dictionary()->get_or_add(tag_name);
			return r;
		};

		auto const make_rtag_kd = [&](str_ref tag_name)
		{
			report_key_timer_descriptor_t r;
			r.name = ff::fmt_str("rtag/{0}", tag_name);
			r.kind = RKD_REQUEST_TAG;
			r.request_tag = globals->dictionary()->get_or_add(tag_name);
			return r;
		};

		auto const make_rfield_kd = [&](str_ref tag_name, uint32_t packet_t:: *field_ptr)
		{
			report_key_timer_descriptor_t r;
			r.name = ff::fmt_str("rfield/{0}", tag_name);
			r.kind = RKD_REQUEST_FIELD;
			r.request_field = field_ptr;
			return r;
		};

		conf.key_d.push_back(make_rfield_kd("script_name", &packet_t::script_id));
		conf.key_d.push_back(make_timertag_kd("group"));
		// conf.key_d.push_back(make_rtag_kd("type"));
		// conf.key_d.push_back(make_rfield_kd("script_name", &packet_t::script_id));
		// conf.key_d.push_back(make_rfield_kd("server_name", &packet_t::server_id));
		// conf.key_d.push_back(make_rfield_kd("host_name", &packet_t::host_id));
		conf.key_d.push_back(make_timertag_kd("server"));

		return conf;
	}();

	auto report = meow::make_unique<report___by_timer_t>(globals.get(), &rconf_timer);
	report->ticks_init(os_unix::clock_monotonic_now());

	report->add(packet);
	report->tick_now(os_unix::clock_monotonic_now());
	report->serialize(stdout, "first");

	report->add(packet);
	report->add(packet);
	report->add(packet);
	report->tick_now(os_unix::clock_monotonic_now());
	report->serialize(stdout, "second");

	{
		auto const snapshot = report->get_snapshot();

		report->serialize(stdout, "second_nochange"); // snapshot should not change

		serialize_report_snapshot(stdout, snapshot.get(), "snapshot_1");
	}

	report->add(packet);
	report->add(packet);
	report->tick_now(os_unix::clock_monotonic_now());
	report->serialize(stdout, "third");
	report->tick_now(os_unix::clock_monotonic_now());
	report->tick_now(os_unix::clock_monotonic_now());
	report->tick_now(os_unix::clock_monotonic_now());
	report->serialize(stdout, "+3");

	{
		auto snapshot = report->get_snapshot();
		serialize_report_snapshot(stdout, snapshot.get(), "snapshot_2");
	}

	return 0;
}
catch (std::exception const& e)
{
	ff::fmt(stderr, "error: {0}\n", e.what());
	return 1;
}

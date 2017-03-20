#include <wchar.h> // wmemcmp

#include <functional>
#include <utility>

#include <meow/stopwatch.hpp>
#include <meow/utility/offsetof.hpp> // MEOW_SELF_FROM_MEMBER

#include <sparsehash/dense_hash_map>

#include "pinba/globals.h"
#include "pinba/histogram.h"
#include "pinba/multi_merge.h"
#include "pinba/packet.h"
#include "pinba/report.h"
#include "pinba/report_util.h"
#include "pinba/report_by_timer.h"

////////////////////////////////////////////////////////////////////////////////////////////////
namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

struct report___by_timer_t : public report_t
{
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
		{
			*this = std::move(other); // operator=()
		}

		void operator=(item_t&& other)
		{
			data = other.data;           // a copy
			hv   = std::move(other.hv);  // real move
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
		: public google::dense_hash_map<key_t, item_t, report_key__hasher_t, report_key__equal_t>
	{
		raw_hashtable_t()
		{
			this->set_empty_key(key_t{});
		}
	};

	raw_hashtable_t raw_data_;

private: // tick data

	struct tick_data_t
	{
		std::vector<key_t>        keys;
		std::vector<data_t>       datas;
		std::vector<histogram_t>  hvs;
#if 0
		struct iterator : public std::vector<key_t>::iterator // std::random_access_iterator_tag
		{
			tick_data_t *td_;

			struct value_type
			{
				key_t       *key;
				data_t      *data;
				histogram_t *hv;

				void swap(value_type& other)
				{
					using std::swap;
					swap(*key, *other.key);
					swap(*data, *other.data);

				}
			};

			explicit iterator(tick_data_t *td, size_t off = 0)
				: td_(td)
				, off_(off)
			{
			}

			value_type operator*()
			{
				return value_type { &td_->keys[off_], &td_->datas[off_], (off_ < td_->keys.size()) ? &td_->hvs[off_] : nullptr };
			}

			iterator& operator=(iterator const& other)
			{
				td_  = other.td_;
				off_ = other.off_;
				return *this;
			}

			bool operator==(iterator const& other) const { return (other.off_ == off_) && (other.td_ == td_); }
			bool operator!=(iterator const& other) const { return (other.off_ != off_) || (other.td_ != td_); }
			bool operator<(iterator const& other) const { return off_ < other.off_; }

			// iterator operator+(iterator const& other) const { return iterator(td_, off_ + other.off_); }
			difference_type operator-(iterator const& other) const { return (ssize_t)off_ - (ssize_t)other.off_; }

			iterator& operator+=(size_t n) { off_ += n; return *this; }
			iterator& operator-=(size_t n) { off_ -= n; return *this; }
		};

		iterator begin() { return iterator(this, 0); }
		iterator end() { return iterator(this, keys.size()); }
#endif
	};

	using ticks_t       = ticks_ringbuffer_t<tick_data_t>;
	using tick_t        = ticks_t::tick_t;
	using ticks_list_t  = ticks_t::ringbuffer_t;

public: // snapshot

	struct snapshot_traits
	{
		using src_ticks_t = ticks_list_t;

		struct row_t
		{
			key_t        key;
			data_t       data;
			histogram_t  hv;
		};

		struct hashtable_t : public std::vector<row_t>
		{
		};

		static report_key_t key_at_position(hashtable_t const&, hashtable_t::iterator const& it)
		{
			return it->key;
		}

		static void* value_at_position(hashtable_t const&, hashtable_t::iterator const& it)
		{
			return &it->data;
		}

		static void* hv_at_position(hashtable_t const&, hashtable_t::iterator const& it)
		{
			return &it->hv;
		}

		// merge from src ringbuffer to snapshot data
		static void merge_ticks_into_data(pinba_globals_t *globals, report_info_t& rinfo, src_ticks_t const& ticks, hashtable_t& to)
		{
			struct merger_t
			{
				hashtable_t     *to;
				report_info_t   *rinfo;
				pinba_globals_t *globals;

				uint64_t n_compare_calls;

				inline int compare(key_t const& l, key_t const& r)
				{
					static_assert(sizeof(key_t::value_type) == sizeof(wchar_t), "wmemchr operates on whcar_t, and we pass key data there");

					assert(l.size() == r.size());

					++n_compare_calls;
					return wmemcmp((wchar_t*)l.data(), (wchar_t*)r.data(), l.size());
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

						return (0 != compare(to->back().key, v));
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
		}
	};

	using snapshot_t = report_snapshot__impl_t<snapshot_traits>;

public: // key extraction and transformation

	typedef meow::string_ref<report_key_t::value_type> key_subrange_t;

	struct key_info_t
	{
		template<class T>
		using chunk_t = meow::chunk<T, key_t::static_size, uint32_t>;

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
		// validate config
		if (conf_.keys.size() > key_t::static_size)
		{
			throw std::runtime_error(ff::fmt_str(
				"required keys ({0}) > supported keys ({1})", conf_.keys.size(), key_t::static_size));
		}

		info_ = report_info_t {
			.name            = conf_.name,
			.kind            = REPORT_KIND__BY_TIMER_DATA,
			.time_window     = conf_.time_window,
			.tick_count      = conf_.tick_count,
			.n_key_parts     = (uint32_t)conf_.keys.size(),
			.hv_enabled      = (conf_.hv_bucket_count > 0),
			.hv_kind         = HISTOGRAM_KIND__HASHTABLE,
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

		// this builds an array of pointers to raw_data_ hashtable values (aka std::pair<key_t, item_t>*)
		// then sorts pointers, according to keys, inside the hash
		// and then we're going to copy full values to destination already sorted
		struct sort_elt_t
		{
			key_t key;
			raw_hashtable_t::value_type const *ptr;
		};

		// fill
		std::vector<sort_elt_t> raw_data_pointers;
		raw_data_pointers.reserve(raw_data_.size());

		for (auto const& raw_pair : raw_data_)
			raw_data_pointers.push_back({raw_pair.first, &raw_pair});

		LOG_DEBUG(globals_->logger(), "{0}/{1} tick sort data prepared, elapsed: {2}", name(), curr_tv, sw.stamp());

		// sort
		sw.reset();

		std::sort(raw_data_pointers.begin(), raw_data_pointers.end(),
			[](sort_elt_t const& l, sort_elt_t const& r) {
				return wmemcmp((wchar_t*)l.key.data(), (wchar_t*)r.key.data(), l.key.size()) < 0; });

		LOG_DEBUG(globals_->logger(), "{0}/{1} tick data sorted, elapsed: {2}", name(), curr_tv, sw.stamp());

		// can copy now
		sw.reset();

		// reserve memory in advance, since we know the final size
		{
			td.keys.reserve(raw_data_.size());
			td.datas.reserve(raw_data_.size());

			if (info_.hv_enabled)
				td.hvs.reserve(raw_data_.size());
		}

		LOG_DEBUG(globals_->logger(), "{0}/{1} tick data storage allocated, elapsed: {2}", name(), curr_tv, sw.stamp());

		// copy, according to pointers
		sw.reset();

		for (auto const& sort_elt : raw_data_pointers)
		{
			// td.keys.push_back(sort_elt.ptr->first);
			td.keys.push_back(sort_elt.key);
			td.datas.push_back(sort_elt.ptr->second.data);

			if (info_.hv_enabled)
			{
				td.hvs.push_back(std::move(sort_elt.ptr->second.hv));
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
		for (uint32_t i = 0; i < info_.n_key_parts; ++i) // zerofill the key for now
			key_inprogress.push_back();

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

				key_t const k = ki_.remap_key(key_inprogress);

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
	return meow::make_unique<aux::report___by_timer_t>(globals, conf);
}

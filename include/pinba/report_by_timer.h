#ifndef PINBA__REPORT_BY_TIMER_H_
#define PINBA__REPORT_BY_TIMER_H_

#include <functional>
#include <utility>

#include <boost/noncopyable.hpp>

#include <sparsehash/dense_hash_map>

#include "pinba/globals.h"
#include "pinba/dictionary.h"
#include "pinba/histogram.h"
#include "pinba/packet.h"
#include "pinba/report.h"
#include "pinba/report_util.h"

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

struct report_conf___by_timer_t
{
	std::string name;

	duration_t  time_window;      // total time window this report covers (report host uses this for ticking)
	uint32_t    ts_count;         // number of timeslices to store

	uint32_t    hv_bucket_count;  // number of histogram buckets, each bucket is hv_bucket_d 'wide'
	duration_t  hv_bucket_d;      // width of each hv_bucket

public: // packet filters

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

public: // key fetchers

	struct key_descriptor_t
	{
		std::string name;
		int         kind;  // see defines above
		union {
			uint32_t             timer_tag;
			uint32_t             request_tag;
			uint32_t packet_t::* request_field;
		};
	};

	// this describes how to form the report key
	// must have at least one element with kind == RKD_TIMER_TAG
	std::vector<key_descriptor_t> keys;


	// some builtins
	static inline key_descriptor_t key_descriptor_by_request_tag(str_ref tag_name, uint32_t tag_name_id)
	{
		key_descriptor_t d;
		d.name        = ff::fmt_str("request_tag/{0}", tag_name);
		d.kind        = RKD_REQUEST_TAG;
		d.request_tag = tag_name_id;
		return d;
	}

	static inline key_descriptor_t key_descriptor_by_request_field(str_ref field_name, uint32_t packet_t::* field_ptr)
	{
		key_descriptor_t d;
		d.name          = ff::fmt_str("request_field/{0}", field_name);
		d.kind          = RKD_REQUEST_FIELD;
		d.request_field = field_ptr;
		return d;
	}

	static inline key_descriptor_t key_descriptor_by_timer_tag(str_ref tag_name, uint32_t tag_name_id)
	{
		key_descriptor_t d;
		d.name      = ff::fmt_str("timer_tag/{0}", tag_name);
		d.kind      = RKD_TIMER_TAG;
		d.timer_tag = tag_name_id;
		return d;
	}
};

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

public: // ticks

	using raw_hashtable_t = google::dense_hash_map<key_t, item_t, report_key__hasher_t, report_key__equal_t>;

	struct hashtable_t : public raw_hashtable_t
	{
		hashtable_t()
		{
			this->set_empty_key(key_t{});
		}
	};

	using ticks_t       = ticks_ringbuffer_t<hashtable_t>;
	using tick_t        = ticks_t::tick_t;
	using ticks_list_t  = ticks_t::ringbuffer_t;

public: // snapshot

	struct snapshot_traits
	{
		using src_ticks_t = ticks_list_t;

		struct hashtable_t : public google::dense_hash_map<
											  key_t
											, report_row___by_timer_t
											, report_key__hasher_t
											, report_key__equal_t>
		{
			hashtable_t() { this->set_empty_key(key_t{}); }
		};

		static report_key_t key_at_position(hashtable_t const&, hashtable_t::iterator const& it)
		{
			return it->first;
		}

		static void* value_at_position(hashtable_t const&, hashtable_t::iterator const& it)
		{
			return (void*)&it->second;
		}

		static histogram_t* hv_at_position(hashtable_t const&, hashtable_t::iterator const& it)
		{
			return &it->second.hv;
		}

		// merge full tick from src ringbuffer to current hashtable_t state
		static void merge_from_to(report_info_t& rinfo, tick_t const *from, hashtable_t& to)
		{
			if (!from)
				return;

			for (auto const& from_pair : from->data)
			{
				auto const& src = from_pair.second;
				auto      & dst = to[from_pair.first];

				dst.data.req_count  += src.data.req_count;
				dst.data.hit_count  += src.data.hit_count;
				dst.data.time_total += src.data.time_total;
				dst.data.ru_utime   += src.data.ru_utime;
				dst.data.ru_stime   += src.data.ru_stime;

				if (rinfo.hv_enabled)
				{
					dst.hv.merge_other(src.hv);
				}
			}
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
	report_conf___by_timer_t  conf_;

	report_info_t  info_;
	key_info_t     ki_;

	ticks_t        ticks_;

	uint64_t       packet_unqiue_;

public:

	report___by_timer_t(pinba_globals_t *globals, report_conf___by_timer_t const& conf)
		: globals_(globals)
		, conf_(conf)
		, ticks_(conf.ts_count)
		, packet_unqiue_{1} // init this to 1, so it's different from 0 in default constructed data_t
	{
		// validate config
		if (conf_.keys.size() > key_t::static_size)
		{
			throw std::runtime_error(ff::fmt_str(
				"required keys ({0}) > supported keys ({1})", conf_.keys.size(), key_t::static_size));
		}

		info_ = report_info_t {
			.kind            = REPORT_KIND__BY_TIMER_DATA,
			.time_window     = conf_.time_window,
			.timeslice_count = conf_.ts_count,
			.n_key_parts     = (uint32_t)conf_.keys.size(),
			.hv_enabled      = (conf_.hv_bucket_count > 0),
			.hv_bucket_count = conf_.hv_bucket_count,
			.hv_bucket_d     = conf_.hv_bucket_d,
		};

		ki_.from_config(conf);
	}

	virtual str_ref name() const override
	{
		return conf_.name;
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
		ticks_.tick(curr_tv);
	}

	virtual report_snapshot_ptr get_snapshot() override
	{
		return meow::make_unique<snapshot_t>(ticks_.get_internal_buffer(), info_, globals_->dictionary());
	}

public:

	virtual void add(packet_t *packet)
	{
		// run all filters and check if packet is 'interesting to us'
		for (size_t i = 0, i_end = conf_.filters.size(); i < i_end; ++i)
		{
			auto const& filter = conf_.filters[i];
			if (!filter.func(packet))
			{
				ff::fmt(stdout, "packet {0} skipped by filter {1}\n", packet, filter.name);
				return;
			}
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

				// ff::fmt(stdout, "real key: {0}\n", report_key_to_string(k, globals_->dictionary()));

				// finally - find and update item
				item_t& item = ticks_.current().data[k];
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

#endif // PINBA__REPORT_BY_TIMER_H_

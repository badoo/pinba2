#include <memory>
#include <list>
#include <vector>
#include <unordered_map>
#include <type_traits>

#include <boost/noncopyable.hpp>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <sparsehash/dense_hash_map>
#include <sparsehash/sparse_hash_map>

#include <meow/stopwatch.hpp>
#include <meow/hash/hash.hpp>
#include <meow/hash/hash_impl.hpp>
#include <meow/format/format_to_string.hpp>

#include "pinba/globals.h"
#include "pinba/dictionary.h"
#include "pinba/packet.h"
#include "pinba/report.h"

////////////////////////////////////////////////////////////////////////////////////////////////

struct report_row_data___by_request_t
{
	uint32_t   req_count;
	duration_t req_time;
	duration_t ru_utime;
	duration_t ru_stime;
	uint64_t   traffic_kb;
	uint64_t   mem_usage;

	report_row_data___by_request_t()
	{
		// FIXME: add a failsafe for memset
		memset(this, 0, sizeof(*this));
	}
};


struct report_row___by_request_t
{
	report_row_data___by_request_t  data;
	histogram_t                     hv;
};

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

struct report_key__hasher_t
{
	template<size_t N>
	inline constexpr size_t operator()(report_key_base_t<N> const& key) const
	{
		// TODO: try a "better" hash function here, like https://github.com/leo-yuriev/t1ha
		return meow::hash_blob(key.data(), N);
	}
};

struct report_key__equal_t
{
	template<size_t N>
	inline bool operator()(report_key_base_t<N> const& l, report_key_base_t<N> const& r) const
	{
		return (l.size() == r.size())
				? (0 == memcmp(l.data(), r.data(), (l.size() * sizeof(typename report_key_base_t<N>::value_type)) ))
				: false
				;
	}
};

template<size_t N>
std::string report_key_to_string(report_key_base_t<N> const& k)
{
	std::string result;

	for (size_t i = 0; i < k.size(); ++i)
	{
		ff::write(result, (i == 0) ? "" : "|", k[i]);
	}

	return result;
}

template<size_t N>
std::string report_key_to_string(report_key_base_t<N> const& k, dictionary_t const *dict)
{
	std::string result;

	for (size_t i = 0; i < k.size(); ++i)
	{
		ff::fmt(result, "{0}{1}<{2}>", (i == 0) ? "" : "|", k[i], dict->get_word(k[i]));
	}

	return result;
}

////////////////////////////////////////////////////////////////////////////////////////////////

/*
struct report_snapshot_traits___example
{
	using src_ticks_t = ; // source ticks_ringbuffer_t
	using hashtable_t = ; // result hashtable (that we're going to iterate over)

	// merge full tick from src ringbuffer to current hashtable_t state
	// from can be NULL (when there is no timeslice for this interval)
	static void merge_from_to(report_info_t& rinfo, typename src_ticks_t::value_t const *from, hashtable_t& to);
};
*/

template<class Traits>
struct report_snapshot__impl_t : public report_snapshot_t
{
	using src_ticks_t = typename Traits::src_ticks_t;
	using hashtable_t = typename Traits::hashtable_t;
	using iterator_t  = typename hashtable_t::iterator;

public:

	report_snapshot__impl_t(src_ticks_t ticks, report_info_t const& rinfo, dictionary_t *d)
		: data_()
		, ticks_(std::move(ticks))  // get a copy, and move explicitly
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

	virtual void prepare() override
	{
		if (ticks_.empty())
			throw std::logic_error(ff::fmt_str("{0}: trying to merge report snapshot a second time", __PRETTY_FUNCTION__));

		// TODO: add rusage tracking to this function

		for (auto& tick : ticks_)
		{
			// perform merge
			Traits::merge_from_to(rinfo_, tick.get(), data_);

			// release ref counted pointer as soon as we're done merging a tick
			// this allows to reduce memory footprint when merging in other thread
			// since report is working and ticks are being erased from it
			// (but this merge is holding them alive still)
			tick.reset();
		}

		ticks_.clear();
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
		return (void*)&it->second; // FIXME: const
	}

	virtual histogram_t const* get_histogram(position_t const& pos) override
	{
		if (!rinfo_.hv_enabled)
			return NULL;

		auto const& it = reinterpret_cast<iterator_t const&>(pos);
		return &it->second.hv;
	}

private:
	hashtable_t    data_;         // real data we iterate over
	src_ticks_t    ticks_;        // ticks we merge our data from (in other thread potentially)
	report_info_t  rinfo_;        // report info, immutable copy taken in ctor
	dictionary_t   *dictionary_;  // dictionary used to transform string ids to their values
};

////////////////////////////////////////////////////////////////////////////////////////////////

template<class T>
struct ticks_ringbuffer_t : private boost::noncopyable
{
	using data_t = T;

	struct tick_t
		: boost::intrusive_ref_counter<tick_t>
	{
		timeval_t  start_tv;
		timeval_t  end_tv;
		data_t     data;

		tick_t(timeval_t curr_tv)
			: start_tv(curr_tv)
			, end_tv({0,0})
			, data()
		{
		}
	};

	using tick_ptr     = boost::intrusive_ptr<tick_t> ;
	using ringbuffer_t = std::vector<tick_ptr>;

public:

	ticks_ringbuffer_t(uint32_t tick_count)
		: tick_count_(tick_count)
	{
	}

	void init(timeval_t curr_tv)
	{
		for (uint32_t i = 0; i < tick_count_; i++)
			ticks_.push_back({});

		curr_tick_.reset(new tick_t(curr_tv));
	}

	void tick(timeval_t curr_tv)
	{
		curr_tick_->end_tv = curr_tv;           // finish current tick

		ticks_.push_back(curr_tick_);           // copy tick to history
		curr_tick_.reset(new tick_t(curr_tv));  // and create new tick at 'current'

		// maybe truncate history, if it has grown too long
		if (ticks_.size() >  tick_count_)
		{
			ticks_.erase(ticks_.begin()); // XXX: O(N)
		}
	}

	ringbuffer_t const& get_internal_buffer() const
	{
		return ticks_;
	}

	tick_t& current()
	{
		return *curr_tick_;
	}

private:
	uint32_t      tick_count_;
	ringbuffer_t  ticks_;
	tick_ptr      curr_tick_;
};


////////////////////////////////////////////////////////////////////////////////////////////////

struct report_key_fetch_res_t
{
	uint32_t key_value;
	bool     found;
};

typedef std::function<report_key_fetch_res_t(packet_t*)> report_key_fetcher_fn_t;

struct report_key_descriptor_t
{
	std::string              name;
	report_key_fetcher_fn_t  fetcher;
};

typedef std::function<bool(packet_t*)> report_packet_selector_fn_t;

struct report_selector_descriptor_t
{
	std::string                  name;
	report_packet_selector_fn_t  func;
};

inline report_selector_descriptor_t report_selector__by_req_time_min(duration_t min_time)
{
	return report_selector_descriptor_t {
		.name = ff::fmt_str("by_min_time/>={0}", min_time),
		.func = [=](packet_t *packet)
		{
			return (packet->request_time >= min_time);
		},
	};
}

inline report_selector_descriptor_t report_selector__by_req_time_max(duration_t max_time)
{
	return report_selector_descriptor_t {
		.name = ff::fmt_str("by_max_time/<{0}", max_time),
		.func = [=](packet_t *packet)
		{
			return (packet->request_time < max_time);
		},
	};
}

struct report_conf_t
{
	std::string name;

	duration_t  time_window;      // total time window this report covers (report host uses this for ticking)
	uint32_t    ts_count;         // number of timeslices to store

	uint32_t    hv_bucket_count;  // number of histogram buckets, each bucket is hv_bucket_d 'wide'
	duration_t  hv_bucket_d;      // width of each hv_bucket

	// functions to select if we're interested in packet at all
	std::vector<report_selector_descriptor_t> selectors;

	// functions to fetch key parts from incoming packets
	std::vector<report_key_descriptor_t>      key_fetchers;
};

////////////////////////////////////////////////////////////////////////////////////////////////

struct report___by_request_t : public report_t
{
	typedef report___by_request_t           self_t;
	typedef report_key_t                    key_t;
	typedef report_row_data___by_request_t  data_t;

	struct item_t
		: private boost::noncopyable
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
		{
			*this = std::move(other); // operator=()
		}

		void operator=(item_t&& other)
		{
			data = other.data;          // a copy
			hv.map.swap(other.hv.map);  // real move
		}

		void data_increment(packet_t *packet)
		{
			data.req_count  += 1;
			data.req_time   += packet->request_time;
			data.ru_utime   += packet->ru_utime;
			data.ru_stime   += packet->ru_stime;
			data.traffic_kb += packet->doc_size;
			data.mem_usage  += packet->memory_peak;
		}

		void hv_increment(packet_t *packet, uint32_t hv_bucket_count, duration_t hv_bucket_d)
		{
			uint32_t const id = packet->request_time.nsec / hv_bucket_d.nsec;
			uint32_t const bucket_id = (id < hv_bucket_count)
										? id + 1 // known tick bucket
										: 0;     // infinity

			hv.map[bucket_id] += 1;
		}

		void merge_other(item_t const& other)
		{
			// data
			data.req_count  += other.data.req_count;
			data.req_time   += other.data.req_time;
			data.ru_utime   += other.data.ru_utime;
			data.ru_stime   += other.data.ru_stime;
			data.traffic_kb += other.data.traffic_kb;
			data.mem_usage  += other.data.mem_usage;

			// hv
			for (auto const& hv_pair : other.hv.map)
			{
				hv.map[hv_pair.first] += hv_pair.second;
			}
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
											, report_row___by_request_t
											, report_key__hasher_t
											, report_key__equal_t>
		{
			hashtable_t() { this->set_empty_key(key_t{}); }
		};

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
				dst.data.req_time   += src.data.req_time;
				dst.data.ru_utime   += src.data.ru_utime;
				dst.data.ru_stime   += src.data.ru_stime;
				dst.data.traffic_kb += src.data.traffic_kb;
				dst.data.mem_usage  += src.data.mem_usage;

				if (rinfo.hv_enabled)
				{
					for (auto const& hv_pair : src.hv.map)
						dst.hv.map[hv_pair.first] += hv_pair.second;
				}
			}
		}
	};

	using snapshot_t = report_snapshot__impl_t<snapshot_traits>;

private:
	pinba_globals_t  *globals_;
	report_conf_t    *conf_;

	report_info_t    info_;

	ticks_t          ticks_;

public:

	report___by_request_t(pinba_globals_t *globals, report_conf_t *conf)
		: globals_(globals)
		, conf_(conf)
		, ticks_(conf_->ts_count)
	{
		// validate config
		if (conf_->key_fetchers.size() > key_t::static_size)
		{
			throw std::runtime_error(ff::fmt_str(
				"required keys ({0}) > supported keys ({1})", conf_->key_fetchers.size(), key_t::static_size));
		}

		info_ = report_info_t {
			.kind            = REPORT_KIND__BY_REQUEST_DATA,
			.timeslice_count = conf_->ts_count,
			.time_window     = conf_->time_window,
			.n_key_parts     = (uint32_t)conf_->key_fetchers.size(),
			.hv_enabled      = (conf_->hv_bucket_count > 0),
		};
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
		ticks_.tick(curr_tv);
	}

	virtual report_snapshot_ptr get_snapshot() override
	{
		return meow::make_unique<snapshot_t>(ticks_.get_internal_buffer(), info_, globals_->dictionary());
	}

public:

	virtual void add(packet_t *packet) override
	{
		// run all selectors and check if packet is 'interesting to us'
		for (size_t i = 0, i_end = conf_->selectors.size(); i < i_end; ++i)
		{
			auto const& selector = conf_->selectors[i];
			if (!selector.func(packet))
			{
				ff::fmt(stdout, "packet {0} skipped by selector {1}\n", packet, selector.name);
				return;
			}
		}

		// construct a key, by runinng all key fetchers
		key_t k;

		for (size_t i = 0, i_end = conf_->key_fetchers.size(); i < i_end; ++i)
		{
			auto const& fetcher = conf_->key_fetchers[i];

			report_key_fetch_res_t const r = fetcher.fetcher(packet);
			if (!r.found)
			{
				ff::fmt(stdout, "packet {0} skipped by key fetcher {1}\n", packet, fetcher.name);
				return;
			}

			k.push_back(r.key_value);
		}

		// finally - find and update item
		item_t& item = ticks_.current().data[k];
		item.data_increment(packet);

		if (info_.hv_enabled)
		{
			item.hv_increment(packet, conf_->hv_bucket_count, conf_->hv_bucket_d);
		}
	}

	virtual void add_multi(packet_t **packets, uint32_t packet_count) override
	{
		// TODO: maybe optimize this we can
		for (uint32_t i = 0; i < packet_count; ++i)
			this->add(packets[i]);
	}

public:

	static void do_serialize(FILE *f, ticks_t const& ticks, str_ref name)
	{
		uint32_t   req_count_total = 0;
		duration_t req_time_total  = {0};
		duration_t time_window     = {0};

		auto const& tlist = ticks.get_internal_buffer();

		ff::fmt(f, ">> {0} ----------------------->\n", name);
		for (unsigned i = 0; i < tlist.size(); i++)
		{
			ff::fmt(f, "ticks[{0}]\n", i);

			auto const& timeslice = tlist[i];

			if (!timeslice)
				continue;

			time_window += duration_from_timeval(timeslice->end_tv - timeslice->start_tv);

			for (auto const& pair : timeslice->data)
			{
				auto const& key  = pair.first;
				auto const& item = pair.second;
				auto const& data = item.data;

				req_count_total += data.req_count;
				req_time_total  += data.req_time;

				ff::fmt(f, "  [{0}] ->  {{ {1}, {2}, {3}, {4} } [", report_key_to_string(key), data.req_count, data.req_time, data.ru_utime, data.ru_stime);
				for (auto it = item.hv.map.begin(), it_end = item.hv.map.end(); it != it_end; ++it)
				{
					ff::fmt(f, "{0}{1}: {2}", (item.hv.map.begin() == it)?"":", ", it->first, it->second);
				}
				ff::fmt(f, "]\n");
			}
		}

		duration_t const avg_req_time = (req_count_total) ? req_time_total / req_count_total : duration_t{0};
		double const avg_rps = ((double)req_count_total / time_window.nsec) * nsec_in_sec; // gives 'weird' results for time_window < 1second

		ff::fmt(f, "<< avg_req_time: {0}, tw: {1}, avg_rps (expected): {2} -------<\n",
			avg_req_time , time_window, avg_rps);
		ff::fmt(f, "\n");
	}

	void serialize(FILE *f, str_ref name)
	{
		do_serialize(f, ticks_, name);
	}
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

	// functions to select if we're interested in packet at all
	std::vector<report_selector_descriptor_t> selectors;

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
			: data()
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
			data = other.data;          // a copy
			hv.map.swap(other.hv.map);  // real move
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
			uint32_t const id = (timer->value.nsec / timer->hit_count) / hv_bucket_d.nsec;
			uint32_t const bucket_id = (id < hv_bucket_count)
										? id + 1 // known tick bucket
										: 0;     // infinity

			hv.map[bucket_id] += 1;
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
			for (auto const& hv_pair : other.hv.map)
			{
				hv.map[hv_pair.first] += hv_pair.second;
			}
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
					for (auto const& hv_pair : src.hv.map)
						dst.hv.map[hv_pair.first] += hv_pair.second;
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
		, packet_unqiue_{0}
	{
		// validate config
		if (conf_->key_d.size() > key_t::static_size)
		{
			throw std::runtime_error(ff::fmt_str(
				"required keys ({0}) > supported keys ({1})", conf_->key_d.size(), key_t::static_size));
		}

		info_ = report_info_t {
			.kind            = REPORT_KIND__BY_TIMER_DATA,
			.timeslice_count = conf_->ts_count,
			.time_window     = conf_->time_window,
			.n_key_parts     = (uint32_t)conf_->key_d.size(),
			.hv_enabled      = (conf_->hv_bucket_count > 0),
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
		ticks_.tick(curr_tv);
	}

	virtual report_snapshot_ptr get_snapshot() override
	{
		return meow::make_unique<snapshot_t>(ticks_.get_internal_buffer(), info_, globals_->dictionary());
	}

public:

	virtual void add(packet_t *packet)
	{
		// run all selectors and check if packet is 'interesting to us'
		for (size_t i = 0, i_end = conf_->selectors.size(); i < i_end; ++i)
		{
			auto const& selector = conf_->selectors[i];
			if (!selector.func(packet))
			{
				ff::fmt(stdout, "packet {0} skipped by selector {1}\n", packet, selector.name);
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

			for (auto const& pair : tick->data)
			{
				auto const& key  = pair.first;
				auto const& item = pair.second;
				auto const& data = item.data;

				hit_count_total += data.req_count;
				hit_time_total  += data.time_total;

				ff::fmt(f, "  [{0}] ->  {{ {1}, {2}, {3}, {4}, {5} } [",
					report_key_to_string(key, globals_->dictionary()),
					data.req_count, data.hit_count, data.time_total, data.ru_utime, data.ru_stime);

				for (auto it = item.hv.map.begin(), it_end = item.hv.map.end(); it != it_end; ++it)
				{
					ff::fmt(f, "{0}{1}: {2}", (item.hv.map.begin() == it)?"":", ", it->first, it->second);
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

template<class SinkT>
SinkT& serialize_report_snapshot(SinkT& sink, report_snapshot_t *snapshot, str_ref name = {})
{
	{
		meow::stopwatch_t sw;

		ff::fmt(sink, ">> {0} ----------------------->\n", name);
		snapshot->prepare();
		ff::fmt(sink, ">> merge took {0} --------->\n", sw.stamp());
	}

	for (auto pos = snapshot->pos_first(), end = snapshot->pos_last(); !snapshot->pos_equal(pos, end); pos = snapshot->pos_next(pos))
	{
		auto const key = snapshot->get_key(pos);
		ff::fmt(sink, "[{0}] -> ", report_key_to_string(key, snapshot->dictionary()));

		auto const data_kind = snapshot->data_kind();

		switch (data_kind)
		{
		case REPORT_KIND__BY_REQUEST_DATA:
		{
			auto const *row = reinterpret_cast<report_row___by_request_t*>(snapshot->get_data(pos));
			auto const& data = row->data;

			ff::fmt(sink, "{{ {0}, {1}, {2}, {3}, {4}, {5} } [",
				data.req_count, data.req_time, data.ru_utime, data.ru_stime,
				data.traffic_kb, data.mem_usage);

			auto const *hv = snapshot->get_histogram(pos);
			if (NULL != hv)
			{
				for (auto it = hv->map.begin(), it_end = hv->map.end(); it != it_end; ++it)
				{
					ff::fmt(sink, "{0}{1}: {2}", (hv->map.begin() == it)?"":", ", it->first, it->second);
				}
			}

			ff::fmt(sink, "]");
		}
		break;

		case REPORT_KIND__BY_TIMER_DATA:
		{
			auto const *row = reinterpret_cast<report_row___by_timer_t*>(snapshot->get_data(pos));
			auto const& data = row->data;

			ff::fmt(sink, "{{ {0}, {1}, {2}, {3} } [",
				data.req_count, data.time_total, data.ru_utime, data.ru_stime);

			auto const *hv = snapshot->get_histogram(pos);
			if (NULL != hv)
			{
				for (auto it = hv->map.begin(), it_end = hv->map.end(); it != it_end; ++it)
				{
					ff::fmt(sink, "{0}{1}: {2}", (hv->map.begin() == it)?"":", ", it->first, it->second);
				}
			}

			ff::fmt(sink, "]");
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
}

////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char const *argv[])
try
{
	pinba_options_t options = {};
	pinba_globals_ptr globals = pinba_init(&options);

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
		.tags = NULL,
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

	report___by_timer_conf_t rconf_timer = [&]()
	{
		report___by_timer_conf_t conf = {};
		conf.time_window     = 60 * d_second,
		conf.ts_count        = 5;
		conf.hv_bucket_d     = 1 * d_microsecond;
		conf.hv_bucket_count = 1 * 1000 * 1000;

		// conf.min_time = 100 * d_millisecond;
		// conf.max_time = 300 * d_millisecond;

		conf.selectors = {
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

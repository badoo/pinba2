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

#include <meow/chunk.hpp>
#include <meow/stopwatch.hpp>
#include <meow/hash/hash.hpp>
#include <meow/hash/hash_impl.hpp>
#include <meow/unix/time.hpp>
#include <meow/std_unique_ptr.hpp>
#include <meow/format/format_to_string.hpp>

#include "pinba/globals.h"
#include "pinba/packet.h"

////////////////////////////////////////////////////////////////////////////////////////////////

struct histogram_t
{
	// map time_interval -> request_count
	//
	// sparse_hash_map is said to be more efficient memory-wise
	// and we kinda need that, since there is a histogram per row per timeslice
	//
	// TODO: a couple of ideas on tuning this
	//  1. maybe make hash map impl configureable, so that if report has few rows
	//     a faster hash map can be use (like dense_hash_map)
	//  2. experiment with hash function for integers here (maybe a simple identity will work?)
	//  3. control hash map grows as much as possible, to save memory (at least use small initial size)
	//  4. keep as little state in this object as possible to save memory
	//     all confguration information (like histogram max size and hash initial size can be passed from outside)
	//  5. maybe use memory pools to allocate objects like this one (x84_64)
	//     sizeof(unordered_map) == 56    // as of gcc 4.9.4
	//     sizeof(dense_hash_map) == 80
	//     sizeof(sparse_hash_map) == 88
	//  6. try https://github.com/greg7mdp/sparsepp (within 1% of sparse_hash_map by mem usage, but faster lookup/insert)
	//     sizeof(spp::sparse_hash_map) == 88
	//  7. sparse_hash_set should use less memory due to using uint32_t but expect 64bit alignments?
	//
	typedef google::sparse_hash_map<uint32_t, uint32_t> map_t;
	map_t map;
};

////////////////////////////////////////////////////////////////////////////////////////////////

struct report_global_info_t
{
	// uint32_t    row_count; // as max of all timeslice's row count

	uint32_t    timeslice_count;
	duration_t  time_window;
};


struct report_row_data___by_request_t
{
	uint32_t    req_count;
	double      req_per_sec;

	duration_t  req_time_total;
	duration_t  req_time_per_sec;
	duration_t  req_time_median;

	duration_t  ru_utime_total;
	duration_t  ru_utime_per_sec;

	duration_t  ru_stime_total;
	duration_t  ru_stime_per_sec;

	uint64_t    traffic_total;
	double      traffic_per_sec;

	uint64_t    memory_footprint_total;

	report_row_data___by_request_t()
	{
		// FIXME: add a failsafe for memset
		memset(this, 0, sizeof(*this));
	}
};


struct report_row___by_request_t
	: public report_row_data___by_request_t
{
	histogram_t hv;
};

struct report_row___by_timer_t
{
	uint32_t    req_count;   // number of requests timer with such tag was present in
	uint32_t    hit_count;   // timer hit X times
	duration_t  total_time;  // sum of all timer values (i.e. total time spent in this timer)
	duration_t  ru_utime;    // same for rusage user
	duration_t  ru_stime;    // same for rusage system
};


////////////////////////////////////////////////////////////////////////////////////////////////

// max number of key parts we support for reports
#define REPORT_MAX_KEY_PARTS 7

template<size_t N>
using report_key_base_t = meow::chunk<uint32_t, N, uint32_t>;

template<size_t N>
using report_key_str_base_t = meow::chunk<str_ref, N, uint32_t>;

using report_key_t     = report_key_base_t<REPORT_MAX_KEY_PARTS>;
using report_key_str_t = report_key_str_base_t<REPORT_MAX_KEY_PARTS>;

namespace detail {
	template<size_t N>
	struct report_key___padding_checker
	{
		static_assert(sizeof(report_key_base_t<N>) == ((N + 1) * sizeof(uint32_t)), "ensure no padding within report_key_base_t");
	};
	constexpr report_key___padding_checker<1> const report_key___padding_checker__1__;
	constexpr report_key___padding_checker<2> const report_key___padding_checker__2__;
	constexpr report_key___padding_checker<3> const report_key___padding_checker__3__;
	constexpr report_key___padding_checker<4> const report_key___padding_checker__4__;
	constexpr report_key___padding_checker<5> const report_key___padding_checker__5__;
	constexpr report_key___padding_checker<6> const report_key___padding_checker__6__;
	constexpr report_key___padding_checker<7> const report_key___padding_checker__7__;
} // namespace detail {

struct report_key__hasher_t
{
	template<size_t N>
	constexpr size_t operator()(report_key_base_t<N> const& key) const
	{
		return meow::hash_blob(key.data(), N);
	}
};

struct report_key__equal_t
{
	template<size_t N>
	constexpr bool operator()(report_key_base_t<N> const& l, report_key_base_t<N> const& r) const
	{
		return (l.size() != r.size())
				? false
				: (0 == memcmp(l.data(), r.data(), (l.size() * sizeof(typename report_key_base_t<N>::value_type)) ))
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

////////////////////////////////////////////////////////////////////////////////////////////////

#define REPORT_KIND__BY_REQUEST_DATA 0
#define REPORT_KIND__BY_TIMER_DATA   1

struct report_snapshot_t
{
	// this struct is just a placeholder, same size as real hashtable iterator
	// FIXME: what about alignment here?
	// TODO: maybe make iteration completely internal
	//       (i.e. never give position away, just "current data" and expose next/prev/reset/valid)
	struct position_t
	{
		uintptr_t dummy___[3];
	};

	virtual ~report_snapshot_t() {}

	// get global snapshot data
	// this function is here (and not JUST in report_t), to avoid race conditions,
	// i.e. globals() and get_snapshot() returning slightly different data,
	// due to those being 2 separate function calls (and some packets might get processed in the middle)
	// virtual report_global_data_t* globals() const = 0;

	// prepare snapshot for use
	// MUST be called before any of the functions below
	// this exists primarily to allow preparation to take place in a thread
	// different from the one handling report data (more parallelism, yey)
	virtual void prepare() = 0;

	// iteration, this should be very cheap
	virtual position_t pos_first() = 0;
	virtual position_t pos_last() = 0;
	virtual position_t pos_next(position_t const&) = 0;
	virtual position_t pos_prev(position_t const&) = 0;
	virtual bool       pos_equal(position_t const&, position_t const&) const = 0;

	// key handling
	virtual uint32_t         n_key_parts() const = 0;
	virtual report_key_t     get_key(position_t const&) const = 0;
	virtual report_key_str_t get_key_str(position_t const&) const = 0;

	// data handling
	virtual int   data_kind() const = 0;
	virtual void* get_data(position_t const&) = 0;

	// histograms
	virtual bool               histogram_enabled() const = 0;
	virtual histogram_t const* get_histogram(position_t const&) = 0;
};
typedef std::unique_ptr<report_snapshot_t> report_snapshot_ptr;

////////////////////////////////////////////////////////////////////////////////////////////////

struct report_t : private boost::noncopyable
{
	virtual ~report_t() {}

	virtual str_ref name() const = 0;

	// TODO: report kinds need some love, maybe just have separate classes for them
	//       or have this one too (since we'd like to store them all in one place)
	virtual int     kind() const = 0;

	virtual void ticks_init(timeval_t curr_tv) = 0;
	virtual void tick_now(timeval_t curr_tv) = 0;

	virtual void add(packet_t*) = 0;
	virtual void add_multi(packet_t**, uint32_t packet_count) = 0;

	virtual report_snapshot_ptr get_snapshot() = 0;
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
		.name = ff::fmt_str("by_min_time/<{0}", max_time),
		.func = [=](packet_t *packet)
		{
			return (packet->request_time < max_time);
		},
	};
}

struct report_conf_t
{
	std::string name;

	// duration_t  time_window;   // total time window this report covers (report host uses this for ticking)
	uint32_t    ts_count;         // number of timeslices to store

	uint32_t    hv_bucket_count;  // number of histogram buckets, each bucket is hv_bucket_d 'wide'
	duration_t  hv_bucket_d;      // width of each hv_bucket

	// functions to select if we're interested in packet at all
	std::vector<report_selector_descriptor_t> selectors;

	// functions to fetch key parts from incoming packets
	std::vector<report_key_descriptor_t>      key_fetchers;
};

////////////////////////////////////////////////////////////////////////////////////////////////

struct report___by_request_data_t : public report_t
{
	typedef report___by_request_data_t  self_t;
	typedef report_key_t                key_t;

	struct data_t
	{
		uint32_t   req_count;
		duration_t req_time;
		duration_t ru_utime;
		duration_t ru_stime;
		uint64_t   traffic_kb;
		uint64_t   mem_usage;

		data_t()
		{
			// static_assert(std::is_standard_layout<decltype(*this)>::value, "make sure data can be zeroed by memset");
			// FIXME: add some safeguards here?
			memset(this, 0, sizeof(*this));
		}
	};

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
		//        !!! AND SWAP() FOR SOME REASON !!!
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
										: id;    // infinity

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

public:

	// typedef std::unordered_map<
	typedef google::dense_hash_map<
						  key_t
						, item_t
						, report_key__hasher_t
						, report_key__equal_t
					> raw_t;

	struct timeslice_t
		: boost::intrusive_ref_counter<timeslice_t>
	{
		timeval_t start_tv = {0};
		timeval_t end_tv   = {0};
		raw_t     items;
	};
	typedef boost::intrusive_ptr<timeslice_t> timeslice_ptr;

	typedef std::vector<timeslice_ptr> ticks_t; // constant size

private:
	report_conf_t *conf_;

	ticks_t        ticks_;

	raw_t          raw_;
	timeval_t      raw_start_tv_; // timeval we've started gathering raw data

public: // should be private really

	struct snapshot_t : public report_snapshot_t
	{
		typedef raw_t                   parent_raw_t;
		typedef parent_raw_t::key_type  key_t;

		typedef std::unordered_map<
								  key_t
								, report_row___by_request_t
								, report_key__hasher_t
								, report_key__equal_t
							> hash_t;

		typedef hash_t::const_iterator iterator_t;

		hash_t    data_;          // real data we iterate over
		ticks_t   ticks_;         // ticks we merge our data from (in other thread potentially)
		uint32_t  n_key_parts_;   // number of key parts that exist here for real
		bool      hv_enabled_;    // histograms enabled ?

	public:

		snapshot_t(ticks_t ticks, uint32_t n_key_parts, bool hv_enabled)
			: data_{}
			, ticks_(std::move(ticks))  // get a copy, and move explicitly
			, n_key_parts_(n_key_parts)
			, hv_enabled_(hv_enabled)
		{
		}

		virtual void prepare() override
		{
			// TODO: release ref counted pointer as soon as we're done merging a tick
			//       this allows to reduce memory footprint when merging in other thread
			//       since report is working and ticks are being erased from it
			//       (but this merge is holding them alive still)

			for (size_t i = 0; i < ticks_.size(); ++i)
			{
				auto const& timeslice = ticks_[i];

				if (!timeslice)
					continue;

				for (auto const& item_pair : timeslice->items)
				{
					report_row___by_request_t& row = data_[item_pair.first];
					auto const& item_data = item_pair.second.data;

					row.req_count                   += item_data.req_count;
					// row.req_per_sec              += ;
					row.req_time_total              += item_data.req_time;
					// row.req_time_percent         += ;
					// row.req_time_per_sec         += ;
					// row.req_time_median          += ;
					row.ru_utime_total              += item_data.ru_utime;
					// row.ru_utime_percent         += ;
					// row.ru_utime_per_sec         += ;
					row.ru_stime_total              += item_data.ru_stime;
					// row.ru_stime_percent         += ;
					// row.ru_stime_per_sec         += ;
					row.traffic_total               += item_data.traffic_kb;
					// row.traffic_percent          += ;
					// row.traffic_per_sec          += ;
					row.memory_footprint_total      += item_data.mem_usage;
					// row.memory_footprint_percent += ;

					if (hv_enabled_)
					{
						for (auto const& hv_pair : item_pair.second.hv.map)
							row.hv.map[hv_pair.first] += hv_pair.second;
					}
				}
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

	public:

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

			// FIXME: implement dictionary here
			report_key_str_t result;
			for (uint32_t i = 0; i < k.size(); ++i)
			{
				result.push_back(meow::ref_lit("-"));
			}
			return result;
		}

		virtual uint32_t n_key_parts() const override
		{
			return n_key_parts_;
		}

		virtual int data_kind() const override
		{
			return REPORT_KIND__BY_REQUEST_DATA;
		}

		virtual void* get_data(position_t const& pos) override
		{
			auto const& it = reinterpret_cast<iterator_t const&>(pos);
			return (void*)&it->second; // FIXME: const
		}

		virtual bool histogram_enabled() const override
		{
			return hv_enabled_;
		}

		virtual histogram_t const* get_histogram(position_t const& pos) override
		{
			if (!this->histogram_enabled())
				return NULL;

			auto const& it = reinterpret_cast<iterator_t const&>(pos);
			return &it->second.hv;
		}
	};

private:

	bool const hv_enabled()
	{
		return conf_->hv_bucket_count > 0;
	}

public:

	report___by_request_data_t(report_conf_t *conf)
		: conf_(conf)
		, raw_start_tv_{0}
	{
		// validate config
		if (conf_->key_fetchers.size() > key_t::static_size)
			throw std::runtime_error(ff::fmt_str(
				"required keys ({0}) > supported keys ({1})",
				conf_->key_fetchers.size(), key_t::static_size));

		// dense_hash_map specific
		raw_.set_empty_key(key_t());
	}

	virtual str_ref name() const override
	{
		return conf_->name;
	}

	virtual int kind() const override
	{
		return REPORT_KIND__BY_REQUEST_DATA;
	}

	virtual void ticks_init(timeval_t curr_tv) override
	{
		for (uint32_t i = 0; i < conf_->ts_count; i++)
		{
			ticks_.push_back({});
		}

		raw_start_tv_ = curr_tv;
	}

	virtual void tick_now(timeval_t curr_tv) override
	{
		timeslice_ptr timeslice { new timeslice_t() };
		timeslice->start_tv = raw_start_tv_;
		timeslice->end_tv   = curr_tv;
		timeslice->items.swap(raw_); // effectively - grab current raw_, replacing it with empty hash

		raw_.set_empty_key(key_t());

		raw_start_tv_ = curr_tv;

		ticks_.push_back(timeslice);
		if (ticks_.size() >  conf_->ts_count)
		{
			ticks_.erase(ticks_.begin()); // FIXME: O(N)
		}
	}

	virtual report_snapshot_ptr get_snapshot() override
	{
		return meow::make_unique<snapshot_t>(ticks_, conf_->key_fetchers.size(), hv_enabled());
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
		item_t& item = raw_[k];
		item.data_increment(packet);

		if (hv_enabled())
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

		ff::fmt(f, ">> {0} ----------------------->\n", name);
		for (unsigned i = 0; i < ticks.size(); i++)
		{
			ff::fmt(f, "items[{0}]\n", i);

			auto const& timeslice = ticks[i];

			if (!timeslice)
				continue;

			time_window += duration_from_timeval(timeslice->end_tv - timeslice->start_tv);

			for (auto const& pair : timeslice->items)
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

	static void do_serialize_raw(FILE *f, raw_t const& raw, str_ref name)
	{
		ff::fmt(f, ">> {0} ----------------------->\n", name);
		for (auto const& pair : raw)
		{
			auto const& key  = pair.first;
			auto const& item = pair.second;
			auto const& data = item.data;

			ff::fmt(f, "  [{0}] ->  {{ {1}, {2}, {3}, {4} } [", report_key_to_string(key), data.req_count, data.req_time, data.ru_utime, data.ru_stime);
			for (auto it = item.hv.map.begin(), it_end = item.hv.map.end(); it != it_end; ++it)
			{
				ff::fmt(f, "{0}{1}: {2}", (item.hv.map.begin() == it)?"":", ", it->first, it->second);
			}
			ff::fmt(f, "]\n");
		}

		ff::fmt(f, "<<-----------------------<\n");
		ff::fmt(f, "\n");
	}

	void serialize(FILE *f, str_ref name)
	{
		do_serialize(f, ticks_, name);
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
		ff::fmt(sink, "[{0}] -> ", report_key_to_string(key));

		auto const data_kind = snapshot->data_kind();

		switch (data_kind)
		{
		case REPORT_KIND__BY_REQUEST_DATA:
		{
			// uint32_t    req_count;
			// double      req_per_sec;
			// duration_t  req_time_total;
			// // double      req_time_percent;
			// duration_t  req_time_per_sec;
			// duration_t  req_time_median;

			// duration_t  ru_utime_total;
			// // double      ru_utime_percent;
			// duration_t  ru_utime_per_sec;

			// duration_t  ru_stime_total;
			// // double      ru_stime_percent;
			// duration_t  ru_stime_per_sec;

			// uint64_t    traffic_total;
			// // double      traffic_percent;
			// uint64_t    traffic_per_sec;
			// uint64_t    memory_footprint_total;
			// // double      memory_footprint_percent;

			auto const *row = reinterpret_cast<report_row___by_request_t*>(snapshot->get_data(pos));
			ff::fmt(sink, "{{ {0}, {1}, {2}, {3}, {4}, {5} } [",
				row->req_count, row->req_time_total, row->ru_utime_total, row->ru_stime_total,
				row->traffic_total, row->memory_footprint_total);

			auto const *hv = snapshot->get_histogram(pos);
			if (NULL != hv)
			{
				for (auto it = hv->map.begin(), it_end = hv->map.end(); it != it_end; ++it)
				{
					ff::fmt(sink, "{0}{1}: {2}", (hv->map.begin() == it)?"":", ", it->first, it->second);
				}
			}

			ff::fmt(sink, "]\n");
		}
		break;

		// case REPORT_KIND__BY_TIMER_DATA:
		// {
		// 	ff::fmt(sink, "");
		// }
		// break;

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
	auto packet = packet_t {
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

	report_conf_t report_conf = {};
	// report_conf.time_window     = 60 * d_second,    // 1 minute aggregation window
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

	auto report = meow::make_unique<report___by_request_data_t>(&report_conf);
	report->ticks_init(os_unix::clock_monotonic_now());

	report->add(&packet);
	report->tick_now(os_unix::clock_monotonic_now());
	report->serialize(stdout, "first");

	report->add(&packet);
	report->add(&packet);
	report->add(&packet);
	report->tick_now(os_unix::clock_monotonic_now());
	report->serialize(stdout, "second");

	{
		auto const snapshot = report->get_snapshot();

		report->serialize(stdout, "second_nochange"); // snapshot should not change

		serialize_report_snapshot(stdout, snapshot.get(), "snapshot_1");
	}

	report->add(&packet);
	report->add(&packet);
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

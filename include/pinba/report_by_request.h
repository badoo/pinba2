#ifndef PINBA__REPORT_BY_REQUEST_H_
#define PINBA__REPORT_BY_REQUEST_H_

#include <functional>
#include <utility>

#include <boost/noncopyable.hpp>

#include <sparsehash/dense_hash_map>
#include <sparsehash/sparse_hash_map>

#include "pinba/globals.h"
#include "pinba/dictionary.h"
#include "pinba/histogram.h"
#include "pinba/report.h"
#include "pinba/report_util.h"

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

// this is the data we return from report___by_request_t snapshot
struct report_row___by_request_t
{
	report_row_data___by_request_t  data;
	histogram_t                     hv;
};

////////////////////////////////////////////////////////////////////////////////////////////////

struct report_conf___by_request_t
{
	std::string name;

	duration_t  time_window;      // total time window this report covers (report host uses this for ticking)
	uint32_t    ts_count;         // number of timeslices to store

	uint32_t    hv_bucket_count;  // number of histogram buckets, each bucket is hv_bucket_d 'wide'
	duration_t  hv_bucket_d;      // width of each hv_bucket

public: // packet filtering

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

public: // key fetchers, from packet fields and tags

	struct key_fetch_result_t
	{
		uint32_t key_value;
		bool     found;
	};

	using key_fetch_func_t = std::function<key_fetch_result_t(packet_t*)>;

	struct key_descriptor_t
	{
		std::string       name;
		key_fetch_func_t  fetcher;
	};

	std::vector<key_descriptor_t> keys;


	static inline key_descriptor_t key_descriptor_by_request_tag(str_ref tag_name, uint32_t tag_name_id)
	{
		return report_conf___by_request_t::key_descriptor_t {
			.name    = ff::fmt_str("request_tag/{0}", tag_name),
			.fetcher = [=](packet_t *packet) -> key_fetch_result_t
			{
				for (uint32_t i = 0; i < packet->tag_count; ++i)
				{
					if (packet->tags[i].name_id == tag_name_id)
					{
						return { packet->tags[i].value_id, true };
					}
				}
				return { 0, false };
			},
		};
	}
};

////////////////////////////////////////////////////////////////////////////////////////////////

struct report___by_request_t : public report_t
{
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
			hv   = std::move(other.hv); // real move
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
			hv.increment({hv_bucket_count, hv_bucket_d}, packet->request_time);
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
					dst.hv.merge_other(src.hv);
				}
			}
		}
	};

	using snapshot_t = report_snapshot__impl_t<snapshot_traits>;

public:

	report___by_request_t(pinba_globals_t *globals, report_conf___by_request_t *conf)
		: globals_(globals)
		, conf_(conf)
		, ticks_(conf_->ts_count)
	{
		// validate config
		if (conf_->keys.size() > key_t::static_size)
		{
			throw std::runtime_error(ff::fmt_str(
				"required keys ({0}) > supported keys ({1})", conf_->keys.size(), key_t::static_size));
		}

		info_ = report_info_t {
			.kind            = REPORT_KIND__BY_REQUEST_DATA,
			.timeslice_count = conf_->ts_count,
			.time_window     = conf_->time_window,
			.n_key_parts     = (uint32_t)conf_->keys.size(),
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

		// construct a key, by runinng all key fetchers
		key_t k;

		for (size_t i = 0, i_end = conf_->keys.size(); i < i_end; ++i)
		{
			auto const& key_descriptor = conf_->keys[i];

			report_conf___by_request_t::key_fetch_result_t const r = key_descriptor.fetcher(packet);
			if (!r.found)
			{
				ff::fmt(stdout, "packet {0} skipped by key fetcher {1}\n", packet, key_descriptor.name);
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

// private:
protected:
	pinba_globals_t             *globals_;
	report_conf___by_request_t  *conf_;

	report_info_t               info_;

	ticks_t                     ticks_;
};

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__REPORT_BY_REQUEST_H_
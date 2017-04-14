#include <meow/defer.hpp>
#include <boost/noncopyable.hpp>

#include <sparsehash/dense_hash_map>

#include "pinba/globals.h"
#include "pinba/histogram.h"
#include "pinba/packet.h"
#include "pinba/report.h"
#include "pinba/report_util.h"
#include "pinba/report_by_request.h"

////////////////////////////////////////////////////////////////////////////////////////////////
namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

	// this is the data we return from report___by_request_t snapshot
	struct report_row___by_request_t
	{
		report_row_data___by_request_t  data;
		histogram_t                     hv;
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
				data.time_total += packet->request_time;
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
				data.time_total += other.data.time_total;
				data.ru_utime   += other.data.ru_utime;
				data.ru_stime   += other.data.ru_stime;
				data.traffic_kb += other.data.traffic_kb;
				data.mem_usage  += other.data.mem_usage;

				// hv
				hv.merge_other(other.hv);
			}
		};

	public: // ticks

		struct raw_hashtable_t : public google::dense_hash_map<key_t, item_t, report_key__hasher_t, report_key__equal_t>
		{
			raw_hashtable_t()
			{
				this->set_empty_key(key_t{});
			}
		};

		using ticks_t       = ticks_ringbuffer_t<raw_hashtable_t>;
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

			// merge from src ringbuffer to snapshot data
			static void merge_ticks_into_data(pinba_globals_t *globals, report_info_t& rinfo, src_ticks_t& ticks, hashtable_t& to)
			{
				MEOW_DEFER(
					ticks.clear();
				);

				uint64_t key_lookups = 0;
				uint64_t hv_lookups = 0;

				for (auto const& tick : ticks)
				{
					if (!tick)
						continue;

					for (auto const& tick_pair : tick->data)
					{
						auto const& src = tick_pair.second;
						auto      & dst = to[tick_pair.first];

						dst.data.req_count  += src.data.req_count;
						dst.data.time_total += src.data.time_total;
						dst.data.ru_utime   += src.data.ru_utime;
						dst.data.ru_stime   += src.data.ru_stime;
						dst.data.traffic_kb += src.data.traffic_kb;
						dst.data.mem_usage  += src.data.mem_usage;

						if (rinfo.hv_enabled)
						{
							dst.hv.merge_other(src.hv);
							hv_lookups += src.hv.map_cref().size();
						}
					}

					key_lookups += tick->data.size();
				}

				LOG_DEBUG(globals->logger(), "prepare '{0}'; n_ticks: {1}, key_lookups: {2}, hv_lookups: {3}",
					rinfo.name, ticks.size(), key_lookups, hv_lookups);
			}
		};

		using snapshot_t = report_snapshot__impl_t<snapshot_traits>;

	public:

		report___by_request_t(pinba_globals_t *globals, report_conf___by_request_t const& conf)
			: globals_(globals)
			, conf_(conf)
			, ticks_(conf.tick_count)
		{
			// validate config
			if (conf_.keys.size() > key_t::static_size)
			{
				throw std::runtime_error(ff::fmt_str(
					"required keys ({0}) > supported keys ({1})", conf_.keys.size(), key_t::static_size));
			}

			info_ = report_info_t {
				.name            = conf_.name,
				.kind            = REPORT_KIND__BY_REQUEST_DATA,
				.time_window     = conf_.time_window,
				.tick_count      = conf_.tick_count,
				.n_key_parts     = (uint32_t)conf_.keys.size(),
				.hv_enabled      = (conf_.hv_bucket_count > 0),
				.hv_kind         = HISTOGRAM_KIND__HASHTABLE,
				.hv_bucket_count = conf_.hv_bucket_count,
				.hv_bucket_d     = conf_.hv_bucket_d,
			};
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
			ticks_.tick(curr_tv);
		}

		virtual report_estimates_t get_estimates() override
		{
			report_estimates_t result = {};

			if (tick_t *tick = ticks_.last())
				result.row_count = tick->data.size();
			else
				result.row_count = ticks_.current().data.size();


			result.mem_used += ticks_.current().data.bucket_count() * sizeof(*ticks_.current().data.begin());

			for (auto const& tick : ticks_.get_internal_buffer())
			{
				if (!tick)
					continue;

				raw_hashtable_t const& ht = tick->data;
				result.mem_used += ht.bucket_count() * sizeof(*ht.begin());

				for (auto const& key_item_pair : ht)
				{
					auto const& hv_map = key_item_pair.second.hv.map_cref();
					result.mem_used += hv_map.bucket_count() * sizeof(*hv_map.begin());
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
			// run all filters and check if packet is 'interesting to us'
			for (size_t i = 0, i_end = conf_.filters.size(); i < i_end; ++i)
			{
				auto const& filter = conf_.filters[i];
				if (!filter.func(packet))
					return;
			}

			// construct a key, by runinng all key fetchers
			key_t k;

			for (size_t i = 0, i_end = conf_.keys.size(); i < i_end; ++i)
			{
				auto const& key_descriptor = conf_.keys[i];

				report_conf___by_request_t::key_fetch_result_t const r = key_descriptor.fetcher(packet);
				if (!r.found)
					return;

				k.push_back(r.key_value);
			}

			// finally - find and update item
			item_t& item = ticks_.current().data[k];
			item.data_increment(packet);

			if (info_.hv_enabled)
			{
				item.hv_increment(packet, conf_.hv_bucket_count, conf_.hv_bucket_d);
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
		report_conf___by_request_t  conf_;

		report_info_t               info_;

		ticks_t                     ticks_;
	};

////////////////////////////////////////////////////////////////////////////////////////////////
}} // namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

report_ptr create_report_by_request(pinba_globals_t *globals, report_conf___by_request_t const& conf)
{
	return meow::make_intrusive<aux::report___by_request_t>(globals, conf);
}

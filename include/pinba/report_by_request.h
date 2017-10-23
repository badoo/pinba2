#ifndef PINBA__REPORT_BY_REQUEST_H_
#define PINBA__REPORT_BY_REQUEST_H_

#include <string>
#include <functional>

#include "pinba/globals.h"
#include "pinba/report.h"
#include "pinba/packet.h"

////////////////////////////////////////////////////////////////////////////////////////////////

struct report_row_data___by_request_t
{
	uint32_t   req_count;
	duration_t time_total;
	duration_t ru_utime;
	duration_t ru_stime;
	uint64_t   traffic;
	uint64_t   mem_used;

	report_row_data___by_request_t()
	{
		// FIXME: add a failsafe for memset
		memset(this, 0, sizeof(*this));
	}
};

////////////////////////////////////////////////////////////////////////////////////////////////

struct report_conf___by_request_t
{
	std::string name;

	duration_t  time_window;      // total time window this report covers (report host uses this for ticking)
	uint32_t    tick_count;       // number of timeslices to store

	uint32_t    hv_bucket_count;  // number of histogram buckets, each bucket is hv_bucket_d 'wide'
	duration_t  hv_bucket_d;      // width of each hv_bucket
	duration_t  hv_min_value;     // lower bound time (upper_bound = min_time + bucket_d*bucket_count)

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

	static inline filter_descriptor_t make_filter___by_request_field(uint32_t packet_t::* field_ptr, uint32_t value_id)
	{
		return filter_descriptor_t {
			.name    = ff::fmt_str("by_request_field/{0}={1}", 0/*FIXME:field_ptr*/, value_id),
			.func = [=](packet_t *packet) -> bool
			{
				return (packet->*field_ptr == value_id);
			},
		};
	}

	static inline filter_descriptor_t make_filter___by_request_tag(uint32_t name_id, uint32_t value_id)
	{
		return filter_descriptor_t {
			.name    = ff::fmt_str("by_request_tag/{0}={1}", name_id, value_id),
			.func = [=](packet_t *packet) -> bool
			{
				for (uint32_t i = 0; i < packet->tag_count; ++i)
				{
					if (packet->tag_name_ids[i] == name_id)
					{
						return (packet->tag_value_ids[i] == value_id);
					}
				}
				return false;
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

	// some builtins
	static inline key_descriptor_t key_descriptor_by_request_tag(str_ref tag_name, uint32_t tag_name_id)
	{
		return key_descriptor_t {
			.name    = ff::fmt_str("request_tag/{0}", tag_name),
			.fetcher = [=](packet_t *packet) -> key_fetch_result_t
			{
				for (uint32_t i = 0; i < packet->tag_count; ++i)
				{
					if (packet->tag_name_ids[i] == tag_name_id)
					{
						return { packet->tag_value_ids[i], true };
					}
				}
				return { 0, false };
			},
		};
	}

	static inline key_descriptor_t key_descriptor_by_request_field(str_ref field_name, uint32_t packet_t::* field_ptr)
	{
		return key_descriptor_t {
			.name    = ff::fmt_str("request_field/{0}", field_name),
			.fetcher = [=](packet_t *packet) -> key_fetch_result_t
			{
				return { packet->*field_ptr, true };
			},
		};
	}
};

////////////////////////////////////////////////////////////////////////////////////////////////

report_ptr create_report_by_request(pinba_globals_t*, report_conf___by_request_t const&);

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__REPORT_BY_REQUEST_H_
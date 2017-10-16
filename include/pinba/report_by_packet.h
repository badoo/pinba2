#ifndef PINBA__REPORT_BY_PACKET_H_
#define PINBA__REPORT_BY_PACKET_H_

#include <cstdint>
#include <functional>
#include <string>

#include "pinba/globals.h"
#include "pinba/report.h"

////////////////////////////////////////////////////////////////////////////////////////////////

struct report_row_data___by_packet_t
{
	uint32_t   req_count;
	uint32_t   timer_count;
	duration_t time_total;
	duration_t ru_utime;
	duration_t ru_stime;
	uint64_t   traffic;
	uint64_t   mem_used;

	report_row_data___by_packet_t()
	{
		// FIXME: add a failsafe for memset
		memset(this, 0, sizeof(*this));
	}
};

////////////////////////////////////////////////////////////////////////////////////////////////

struct report_conf___by_packet_t
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

};

////////////////////////////////////////////////////////////////////////////////////////////////

report_ptr create_report_by_packet(pinba_globals_t*, report_conf___by_packet_t const&);

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__REPORT_BY_PACKET_H_
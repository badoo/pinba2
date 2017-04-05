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
	uint64_t   traffic_kb;
	uint64_t   mem_usage;

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
	uint32_t    tick_count;         // number of timeslices to store

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
};

////////////////////////////////////////////////////////////////////////////////////////////////

report_ptr create_report_by_packet(pinba_globals_t*, report_conf___by_packet_t const&);

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__REPORT_BY_PACKET_H_
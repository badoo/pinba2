#ifndef PINBA__REPORT_BY_PACKET_H_
#define PINBA__REPORT_BY_PACKET_H_

#include <cstdint>
#include <string>

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
};

////////////////////////////////////////////////////////////////////////////////////////////////

report_ptr create_report_by_packet(pinba_globals_t*, report_conf___by_packet_t const&);

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__REPORT_BY_PACKET_H_
#ifndef PINBA__REPORT_BY_TIMER_H_
#define PINBA__REPORT_BY_TIMER_H_

#include <string>
#include <functional>

#include "pinba/globals.h"
#include "pinba/report.h"

////////////////////////////////////////////////////////////////////////////////////////////////

struct report_row_data___by_timer_t
{
	uint32_t    req_count;   // number of requests timer with such tag was present in
	uint32_t    hit_count;   // timer hit X times
	duration_t  time_total;  // sum of all timer values (i.e. total time spent in this timer)
	duration_t  ru_utime;    // same for rusage user
	duration_t  ru_stime;    // same for rusage system

	report_row_data___by_timer_t()
	{
		// FIXME: add a failsafe for memset
		memset(this, 0, sizeof(*this));
	}
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
	uint32_t    tick_count;         // number of timeslices to store

	uint32_t    hv_bucket_count;  // number of histogram buckets, each bucket is hv_bucket_d 'wide'
	duration_t  hv_bucket_d;      // width of each hv_bucket
	duration_t  hv_min_value;     // lower bound time (upper_bound = min_time + bucket_d*bucket_count)

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

public: // timertag filters

	struct timertag_filter_descriptor_t
	{
		std::string name;
		uint32_t    name_id;
		uint32_t    value_id;
	};

	std::vector<timertag_filter_descriptor_t> timertag_filters;

	// some builtins
	static inline timertag_filter_descriptor_t make_timertag_filter(uint32_t name_id, uint32_t value_id)
	{
		return timertag_filter_descriptor_t {
			.name     = ff::fmt_str("timer_tag/{0}={1}", name_id, value_id),
			.name_id  = name_id,
			.value_id = value_id,
		};
	}

public: // key fetchers

	struct key_descriptor_t
	{
		std::string name;
		int         kind;  // see defines above
		union {
			uintptr_t                flat_value;
			struct {
				uint32_t             timer_tag;
				uint32_t             request_tag;
				uint32_t packet_t::* request_field;
			};
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

report_ptr create_report_by_timer(pinba_globals_t*, report_conf___by_timer_t const&);

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__REPORT_BY_TIMER_H_

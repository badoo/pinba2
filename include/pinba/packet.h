#ifndef PINBA__PACKET_H_
#define PINBA__PACKET_H_

#include <cstdint>
#include <type_traits>

#include <meow/unix/time.hpp>
#include <meow/smart_enum.hpp>

#include "pinba/globals.h"
#include "pinba/bloom.h"

////////////////////////////////////////////////////////////////////////////////////////////////
// TODO: maybe move this to it's own header
#if 0
struct microseconds_t
{
	// max repserentable time interval: 2147,483 seconds ~= 35 minutes
	int32_t usec;
};

inline microseconds_t microseconds_from_float(float const d)
{
	float sec_d;
	float const usec_d = modff(d, &sec_d);

	microseconds_t const result = {
		.usec = static_cast<int32_t>(sec_d) * usec_in_sec + static_cast<int32_t>(usec_d * usec_in_sec)
	};
	return result;
}

namespace meow { namespace format {
	template<>
	struct type_tunnel<microseconds_t>
	{
		enum { buffer_size = sizeof("-1234567890.123456") };
		typedef meow::tmp_buffer<buffer_size> buffer_t;

		inline static str_ref call(microseconds_t const& m, buffer_t const& buf = buffer_t())
		{
			char *b = buf.begin();
			char *p = buf.end();
			p = detail::integer_to_string(b, p - b, (m.usec % usec_in_sec));

			// pad with '0', easier to read that way
			static unsigned const field_size = sizeof("000000") - 1;
			unsigned const printed_size = (buf.end() - p);
			for (unsigned i = 0; i < field_size - printed_size; ++i)
				*--p = '0';

			*--p = '.';
			p = detail::integer_to_string(b, p - b, (m.usec / usec_in_sec));
			return str_ref(p, buf.end());
		}
	};
}}
#endif

////////////////////////////////////////////////////////////////////////////////////////////////

struct packed_timer_t
{
	uint32_t        hit_count;
	uint32_t        tag_count;
	duration_t      value;
	duration_t      ru_utime;
	duration_t      ru_stime;
	uint32_t        *tag_name_ids;
	uint32_t        *tag_value_ids;  // TODO: remove this ptr, address via tag_name_ids
	// timer_bloom_t   bloom;           // 64bit
}; // __attribute__((packed)); // needed only with sizeof() == 28 below

// check the size, had to add __attribute__((packed)) to definition, since the compiler likes
// to have struct sizes % 8 == 0, to have them nicely aligned in arrays
// but we don't need that, since 1st member is uint32 and is aligned properly in arrays as well
// static_assert(sizeof(packed_timer_t) == 28, "make sure packed_timer_t has no padding inside");
static_assert(sizeof(packed_timer_t) == 48, "make sure packed_timer_t has no padding inside");
// static_assert(sizeof(packed_timer_t) == 56, "make sure packed_timer_t has no padding inside");
static_assert(std::is_standard_layout<packed_timer_t>::value == true, "packed_timer_t must have standard layout");

struct packet_t
{
	uint32_t          host_id;
	uint32_t          server_id;
	uint32_t          script_id;
	uint32_t          schema_id;
	uint32_t          status;
	uint32_t          traffic;         // document_size
	uint32_t          mem_used;        // memory_footprint
	uint16_t          tag_count;       // length of this->tags
	uint16_t          timer_count;     // length of this->timers
	duration_t        request_time;    // use microseconds_t here?
	duration_t        ru_utime;        // use microseconds_t here?
	duration_t        ru_stime;        // use microseconds_t here?
	uint32_t          *tag_name_ids;   // request tag names  (sequential in memory = scan speed)
	uint32_t          *tag_value_ids;  // request tag values (sequential in memory = scan speed) TODO: remove this ptr, address via tag_name_ids
	timer_bloom_t     *timers_blooms;  // blooms for all timers, sequential for check speed
	packed_timer_t    *timers;
	timertag_bloom_t  bloom;     // poor man's bloom filter over timer[].tag_name_ids
};

// packet_t has been carefully crafted to avoid padding inside and eat as little memory as possible
// make sure we haven't made a mistake anywhere
// static_assert(sizeof(packet_t) == 96, "make sure packet_t has no padding inside");
static_assert(sizeof(packet_t) == 104, "make sure packet_t has no padding inside");
static_assert(std::is_standard_layout<packet_t>::value == true, "packet_t must be a standard layout type");

////////////////////////////////////////////////////////////////////////////////////////////////

MEOW_DEFINE_SMART_ENUM(request_validate_result,
					((okay,                           "okay"))
					((status_is_too_large,            "status_is_too_large"))
					((bad_hit_count,                  "bad_hit_count"))
					((bad_tag_count,                  "bad_tag_count"))
					((not_enough_tag_names,           "not_enough_tag_names"))
					((not_enough_tag_values,          "not_enough_tag_values"))
					// ((bad_timer_ru_utime_count,     "bad_timer_ru_utime_count"))
					// ((bad_timer_ru_stime_count,     "bad_timer_ru_stime_count"))
					((bad_timer_hit_count,            "bad_timer_hit_count"))

					((bad_float_request_time,         "bad_float_request_time"))
					// ((zero_float_request_time,         "zero_float_request_time"))
					// ((negative_float_request_time,    "negative_float_request_time"))

					((bad_float_ru_utime,             "bad_float_ru_utime"))
					// ((negative_float_ru_utime,        "negative_float_ru_utime"))

					((bad_float_ru_stime,             "bad_float_ru_stime"))
					// ((negative_float_ru_stime,        "negative_float_ru_stime"))

					((bad_float_timer_value,          "bad_float_timer_value"))
					((zero_float_timer_value,         "zero_float_timer_value"))
					((negative_float_timer_value,     "negative_float_timer_value"))

					((bad_float_timer_ru_utime,       "bad_float_timer_ru_utime"))
					// ((negative_float_timer_ru_utime,  "negative_float_timer_ru_utime"))

					((bad_float_timer_ru_stime,       "bad_float_timer_ru_stime"))
					// ((negative_float_timer_ru_stime,  "negative_float_timer_ru_stime"))
					);

// validate that request makes sense and can be used further,
// i.e. other parts further down the pipeline depend on checks done here
// this function might change request slightly
// sometimes it's easier to do it here, than in pinba_request_to_packet()
request_validate_result_t pinba_validate_request(Pinba__Request *r);

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__PACKET_H_
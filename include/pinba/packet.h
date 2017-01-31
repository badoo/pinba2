#ifndef PINBA__PACKET_H_
#define PINBA__PACKET_H_

#include <cmath> // modff
#include <cstdint>
#include <type_traits>

#include <meow/format/format.hpp>
#include <meow/unix/time.hpp>
#include <meow/smart_enum.hpp>

#include "pinba/globals.h"
#include "proto/pinba.pb-c.h"

#include "misc/nmpa.h"

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

struct dictionary_t;

struct packed_tag_t
{
	uint32_t name_id;
	uint32_t value_id;
};
static_assert(sizeof(packed_tag_t) == 8, "make sure packed_tag_t has no padding inside");
static_assert(std::is_standard_layout<packed_tag_t>::value == true, "packed_tag_t must be a standard layout type");

struct packed_timer_t // sizeof == 40bytes (can reduce further using microseconds_t)
{
	uint32_t        hit_count;
	uint32_t        tag_count;
	duration_t      value;
	duration_t      ru_utime;
	duration_t      ru_stime;
	packed_tag_t   *tags;
}; // __attribute__((packed)); // needed only with sizeof() == 28 below

// check the size, had to add __attribute__((packed)) to definition, since the compiler likes
// to have struct sizes % 8 == 0, to have them nicely aligned in arrays
// but we don't need that, since 1st member is uint32 and is aligned properly in arrays as well
// static_assert(sizeof(packed_timer_t) == 28, "make sure packed_timer_t has no padding inside");
static_assert(sizeof(packed_timer_t) == 40, "make sure packed_timer_t has no padding inside");
static_assert(std::is_standard_layout<packed_timer_t>::value == true, "packed_timer_t must have standard layout");

struct packet_t
{
	uint32_t        host_id;
	uint32_t        server_id;
	uint32_t        script_id;
	uint32_t        schema_id;    // can we make this uint16_t, expecting number of schemas to be small?
	uint32_t        status;       // can we make this uint16_t for http statuses only ?
	uint32_t        doc_size;
	uint32_t        memory_peak;
	uint16_t        tag_count;    // length of this->tags
	uint16_t        timer_count;  // length of this->timers
	duration_t      request_time; // use microseconds_t here?
	duration_t      ru_utime;     // use microseconds_t here?
	duration_t      ru_stime;     // use microseconds_t here?
	dictionary_t    *dictionary;  // dictionary used to translate ids to names
	packed_tag_t    *tags;
	packed_timer_t  *timers;
};

// packet_t has been carefully crafted to avoid padding inside and eat as little memory as possible
// make sure we haven't made a mistake anywhere
static_assert(sizeof(packet_t) == 80, "make sure packet_t has no padding inside");
static_assert(std::is_standard_layout<packet_t>::value == true, "packet_t must be a standard layout type");

////////////////////////////////////////////////////////////////////////////////////////////////

struct timer_data_t
{
	uint16_t                          id;
	uint16_t                          tag_count;
	uint32_t                          hit_count;
	duration_t                        value;
	duration_t                        ru_utime;
	duration_t                        ru_stime;
	meow::string_ref<uint32_t const>  tag_name_ids;
	meow::string_ref<uint32_t const>  tag_value_ids;
};

// run Function for each timer
// Function = std::function<void(Pinba__Request *r, timer_data_t const& timer)>
template<class Function>
inline void for_each_timer(Pinba__Request *r, Function const& cb)
{
	unsigned current_tag_offset = 0;

	for (unsigned i = 0; i < r->n_timer_value; i++)
	{
		auto const tag_count = r->timer_tag_count[i];

		auto const timer = timer_data_t	{
			.id            = static_cast<uint16_t>(i),
			.tag_count     = static_cast<uint16_t>(tag_count),
			.hit_count     = r->timer_hit_count[i],
			.value         = duration_from_float(r->timer_value[i]),
			.ru_utime      = (i < r->n_timer_ru_utime) ? duration_from_float(r->timer_ru_utime[i]) : duration_t{0},
			.ru_stime      = (i < r->n_timer_ru_stime) ? duration_from_float(r->timer_ru_stime[i]) : duration_t{0},
			.tag_name_ids  = meow::ref_array(&r->timer_tag_name[current_tag_offset], tag_count),
			.tag_value_ids = meow::ref_array(&r->timer_tag_value[current_tag_offset], tag_count),
		};
		current_tag_offset += tag_count;

		cb(r, timer);
	}
};

////////////////////////////////////////////////////////////////////////////////////////////////

MEOW_DEFINE_SMART_ENUM(packet_validate_result,
					((okay,                      "okay"))
					((bad_hit_count,             "bad_hit_count"))
					((bad_tag_count,             "bad_tag_count"))
					((not_enough_tag_names,      "not_enough_tag_names"))
					((not_enough_tag_values,     "not_enough_tag_values"))
					// ((bad_timer_ru_utime_count,  "bad_timer_ru_utime_count"))
					// ((bad_timer_ru_stime_count,  "bad_timer_ru_stime_count"))
					);

packet_validate_result_t validate_packet(Pinba__Request *r);
packet_t* pinba_request_to_packet(Pinba__Request *r, dictionary_t *d, struct nmpa_s *nmpa);

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__PACKET_H_
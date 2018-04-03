#include <cmath>

#include "pinba/globals.h"
#include "pinba/limits.h"
#include "pinba/dictionary.h"
#include "pinba/packet.h"
#include "pinba/bloom.h"

#include "proto/pinba.pb-c.h"

////////////////////////////////////////////////////////////////////////////////////////////////

request_validate_result_t pinba_validate_request(Pinba__Request *r)
{
	if (r->status >= PINBA_INTERNAL___STATUS_MAX)
		return request_validate_result::status_is_too_large;

	if (r->n_timer_value != r->n_timer_hit_count) // all timers have hit counts
		return request_validate_result::bad_hit_count;

	if (r->n_timer_value != r->n_timer_tag_count) // all timers have tag counts
		return request_validate_result::bad_tag_count;

	// NOTE(antoxa): some clients don't send rusage at all, let them

	// if (r->n_timer_value != r->n_timer_ru_utime)
	// 	return request_validate_result::bad_timer_ru_utime_count;

	// if (r->n_timer_value != r->n_timer_ru_stime)
	// 	return request_validate_result::bad_timer_ru_stime_count;

	// all timer hit counts are > 0
	for (unsigned i = 0; i < r->n_timer_hit_count; i++) {
		if (r->timer_hit_count[i] <= 0)
			return request_validate_result::bad_timer_hit_count;
	}

	auto const total_tag_count = [&]()
	{
		size_t result = 0;
		for (unsigned i = 0; i < r->n_timer_tag_count; i++) {
			result += r->timer_tag_count[i];
		}
		return result;
	}();

	if (total_tag_count != r->n_timer_tag_name) // all tags have names
		return request_validate_result::not_enough_tag_names;

	if (total_tag_count != r->n_timer_tag_value) // all tags have values
		return request_validate_result::not_enough_tag_values;



	// request_time should be > 0, reset to 0 when < 0
	{
		switch (std::fpclassify(r->request_time))
		{
			case FP_ZERO:    break;
			case FP_NORMAL:	 break;
			default:         return request_validate_result::bad_float_request_time;
		}
		if (std::signbit(r->request_time))
			r->request_time = 0;
	}

	// NOTE(antoxa): this should not happen, but happens A LOT
	//               so just reset them to zero if negative
	{
		switch (std::fpclassify(r->ru_utime))
		{
			case FP_ZERO:    break;
			case FP_NORMAL:	 break;
			default:         return request_validate_result::bad_float_ru_utime;
		}
		if (std::signbit(r->ru_utime))
			r->ru_utime = 0;
	}

	{
		switch (std::fpclassify(r->ru_stime))
		{
			case FP_ZERO:    break;
			case FP_NORMAL:	 break;
			default:         return request_validate_result::bad_float_ru_stime;
		}
		if (std::signbit(r->ru_stime))
			r->ru_stime = 0;
	}

	// timer values must be >= 0
	for (unsigned i = 0; i < r->n_timer_value; i++)
	{
		switch (std::fpclassify(r->timer_value[i]))
		{
			case FP_ZERO:    break;
			case FP_NORMAL:	 break;
			default:         return request_validate_result::bad_float_timer_value;
		}
		if (std::signbit(r->timer_value[i]))
			return request_validate_result::negative_float_timer_value;
	}

	// NOTE(antoxa): same as r->ru_utime, r->ru_stime
	//               negative values happen, just make them zero
	for (unsigned i = 0; i < r->n_timer_ru_utime; i++)
	{
		switch (std::fpclassify(r->timer_ru_utime[i]))
		{
			case FP_ZERO:    break;
			case FP_NORMAL:	 break;
			default:         return request_validate_result::bad_float_timer_ru_utime;
		}
		if (std::signbit(r->timer_ru_utime[i]))
			r->timer_ru_utime[i] = 0;
	}

	for (unsigned i = 0; i < r->n_timer_ru_stime; i++)
	{
		switch (std::fpclassify(r->timer_ru_stime[i]))
		{
			case FP_ZERO:    break;
			case FP_NORMAL:	 break;
			default:         return request_validate_result::bad_float_timer_ru_stime;
		}
		if (std::signbit(r->timer_ru_stime[i]))
			r->timer_ru_stime[i] = 0;
	}

	return request_validate_result::okay;
}

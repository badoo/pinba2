#include <cmath>

#include "pinba/globals.h"
#include "pinba/dictionary.h"
#include "pinba/packet.h"

#include "proto/pinba.pb-c.h"

////////////////////////////////////////////////////////////////////////////////////////////////

request_validate_result_t pinba_validate_request(Pinba__Request *r)
{
	if (r->n_timer_value != r->n_timer_hit_count) // all timers have hit counts
		return request_validate_result::bad_hit_count;

	if (r->n_timer_value != r->n_timer_tag_count) // all timers have tag counts
		return request_validate_result::bad_tag_count;

	// if (r->n_timer_value != r->n_timer_ru_utime)
	// 	return request_validate_result::bad_timer_ru_utime_count;

	// if (r->n_timer_value != r->n_timer_ru_stime)
	// 	return request_validate_result::bad_timer_ru_stime_count;

	// all timer hit counts are > 0
	for (unsigned i = 0; i < r->n_timer_hit_count; i++) {
		if (r->timer_hit_count[i] <= 0)
			return request_validate_result::bad_timer_hit_count;
	}

	auto const total_tag_count = [](Pinba__Request *r)
	{
		size_t result = 0;
		for (unsigned i = 0; i < r->n_timer_tag_count; i++) {
			result += r->timer_tag_count[i];
		}
		return result;
	}(r);

	if (total_tag_count != r->n_timer_tag_name) // all tags have names
		return request_validate_result::not_enough_tag_names;

	if (total_tag_count != r->n_timer_tag_value) // all tags have values
		return request_validate_result::not_enough_tag_values;

#define VALIDATE_FLOAT_V(value, name)                                            \
	do {                                                                         \
		switch (std::fpclassify(value)) {                                        \
			case FP_ZERO:    break;                                              \
			case FP_NORMAL:	 break;                                              \
			default:         return request_validate_result::bad_float_##name;   \
		}                                                                        \
		if (std::signbit(value))                                                 \
			return request_validate_result::negative_float_##name;               \
	} while(0)                                                                   \
/**/

#define VALIDATE_FLOAT_NONZERO_V(value, name)                                    \
	do {                                                                         \
		switch (std::fpclassify(value)) {                                        \
			case FP_ZERO:    return request_validate_result::zero_float_##name;  \
			case FP_NORMAL:	 break;                                              \
			default:         return request_validate_result::bad_float_##name;   \
		}                                                                        \
		if (std::signbit(value))                                                 \
			return request_validate_result::negative_float_##name;               \
	} while(0)                                                                   \
/**/

#define VALIDATE_FLOAT_ARRAY(array_name)                 \
	for (unsigned i = 0; i < r->n_##array_name; i++) {   \
		VALIDATE_FLOAT_V(r->array_name[i], array_name);  \
	}                                                    \
/**/

#define VALIDATE_FLOAT_NONZERO_ARRAY(array_name)                \
	for (unsigned i = 0; i < r->n_##array_name; i++) {          \
		VALIDATE_FLOAT_NONZERO_V(r->array_name[i], array_name); \
	}                                                           \
/**/

	VALIDATE_FLOAT_NONZERO_V(r->request_time, request_time);
	VALIDATE_FLOAT_V(r->ru_utime, ru_utime);
	VALIDATE_FLOAT_V(r->ru_stime, ru_stime);

	VALIDATE_FLOAT_ARRAY(timer_value);
	VALIDATE_FLOAT_ARRAY(timer_ru_utime);
	VALIDATE_FLOAT_ARRAY(timer_ru_stime);

	return request_validate_result::okay;
}


packet_t* pinba_request_to_packet(Pinba__Request *r, dictionary_t *d, struct nmpa_s *nmpa)
{
	auto p = (packet_t*)nmpa_calloc(nmpa, sizeof(packet_t)); // NOTE: no ctor is called here!

	uint32_t td[r->n_dictionary];

	for (unsigned i = 0; i < r->n_dictionary; i++)
	{
		td[i] = d->get_or_add(r->dictionary[i]);
		// ff::fmt(stdout, "{0}; dict xform {1} -> {2}\n", __func__, r->dictionary[i], td[i]);
	}

	p->host_id      = d->get_or_add(r->hostname);
	p->server_id    = d->get_or_add(r->server_name);
	p->script_id    = d->get_or_add(r->script_name);
	p->schema_id    = d->get_or_add(r->schema);
	p->status       = r->status;
	p->doc_size     = r->document_size;
	p->memory_peak  = r->memory_peak;
	p->request_time = duration_from_float(r->request_time);
	p->ru_utime     = duration_from_float(r->ru_utime);
	p->ru_stime     = duration_from_float(r->ru_stime);
	p->dictionary   = d;

	p->timer_count = r->n_timer_value;
	p->timers = (packed_timer_t*)nmpa_alloc(nmpa, sizeof(packed_timer_t) * r->n_timer_value);
	for_each_timer(r, [&](Pinba__Request *r, timer_data_t const& timer)
	{
		packed_timer_t *t = p->timers + timer.id;
		t->hit_count = timer.hit_count;
		t->value     = timer.value;
		t->ru_utime  = timer.ru_utime;
		t->ru_stime  = timer.ru_stime;
		t->tag_count = timer.tag_count;

		if (timer.tag_count > 0)
		{
			t->tags = (packed_tag_t*)nmpa_alloc(nmpa, sizeof(packed_tag_t) * timer.tag_count);

			for (unsigned i = 0; i < timer.tag_count; i++)
			{
				t->tags[i] = { td[timer.tag_name_ids[i]], td[timer.tag_value_ids[i]] };
			}
		}
	});

	p->tag_count = r->n_tag_name;
	p->tags = (packed_tag_t*)nmpa_alloc(nmpa, sizeof(packed_tag_t) * r->n_tag_name);
	for (unsigned i = 0; i < r->n_tag_name; i++)
	{
		p->tags[i] = { td[r->tag_name[i]], td[r->tag_value[i]] };
	}

	return p;
}


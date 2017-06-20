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

template<class D>
static packet_t* pinba_request_to_packet___impl(Pinba__Request const *r, D *d, dictionary_t *dict, struct nmpa_s *nmpa)
{
	auto *p = (packet_t*)nmpa_calloc(nmpa, sizeof(packet_t)); // NOTE: no ctor is called here!

	uint32_t td[r->n_dictionary];

	for (unsigned i = 0; i < r->n_dictionary; i++)
	{
		td[i] = d->get_or_add(r->dictionary[i]);
		// ff::fmt(stdout, "{0}; dict xform {1} -> {2}\n", __func__, r->dictionary[i], td[i]);
	}

	// p->sequence_id  = 0; // there is no need to touch this field, caller know how to deal with this
	p->host_id      = d->get_or_add(r->hostname);
	p->server_id    = d->get_or_add(r->server_name);
	p->script_id    = d->get_or_add(r->script_name);
	p->schema_id    = d->get_or_add(r->schema);
	// p->status       = r->status;
	p->status       = d->get_or_add(meow::format::tunnel(r->status));
	p->traffic      = r->document_size;
	p->mem_used     = r->memory_footprint;
	p->request_time = duration_from_float(r->request_time);
	p->ru_utime     = duration_from_float(r->ru_utime);
	p->ru_stime     = duration_from_float(r->ru_stime);

	// bloom should be somewhere close to the top
	p->timer_bloom = (timertag_bloom_t*)nmpa_calloc(nmpa, sizeof(timertag_bloom_t)); // NOTE: no ctor is called here!

	// timers basic info
	p->timer_count = r->n_timer_value;
	p->timers = (packed_timer_t*)nmpa_alloc(nmpa, sizeof(packed_timer_t) * r->n_timer_value);
	for_each_timer(r, [&](Pinba__Request const *r, timer_data_t const& timer)
	{
		packed_timer_t *t = p->timers + timer.id;
		t->hit_count = timer.hit_count;
		t->value     = timer.value;
		t->ru_utime  = timer.ru_utime;
		t->ru_stime  = timer.ru_stime;
		t->tag_count = timer.tag_count;
	});

	// timer tags
	{
		// copy tag names / values in one go, have them be contiguous in memory
		uint32_t *timer_tag_name_ids = (uint32_t*)nmpa_alloc(nmpa, sizeof(uint32_t) * r->n_timer_tag_name);
		uint32_t *timer_tag_value_ids = (uint32_t*)nmpa_alloc(nmpa, sizeof(uint32_t) * r->n_timer_tag_value);
		for (unsigned i = 0; i < r->n_timer_tag_name; i++)
		{
			p->timer_bloom->add(td[r->timer_tag_name[i]]);

			timer_tag_name_ids[i] = td[r->timer_tag_name[i]];
			timer_tag_value_ids[i] = td[r->timer_tag_value[i]];
		}

		// fixup base pointers
		unsigned current_tag_offset = 0;
		for (unsigned i = 0; i < r->n_timer_value; i++)
		{
			p->timers[i].tag_name_ids = timer_tag_name_ids + current_tag_offset;
			p->timers[i].tag_value_ids = timer_tag_value_ids + current_tag_offset;
			current_tag_offset += r->timer_tag_count[i];
		}
	}

	// request tags
	p->tag_count = r->n_tag_name;
	if (p->tag_count > 0)
	{
		p->tag_name_ids  = (uint32_t*)nmpa_alloc(nmpa, sizeof(uint32_t) * r->n_tag_name);
		p->tag_value_ids = (uint32_t*)nmpa_alloc(nmpa, sizeof(uint32_t) * r->n_tag_name);
		for (unsigned i = 0; i < r->n_tag_name; i++)
		{
			p->tag_name_ids[i]  = td[r->tag_name[i]];
			p->tag_value_ids[i] = td[r->tag_value[i]];
		}
	}

	return p;
}

packet_t* pinba_request_to_packet(Pinba__Request const *r, dictionary_t *d, struct nmpa_s *nmpa)
{
	return pinba_request_to_packet___impl(r, d, d, nmpa);
}


packet_t* pinba_request_to_packet(Pinba__Request const *r, repacker_dictionary_t *d, struct nmpa_s *nmpa)
{
	return pinba_request_to_packet___impl(r, d, d->d, nmpa);
}

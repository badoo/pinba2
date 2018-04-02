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
static packet_t* pinba_request_to_packet___impl(Pinba__Request const *r, D *d, struct nmpa_s *nmpa, bool enable_bloom)
{
	auto *p = (packet_t*)nmpa_calloc(nmpa, sizeof(packet_t)); // NOTE: no ctor is called here!

	uint32_t td[r->n_dictionary]; // local word_offset -> global dictinary word_id

	for (unsigned i = 0; i < r->n_dictionary; i++)
	{
		td[i] = d->get_or_add(r->dictionary[i]);
		// ff::fmt(stdout, "{0}; dict xform {1} -> {2}\n", __func__, r->dictionary[i], td[i]);
	}

	// have we added this local word_offset to bloom?
	uint8_t bloom_added[r->n_dictionary];
	memset(bloom_added, 0, sizeof(bloom_added));


	p->host_id      = d->get_or_add(r->hostname);
	p->server_id    = d->get_or_add(r->server_name);
	p->script_id    = d->get_or_add(r->script_name);
	p->schema_id    = d->get_or_add(r->schema);
	// p->status       = r->status;
	p->status       = d->get_or_add(meow::format::type_tunnel<uint32_t>::call(r->status));
	p->traffic      = r->document_size;
	p->mem_used     = r->memory_footprint;
	p->request_time = duration_from_float(r->request_time);
	p->ru_utime     = duration_from_float(r->ru_utime);
	p->ru_stime     = duration_from_float(r->ru_stime);

	// bloom should be somewhere close to the top
	if (enable_bloom)
	{
		p->timer_bloom = (timertag_bloom_t*)nmpa_alloc(nmpa, sizeof(timertag_bloom_t));
		new (p->timer_bloom) timertag_bloom_t();
	}

	// timers
	p->timer_count = r->n_timer_value;
	if (p->timer_count > 0)
	{
		p->timers = (packed_timer_t*)nmpa_alloc(nmpa, sizeof(packed_timer_t) * r->n_timer_value);

		// contiguous storage for all timer tag names/values
		uint32_t *timer_tag_name_ids = (uint32_t*)nmpa_alloc(nmpa, sizeof(uint32_t) * r->n_timer_tag_name);
		uint32_t *timer_tag_value_ids = (uint32_t*)nmpa_alloc(nmpa, sizeof(uint32_t) * r->n_timer_tag_value);

		unsigned current_tag_offset = 0;

		for (unsigned i = 0; i < r->n_timer_value; i++)
		{
			packed_timer_t *t = &p->timers[i];
			t->tag_count     = r->timer_tag_count[i];
			t->hit_count     = r->timer_hit_count[i];
			t->value         = duration_from_float(r->timer_value[i]);
			t->ru_utime      = (i < r->n_timer_ru_utime) ? duration_from_float(r->timer_ru_utime[i]) : duration_t{0};
			t->ru_stime      = (i < r->n_timer_ru_stime) ? duration_from_float(r->timer_ru_stime[i]) : duration_t{0};

			// process timer tags
			t->tag_name_ids  = timer_tag_name_ids + current_tag_offset;
			t->tag_value_ids = timer_tag_value_ids + current_tag_offset;

			for (size_t i = 0; i < t->tag_count; i++)
			{
				// offsets in r->dictionary and td
				uint32_t const tag_name_off  = r->timer_tag_name[current_tag_offset + i];
				uint32_t const tag_value_off = r->timer_tag_value[current_tag_offset + i];

				// translate through td
				uint32_t const tag_name_id  = td[tag_name_off];
				uint32_t const tag_value_id = td[tag_value_off];

				// copy to final destination
				t->tag_name_ids[i]  = tag_name_id;
				t->tag_value_ids[i] = tag_value_id;

				// TODO: enable_bloom should only work for timer blooms, request should always be enabled
				if (enable_bloom)
				{
					// hash anyway, since we need to construct timer-level bloom
					uint64_t const tag_name_hashed = pinba::fixlen_bloom___hash(tag_name_id);

					// ff::fmt(stdout, "bloom add: [{0}] {1} -> {2}\n", d->get_word(tag_name_id), tag_name_id, tag_name_hashed);

					t->bloom.add_hashed(tag_name_hashed);

					// maybe also add to packet-level bloom, if we haven't already
					if (0 == bloom_added[tag_name_off])
					{
						bloom_added[tag_name_off] = 1;
						p->timer_bloom->add_hashed(tag_name_hashed);
					}
				}
			}

			// ff::fmt(stdout, "timer_bloom[{0}]: {1}\n", i, t->bloom.to_string());

			// advance base offset in original request
			current_tag_offset += t->tag_count;
		}
	}

#if 0
		// timer tags
		if (r->n_timer_tag_name > 0)
		{
			// copy tag names / values in one go, have them be contiguous in memory
			uint32_t *timer_tag_name_ids = (uint32_t*)nmpa_alloc(nmpa, sizeof(uint32_t) * r->n_timer_tag_name);
			uint32_t *timer_tag_value_ids = (uint32_t*)nmpa_alloc(nmpa, sizeof(uint32_t) * r->n_timer_tag_value);

			for (unsigned i = 0; i < r->n_timer_tag_name; i++)
			{
				uint32_t const tag_name_off = r->timer_tag_name[i];
				uint32_t const tag_value_off = r->timer_tag_value[i];

				if (enable_bloom)
				{
					// hash anyway, since we need to construct timer-level bloom
					uint64_t const tag_name_hashed = pinba::fixlen_bloom___hash(td[tag_name_off]);
					p->timers[i].bloom.add(tag_name_hashed); // FIXME: not very cache friendly touching p->timers[] here

					// maybe also add to packet-level bloom, if we haven't already
					if (0 == bloom_added[tag_name_off])
					{
						bloom_added[tag_name_off] = 1;
						p->timer_bloom->add(tag_name_hashed);
					}
				}

				timer_tag_name_ids[i] = td[tag_name_off];
				timer_tag_value_ids[i] = td[tag_value_off];
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
	}
#endif

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

packet_t* pinba_request_to_packet(Pinba__Request const *r, dictionary_t *d, struct nmpa_s *nmpa, bool enable_bloom)
{
	return pinba_request_to_packet___impl(r, d, nmpa, enable_bloom);
}


packet_t* pinba_request_to_packet(Pinba__Request const *r, repacker_dictionary_t *d, struct nmpa_s *nmpa, bool enable_bloom)
{
	return pinba_request_to_packet___impl(r, d, nmpa, enable_bloom);
}

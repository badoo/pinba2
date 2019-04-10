#ifndef PINBA__PACKET_IMPL_H_
#define PINBA__PACKET_IMPL_H_

#include <vector>
#include <string>

#include "pinba/globals.h"
#include "pinba/packet.h"
#include "pinba/bloom.h"
#include "pinba/hash.h"
#include "pinba/dictionary.h"

#include "proto/pinba.pb-c.h"

#include "misc/nmpa.h"

////////////////////////////////////////////////////////////////////////////////////////////////
// helper to convert protobuf-c strings and bindata to str_ref

inline meow::str_ref pb_string_as_str_ref(char const *pb_string)
{
	return { pb_string };
}

inline meow::str_ref pb_string_as_str_ref(ProtobufCBinaryData const& pb_bin)
{
	return { (char const*)pb_bin.data, pb_bin.len };
}

////////////////////////////////////////////////////////////////////////////////////////////////

inline std::vector<std::string> pinba_request_status_to_str_ref___generate(size_t sz)
{
	std::vector<std::string> result;
	result.resize(sz);

	for (size_t i = 0; i < result.size(); i++)
	{
		result[i] = ff::write_str(i);
	}

	return result;
}

inline meow::str_ref pinba_request_status_to_str_ref_tmp(uint32_t status, meow::format::type_tunnel<uint32_t>::buffer_t const& buf = {})
{
	// a table with pre-generated strings for all http statuses
	static auto const table = pinba_request_status_to_str_ref___generate(1024);

	if (status < table.size())
		return table[status];

	return meow::format::type_tunnel<uint32_t>::call(status, buf);
}

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
inline void for_each_timer(Pinba__Request const *r, Function const& cb)
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

template<class D>
inline packet_t* pinba_request_to_packet(Pinba__Request const *r, D *d, struct nmpa_s *nmpa)
{
	auto *p = (packet_t*)nmpa_calloc(nmpa, sizeof(packet_t)); // NOTE: no ctor is called here!

	uint32_t td[r->n_dictionary];        // local word_offset -> global dictinary word_id
	uint64_t td_hashed[r->n_dictionary]; // global dictionary word_id -> number hashed

	for (unsigned i = 0; i < r->n_dictionary; i++)
	{
		td[i]        = d->get_or_add(pb_string_as_str_ref(r->dictionary[i]));
		td_hashed[i] = pinba::hash_number(td[i]);
		// ff::fmt(stdout, "{0}; dict xform {1} -> {2} {3}\n", __func__, r->dictionary[i], td[i], td_hashed[i]);
	}

	// have we added this local word_offset to bloom?
	uint8_t bloom_added[r->n_dictionary];
	memset(bloom_added, 0, sizeof(bloom_added));


	p->host_id      = d->get_or_add(pb_string_as_str_ref(r->hostname));
	p->server_id    = d->get_or_add(pb_string_as_str_ref(r->server_name));
	p->script_id    = d->get_or_add(pb_string_as_str_ref(r->script_name));
	p->schema_id    = d->get_or_add(pb_string_as_str_ref(r->schema));
	p->status       = d->get_or_add(pinba_request_status_to_str_ref_tmp(r->status));
	p->traffic      = r->document_size;
	p->mem_used     = r->memory_footprint;
	p->request_time = duration_from_float(r->request_time);
	p->ru_utime     = duration_from_float(r->ru_utime);
	p->ru_stime     = duration_from_float(r->ru_stime);

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

				// packet and timer level blooms
				{
					// ff::fmt(stdout, "bloom add: [{0}] {1} -> {2}\n", d->get_word(tag_name_id), tag_name_id, td_hashed[tag_name_off]);

					// always add tag name to timer bloom for current timer
					t->bloom.add_hashed(td_hashed[tag_name_off]);

					// maybe also add to packet-level bloom, if we haven't already
					if (0 == bloom_added[tag_name_off])
					{
						bloom_added[tag_name_off] = 1;
						p->timer_bloom.add_hashed(td_hashed[tag_name_off]);
					}
				}
			}

			// ff::fmt(stdout, "timer_bloom[{0}]: {1}\n", i, t->bloom.to_string());

			// advance base offset in original request
			current_tag_offset += t->tag_count;
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


template<class SinkT>
inline SinkT& debug_dump_packet(SinkT& sink, packet_t *packet, dictionary_t *d, struct nmpa_s *nmpa = NULL)
{
	auto const n_timer_tags = [&]()
	{
		uint32_t result = 0;
		for (unsigned i = 0; i < packet->timer_count; i++)
			result += packet->timers[i].tag_count;
		return result;
	}();

	ff::fmt(sink, "p: {0}, n_req_tags: {1}, n_timers: {2}, n_timer_tags: {3}\n",
		packet, packet->tag_count, packet->timer_count, n_timer_tags);

	ff::fmt(sink, "host: {0} [{1}], server: {2} [{3}], script: {4} [{5}]\n",
		d->get_word(packet->host_id), packet->host_id,
		d->get_word(packet->server_id), packet->server_id,
		d->get_word(packet->script_id), packet->script_id);

	ff::fmt(sink, "req_time: {0}, ru_u: {1}, ru_s: {2}, schema: {3} [{4}], status: {5} [{6}], mem_footprint: {7}, traffic: {8}\n",
		packet->request_time, packet->ru_utime, packet->ru_stime,
		d->get_word(packet->schema_id), packet->schema_id,
		d->get_word(packet->status), packet->status,
		packet->mem_used, packet->traffic);

	ff::fmt(sink, "bloom: {0}\n", packet->timer_bloom.to_string());

	for (unsigned i = 0; i < packet->tag_count; i++)
	{
		auto const name_id = packet->tag_name_ids[i];
		auto const value_id = packet->tag_value_ids[i];
		ff::fmt(sink, "  tag[{0}]: {{ [{1}] {2} -> {3} [{4}] }\n",
			i,
			name_id, d->get_word(name_id),
			d->get_word(value_id), value_id);
	}

	for (unsigned i = 0; i < packet->timer_count; i++)
	{
		auto const& t = packet->timers[i];

		ff::fmt(sink, "  timer[{0}]: {{ h: {1}, v: {2}, ru_u: {3}, ru_s: {4} }\n", i, t.hit_count, t.value, t.ru_utime, t.ru_stime);
		ff::fmt(sink, "    bloom: {0}\n", t.bloom.to_string());

		for (unsigned j = 0; j < t.tag_count; j++)
		{
			auto const name_id = t.tag_name_ids[j];
			auto const value_id = t.tag_value_ids[j];

			ff::fmt(sink, "    [{0}] {1} -> {2} [{3}]\n",
				name_id, d->get_word(name_id),
				d->get_word(value_id), value_id);
		}
	}

	ff::fmt(sink, "\n");

	return sink;
}

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__PACKET_IMPL_H_

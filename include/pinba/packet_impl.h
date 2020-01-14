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
inline packet_t* pinba_request_to_packet(Pinba__Request const *r, nameword_dictionary_t *nw_d, D *d, struct nmpa_s *nmpa)
{
	auto *p = (packet_t*)nmpa_calloc(nmpa, sizeof(packet_t)); // NOTE: no ctor is called here!

	struct name_id_t
	{
		// TODO: maybe redo with bit flags, and not status numbers (but probably doesn't matter)
		enum : uint8_t { not_checked = 0, not_found = 1, ok = 2 };

		uint8_t  status;
		uint8_t  bloom_added;
		uint32_t word_id;
		uint64_t bloom_hashed;
	};
	name_id_t names_translated[r->n_dictionary];
	memset(names_translated, 0, sizeof(names_translated)); // FIXME: can zerofill status only

	auto const get_name_id_by_dict_offset = [&](uint32_t dict_offset) -> name_id_t&
	{
		name_id_t& nid = names_translated[dict_offset];

		// ff::fmt(stderr, "name_get: {0}:{1} [b] {{ {2}, {3} }"
		// 	, dict_offset, pb_string_as_str_ref(r->dictionary[dict_offset]), nid.status, nid.word_id);

		if (nid.status == name_id_t::not_checked)
		{
			// uint32_t const word_id = d->get_or_add(pb_string_as_str_ref(r->dictionary[dict_offset]));
			// dictionary_t::nameword_t const nw = d->get_nameword(pb_string_as_str_ref(r->dictionary[dict_offset]));
			nameword_dictionary_t::nameword_t const *nw = nw_d->get(pb_string_as_str_ref(r->dictionary[dict_offset]));

			nid.status += (nw != nullptr) + 1;
			if (nid.status == name_id_t::ok)
			{
				nid.word_id      = nw->id;
				nid.bloom_hashed = nw->id_hash;
			}
		}

		// ff::fmt(stderr, " -> {{ {0}, {1}, {2} }\n", nid.status, nid.word_id, nid.bloom_hashed);

		return nid;
	};

	struct value_id_t
	{
		enum : uint8_t { not_checked = 0, ok = 1 };

		uint8_t  status;
		uint32_t word_id;
	};
	value_id_t values_translated[r->n_dictionary];
	memset(values_translated, 0, sizeof(values_translated));

	auto const get_value_id_by_dict_offset = [&](uint32_t dict_offset) -> value_id_t const&
	{
		value_id_t& vid = values_translated[dict_offset];

		// ff::fmt(stderr, "value_get: {0}:{1} [b] {{ {2}, {3} }"
		// 	, dict_offset, pb_string_as_str_ref(r->dictionary[dict_offset]), vid.status, vid.word_id);

		if (vid.status == value_id_t::not_checked)
		{
			uint32_t const word_id = d->get_or_add(pb_string_as_str_ref(r->dictionary[dict_offset]));
			vid.status  = value_id_t::ok;
			vid.word_id = word_id;
		}

		// ff::fmt(stderr, " -> {{ {0}, {1} }\n", vid.status, vid.word_id);

		return vid;
	};

	p->host_id      = d->get_or_add(pb_string_as_str_ref(r->hostname));
	p->server_id    = d->get_or_add(pb_string_as_str_ref(r->server_name));
	p->script_id    = d->get_or_add(pb_string_as_str_ref(r->script_name));
	p->schema_id    = d->get_or_add(pb_string_as_str_ref(r->schema));
	p->status       = d->get_or_add(pinba_request_status_to_str_ref_tmp(r->status)); // TODO: can avoid get_or_add for small values (cache in perm dict)
	p->traffic      = r->document_size;
	p->mem_used     = r->memory_footprint;
	p->request_time = duration_from_float(r->request_time);
	p->ru_utime     = duration_from_float(r->ru_utime);
	p->ru_stime     = duration_from_float(r->ru_stime);

	// timers
	p->timer_count = r->n_timer_value;
	if (p->timer_count > 0)
	{
		p->timers_blooms = (timer_bloom_t*)nmpa_alloc(nmpa, sizeof(timer_bloom_t) * r->n_timer_value);
		p->timers = (packed_timer_t*)nmpa_alloc(nmpa, sizeof(packed_timer_t) * r->n_timer_value);

		// contiguous storage for all timer tag names/values
		uint32_t *timer_tag_name_ids = (uint32_t*)nmpa_alloc(nmpa, sizeof(uint32_t) * r->n_timer_tag_name);
		uint32_t *timer_tag_value_ids = (uint32_t*)nmpa_alloc(nmpa, sizeof(uint32_t) * r->n_timer_tag_value);

		unsigned src_tag_offset = 0;
		unsigned dst_tag_offset = 0;

		for (unsigned timer_i = 0; timer_i < r->n_timer_value; timer_i++)
		{
			packed_timer_t *t = &p->timers[timer_i];
			t->tag_count     = 0; // see it's incremented when scanning tags (as we can skip)
			t->hit_count     = r->timer_hit_count[timer_i];
			t->value         = duration_from_float(r->timer_value[timer_i]);
			t->ru_utime      = (timer_i < r->n_timer_ru_utime) ? duration_from_float(r->timer_ru_utime[timer_i]) : duration_t{0};
			t->ru_stime      = (timer_i < r->n_timer_ru_stime) ? duration_from_float(r->timer_ru_stime[timer_i]) : duration_t{0};

			t->tag_name_ids  = timer_tag_name_ids + dst_tag_offset;
			t->tag_value_ids = timer_tag_value_ids + dst_tag_offset;

			uint32_t const src_tag_count = r->timer_tag_count[timer_i];

			for (unsigned tag_i = 0; tag_i < src_tag_count; tag_i++)
			{
				// offsets in r->dictionary and td
				uint32_t const tag_name_off  = r->timer_tag_name[src_tag_offset + tag_i];
				uint32_t const tag_value_off = r->timer_tag_value[src_tag_offset + tag_i];

				// find name, it must be present
				// if not present - just skip the tag completely, and don't check or add the value
				name_id_t& nid = get_name_id_by_dict_offset(tag_name_off);
				if (nid.status != name_id_t::ok)
					continue;

				// translate value, it's going to be added if not already present
				value_id_t const& vid = get_value_id_by_dict_offset(tag_value_off);

				// copy to final destination
				t->tag_name_ids[t->tag_count]  = nid.word_id;
				t->tag_value_ids[t->tag_count] = vid.word_id;
				t->tag_count++;

				// packet and timer level blooms
				{
					// ff::fmt(stdout, "bloom add: [{0}] {1} -> {2}\n", d->get_word(tag_name_id), tag_name_id, td_hashed[tag_name_off]);

					// always add tag name to timer bloom for current timer
					p->timers_blooms[timer_i].add_hashed(nid.bloom_hashed);

					// maybe also add to packet-level bloom, if we haven't already
					if (0 == nid.bloom_added)
					{
						nid.bloom_added = 1;
						p->bloom.add_hashed(nid.bloom_hashed);
					}
				}
			}

			// ff::fmt(stdout, "timer_bloom[{0}]: {1}\n", i, t->bloom.to_string());

			// advance base offset in original request
			src_tag_offset += src_tag_count;
			dst_tag_offset += t->tag_count;
		}
	}

	// request tags
	if (r->n_tag_name > 0)
	{
		p->tag_count     = 0; // see it's incremented below (as we can skip tags)
		p->tag_name_ids  = (uint32_t*)nmpa_alloc(nmpa, sizeof(uint32_t) * r->n_tag_name);
		p->tag_value_ids = (uint32_t*)nmpa_alloc(nmpa, sizeof(uint32_t) * r->n_tag_name);

		for (unsigned tag_i = 0; tag_i < r->n_tag_name; tag_i++)
		{
			name_id_t const& nid  = get_name_id_by_dict_offset(r->tag_name[tag_i]);
			if (nid.status != name_id_t::ok)
				continue;

			value_id_t const& vid = get_value_id_by_dict_offset(r->tag_value[tag_i]);

			// copy to dest
			p->tag_name_ids[p->tag_count]  = nid.word_id;
			p->tag_value_ids[p->tag_count] = vid.word_id;
			p->tag_count++;
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

	ff::fmt(sink, "bloom: {0}\n", packet->bloom.to_string());

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
		auto const& tbloom = packet->timers_blooms[i];
		auto const& t      = packet->timers[i];

		ff::fmt(sink, "  timer[{0}]: {{ h: {1}, v: {2}, ru_u: {3}, ru_s: {4} }\n", i, t.hit_count, t.value, t.ru_utime, t.ru_stime);
		ff::fmt(sink, "    bloom: {0}\n", tbloom.to_string());

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

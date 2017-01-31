#include <stdexcept>
#include <unordered_map>
#include <vector>
#include <type_traits>
#include <cstdint>

#include <meow/str_ref.hpp>
#include <meow/stopwatch.hpp>
#include <meow/hash/hash.hpp>
#include <meow/hash/hash_impl.hpp>
#include <meow/format/format.hpp>
#include <meow/format/format_to_string.hpp>
#include <meow/unix/time.hpp>

#include "pinba/dictionary.h"
#include "pinba/collector.h"
#include "pinba/packet.h"

// this builtin is unavailable in C++ mode, so have to disable
// #define __builtin_types_compatible_p(a, b) true
#include "nmpa.h"
#include "nmpa_pba.h"

using meow::string_ref;

////////////////////////////////////////////////////////////////////////////////////////////////
#if 0
struct dictionary_t
{
	typedef std::vector<std::string>                                   words_t;
	typedef std::unordered_map<str_ref, uint32_t, meow::hash<str_ref>> hash_t;

	words_t  words;
	hash_t   hash;

	str_ref get_word(uint32_t word_id)
	{
		if (word_id >= words.size())
			return {};
		return words[word_id];
	}

	uint32_t get_or_add(str_ref const word)
	{
		auto const it = hash.find(word);
		if (hash.end() != it)
		{
			return it->second;
		}

		// insert new element
		words.push_back(word.str());

		assert(words.size() < size_t(INT_MAX));

		auto const word_id = static_cast<uint32_t>(words.size() - 1);
		hash.insert({word, word_id});
		return word_id;
	}
};
#endif
////////////////////////////////////////////////////////////////////////////////////////////////
#if 0
packet_t* pinba_request_to_packet(Pinba__Request *r, dictionary_t *d, struct nmpa_s *nmpa)
{
	auto p = (packet_t*)nmpa_calloc(nmpa, sizeof(packet_t)); // NOTE: no ctor is called here!

	uint32_t td[r->n_dictionary] = {0};

	for (unsigned i = 0; i < r->n_dictionary; i++)
	{
		td[i] = d->get_or_add(r->dictionary[i]);
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
#endif

static inline size_t nmpa_user_space_used(const struct nmpa_s *nmpa)
{
	size_t ret = 0; //array_mem_used(&nmpa->pool) + array_mem_used(&nmpa->big_chunks);

	for (unsigned i = 0; i < nmpa->pool.used; i++) {
		const struct array_s *a = array_v(&nmpa->pool, struct array_s) + i;
		if (i == nmpa->pool.used - 1) {
			ret += a->sz * a->used;
			continue;
		}
		ret += array_mem_used(a);
	}

	for (unsigned i = 0; i < nmpa->big_chunks.used; i++) {
		const struct array_s *a = array_v(&nmpa->big_chunks, struct array_s) + i;
		ret += array_mem_used(a);
	}

	return ret;
}


inline void dump_packet(packet_t *packet, struct nmpa_s *nmpa)
{
	auto const n_timer_tags = [&]()
	{
		uint32_t result = 0;
		for (unsigned i = 0; i < packet->timer_count; i++)
			result += packet->timers[i].tag_count;
		return result;
	}();

	ff::fmt(stderr, "memory: {0}, {1}\n", nmpa_mem_used(nmpa), nmpa_user_space_used(nmpa));
	ff::fmt(stderr, "p: {0}, {1}, {2}, {3}\n", packet, sizeof(*packet), sizeof(packet->timers[0]), sizeof(packet->tags[0]));
	ff::fmt(stderr, "p: {0}, {1}, {2}, n_timers: {3}, n_tags: {4}, n_timer_tags: {5}\n",
		packet->dictionary->get_word(packet->host_id),
		packet->dictionary->get_word(packet->server_id),
		packet->dictionary->get_word(packet->script_id),
		packet->timer_count, packet->tag_count, n_timer_tags);

	for (unsigned i = 0; i < packet->timer_count; i++)
	{
		auto const& t = packet->timers[i];

		ff::fmt(stdout, "  t[{0}]: {{ {1}, {2}, {3} }\n", i, t.value, t.ru_utime, t.ru_stime);
		for (unsigned j = 0; j < t.tag_count; j++)
		{
			auto const& tag = t.tags[j];

			ff::fmt(stdout, "    {0}:{1} -> {2}:{3}\n",
				tag.name_id, packet->dictionary->get_word(tag.name_id),
				tag.value_id, packet->dictionary->get_word(tag.value_id));
		}
		ff::fmt(stdout, "\n");
	}
}

// typedef std::unique_ptr<nmpa_s, nmpa_free> nmpa_ptr;

// inline nmpa_ptr nmpa_create(size_t block_sz)
// {
// 	nmpa_ptr nmpa { new nmpa_s; };
// 	nmpa_init(nmpa.get(), block_sz);
// 	return move(nmpa);
// }

////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char const *argv[])
try
{
	nmpa_s nmpa;
	nmpa_init(&nmpa, 64 * 1024);

	auto pba = ProtobufCAllocator {
		.alloc = nmpa___pba_alloc,
		.free = nmpa___pba_free,
		.allocator_data = &nmpa,
	};

	dictionary_t d;

	if (argc < 2)
		throw std::runtime_error(ff::fmt_str("usage {0} <filename>", argv[0]));

	FILE *f = fopen(argv[1], "r");
	if (NULL == f)
		throw std::runtime_error(ff::fmt_str("open failed: {0}:{1}", errno, strerror(errno)));

	uint8_t buf[64*1024] = {0};
	int const buf_sz = fread(buf, 1, sizeof(buf), f);

	ff::fmt(stderr, "got packet {0} bytes\n", buf_sz);

	auto *req = [](uint8_t const *buf, size_t buf_sz)
	{
		static nmpa_s local_nmpa;
		nmpa_init(&local_nmpa, 16 * 1024);

		auto local_pba = ProtobufCAllocator {
			.alloc = nmpa___pba_alloc,
			.free = nmpa___pba_free,
			.allocator_data = &local_nmpa,
		};

		auto *req = pinba__request__unpack(&local_pba, buf_sz, buf);
		if (req == NULL) {
			throw std::runtime_error("packet decode failed\n");
		}

		ff::fmt(stderr, "decoded size {0} bytes, dict_size: {1}\n", nmpa_user_space_used(&local_nmpa), req->n_dictionary);

		return req;
	}(buf, buf_sz);

	// just unpack + print
	{
		Pinba__Request *request = pinba__request__unpack(&pba, buf_sz, buf);
		if (request == NULL) {
			throw std::runtime_error("packet decode failed\n");
		}

		packet_t *packet = pinba_request_to_packet(request, &d, &nmpa);

		dump_packet(packet, &nmpa);
		nmpa_empty(&nmpa);
	}

	size_t constexpr n_iterations = 100 * 1000;

	auto const run_deserialize = [&](str_ref name, ProtobufCAllocator *pba)
	{
		meow::stopwatch_t sw;

		for (size_t i = 0; i < n_iterations; i++)
		{
			Pinba__Request *request = pinba__request__unpack(pba, buf_sz, buf);
			if (request == NULL) {
				throw std::runtime_error("packet decode failed\n");
			}

			nmpa_empty(&nmpa);
		}

		auto const elapsed = sw.stamp();
		ff::fmt(stderr, "{0}; {1} iterations, {2} lookups, {3} inserts, elapsed: {4}, {5} req/sec\n",
			name, n_iterations, d.lookup_count, d.insert_count, elapsed, (double)n_iterations / timeval_to_double(elapsed));
	};

	auto const run_repack = [&](str_ref name, ProtobufCAllocator *pba)
	{
		meow::stopwatch_t sw;

		for (size_t i = 0; i < n_iterations; i++)
		{
			packet_t *packet = pinba_request_to_packet(req, &d, &nmpa);
			nmpa_empty(&nmpa);
		}

		auto const elapsed = sw.stamp();
		ff::fmt(stderr, "{0}; {1} iterations, {2} lookups, {3} inserts, elapsed: {4}, {5} req/sec\n",
			name, n_iterations, d.lookup_count, d.insert_count, elapsed, (double)n_iterations / timeval_to_double(elapsed));
	};

	auto const run_full_repack = [&](str_ref name, ProtobufCAllocator *pba)
	{
		meow::stopwatch_t sw;

		for (size_t i = 0; i < n_iterations; i++)
		{
			Pinba__Request *request = pinba__request__unpack(pba, buf_sz, buf);
			if (request == NULL) {
				throw std::runtime_error("packet decode failed\n");
			}

			packet_t *packet = pinba_request_to_packet(request, &d, &nmpa);

			nmpa_empty(&nmpa);
		}

		auto const elapsed = sw.stamp();
		ff::fmt(stderr, "{0}; {1} iterations, {2} lookups, {3} inserts, elapsed: {4}, {5} req/sec\n",
			name, n_iterations, d.lookup_count, d.insert_count, elapsed, (double)n_iterations / timeval_to_double(elapsed));
	};

	// run_deserialize("deserialize[with_pba]", &pba);
	// run_deserialize("deserialize[no_pba]", NULL);

	run_repack("repack[with_pba]", &pba);
	// run_repack("repack[no_pba]", NULL);

	// run_full_repack("full[with_pba]", &pba);
	// run_full_repack("full[no_pba]", NULL);

	return 0;
}
catch (std::exception const& e)
{
	ff::fmt(stderr, "error: {0}\n", e.what());
	return 1;
}

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

#include "misc/nmpa.h"
#include "misc/nmpa_pba.h"

using meow::string_ref;

////////////////////////////////////////////////////////////////////////////////////////////////

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


inline void dump_packet(packet_t *packet, dictionary_t const *d, struct nmpa_s *nmpa)
{
	auto const n_timer_tags = [&]()
	{
		uint32_t result = 0;
		for (unsigned i = 0; i < packet->timer_count; i++)
			result += packet->timers[i].tag_count;
		return result;
	}();

	ff::fmt(stderr, "memory: {0}, {1}\n", nmpa_mem_used(nmpa), nmpa_user_space_used(nmpa));
	ff::fmt(stderr, "p: {0}, {1}, {2}, {3}\n", packet, sizeof(*packet), sizeof(packet->timers[0]), sizeof(packed_tag_t));
	ff::fmt(stderr, "p: {0}, {1}, {2}, n_timers: {3}, n_tags: {4}, n_timer_tags: {5}\n",
		d->get_word___noref(packet->host_id),
		d->get_word___noref(packet->server_id),
		d->get_word___noref(packet->script_id),
		packet->timer_count, packet->tag_count, n_timer_tags);

	for (unsigned i = 0; i < packet->timer_count; i++)
	{
		auto const& t = packet->timers[i];

		ff::fmt(stdout, "  t[{0}]: {{ {1}, {2}, {3} }\n", i, t.value, t.ru_utime, t.ru_stime);
		for (unsigned j = 0; j < t.tag_count; j++)
		{
			auto const name_id = t.tag_name_ids[j];
			auto const value_id = t.tag_value_ids[j];

			ff::fmt(stdout, "    {0}:{1} -> {2}:{3}\n",
				name_id, d->get_word___noref(name_id),
				value_id, d->get_word___noref(value_id));
		}
		ff::fmt(stdout, "\n");
	}
}

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

		dump_packet(packet, &d, &nmpa);
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
		ff::fmt(stderr, "{0}; {1} iterations, elapsed: {2}, {3} req/sec\n",
			name, n_iterations, elapsed, (double)n_iterations / timeval_to_double(elapsed));
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
		ff::fmt(stderr, "{0}; {1} iterations, elapsed: {2}, {3} req/sec\n",
			name, n_iterations, elapsed, (double)n_iterations / timeval_to_double(elapsed));
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
		ff::fmt(stderr, "{0}; {1} iterations, elapsed: {2}, {3} req/sec\n",
			name, n_iterations, elapsed, (double)n_iterations / timeval_to_double(elapsed));
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

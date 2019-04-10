#include <stdexcept>
#include <unordered_map>
#include <vector>
#include <type_traits>
#include <cstdint>

#include <sparsehash/dense_hash_map>

#include <meow/str_ref.hpp>
#include <meow/stopwatch.hpp>
#include <meow/hash/hash.hpp>
#include <meow/hash/hash_impl.hpp>
#include <meow/format/format.hpp>
#include <meow/format/format_to_string.hpp>
#include <meow/unix/time.hpp>

#include "pinba/globals.h"
#include "pinba/dictionary.h"
#include "pinba/collector.h"
#include "pinba/packet.h"
#include "pinba/packet_impl.h"
#include "pinba/bloom.h"

#include "proto/pinba.pb-c.h"

#include "misc/nmpa.h"
#include "misc/nmpa_pba.h"

using meow::string_ref;

////////////////////////////////////////////////////////////////////////////////////////////////

struct permanent_dictionary_t
{
	// struct hasher_t
	// {
	// 	inline uint64_t operator()(str_ref const& key) const
	// 	{
	// 		return t1ha0(key.data(), key.size(), 0);
	// 	}
	// };

	using words_t = std::deque<std::string>; // deque to save a lil on push_back reallocs
	using hash_t  = google::dense_hash_map<str_ref, uint32_t, dictionary_word_hasher_t>;

	mutable rw_mutex_t mtx_;

	words_t  words;
	hash_t   hash;

	uint64_t mem_used_by_word_strings;
	uint64_t lookup_count;
	uint64_t insert_count;

	permanent_dictionary_t()
		: hash(64 * 1024)
		, mem_used_by_word_strings(0)
		, lookup_count(0)
		, insert_count(0)
	{
		hash.set_empty_key(str_ref{});
	}

	uint32_t size() const
	{
		scoped_read_lock_t lock_(mtx_);
		return words.size();
	}

	str_ref get_word(uint32_t word_id) const
	{
		if (word_id == 0)
			return {};

		scoped_read_lock_t lock_(mtx_);

		if (word_id > words.size())
			return {};

		return words[word_id-1];
	}

	uint32_t get_or_add(str_ref const word)
	{
		if (!word)
			return 0;

		// fastpath
		scoped_read_lock_t lock_(mtx_);

		{
			++this->lookup_count;
			auto const it = hash.find(word);
			if (hash.end() != it)
				return it->second;
		}

		// slowpath
		lock_.upgrade_to_wrlock();

		return this->get_or_add_wrlocked(word);
	}

	uint32_t get_or_add_pessimistic(str_ref const word, str_ref *real_string)
	{
		if (!word)
			return 0;

		scoped_write_lock_t lock_(mtx_);

		uint32_t const word_id = this->get_or_add_wrlocked(word);

		if (real_string)
			*real_string = words[word_id - 1];

		return word_id;
	}

private:

	uint32_t get_or_add_wrlocked(str_ref const word)
	{
		++this->lookup_count;
		auto const it = hash.find(word);
		if (hash.end() != it)
			return it->second;

		mem_used_by_word_strings += word.size();

		// insert new element
		words.push_back(word.str());

		// word_id starts with 1, since 0 is reserved for empty
		assert(words.size() < size_t(INT_MAX));
		auto const word_id = static_cast<uint32_t>(words.size());

		++this->insert_count;
		hash.insert({words.back(), word_id});

		return word_id;
	}
};

////////////////////////////////////////////////////////////////////////////////////////////////

struct str_timer_t
{
	uint32_t        hit_count;
	uint32_t        tag_count;
	duration_t      value;
	duration_t      ru_utime;
	duration_t      ru_stime;
	uint32_t        *tag_name_ids;
	str_ref         *tag_values;  // TODO: remove this ptr, address via tag_name_ids
};

static_assert(sizeof(str_timer_t) == 48, "make sure str_timer_t has no padding inside");
static_assert(std::is_standard_layout<str_timer_t>::value == true, "str_timer_t must have standard layout");

struct str_packet_t
{
	str_ref           host;
	str_ref           server;
	str_ref           script;
	str_ref           schema;          // can we make this uint16_t, expecting number of schemas to be small?
	str_ref           status;          // can we make this uint16_t for http statuses only ?
	uint32_t          traffic;         // document_size
	uint32_t          mem_used;        // memory_footprint
	uint16_t          tag_count;       // length of this->tags
	uint16_t          timer_count;     // length of this->timers
	uint32_t          PADDING___;
	duration_t        request_time;    // use microseconds_t here?
	duration_t        ru_utime;        // use microseconds_t here?
	duration_t        ru_stime;        // use microseconds_t here?
	uint32_t          *tag_name_ids;   // request tag names  (sequential in memory = scan speed)
	str_ref           *tag_values;     // request tag values (sequential in memory = scan speed) TODO: remove this ptr, address via tag_name_ids
	str_timer_t       *timers;
	timertag_bloom_t  *timer_bloom;    // poor man's bloom filter over timer[].tag_name_ids
};

// packet_t has been carefully crafted to avoid padding inside and eat as little memory as possible
// make sure we haven't made a mistake anywhere
static_assert(sizeof(str_packet_t) == 152, "make sure padding is under control, kinda");
static_assert(std::is_standard_layout<packet_t>::value == true, "packet_t must be a standard layout type");

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

template<class SinkT, class D>
inline void dump_packet(SinkT& sink, str_packet_t *packet, D const *d, struct nmpa_s *nmpa)
{
	auto const n_timer_tags = [&]()
	{
		uint32_t result = 0;
		for (unsigned i = 0; i < packet->timer_count; i++)
			result += packet->timers[i].tag_count;
		return result;
	}();

	ff::fmt(sink, "memory: {0}, {1}\n", nmpa_mem_used(nmpa), nmpa_user_space_used(nmpa));
	ff::fmt(sink, "p: {0}, p_sz: {1}, t_sz: {2}\n", packet, sizeof(*packet), sizeof(packet->timers[0]));
	ff::fmt(sink, "p: {0}, {1}, {2}, n_timers: {3}, n_tags: {4}, n_timer_tags: {5}\n",
		packet->host, packet->server, packet->script,
		packet->timer_count, packet->tag_count, n_timer_tags);

	for (uint32_t i = 0; i < packet->tag_count; i++)
	{
		ff::fmt(sink, "  [{0}]: {1}:{2} -> {3}\n",
			i,
			packet->tag_name_ids[i], d->get_word(packet->tag_name_ids[i]),
			packet->tag_values[i]);
	}

	for (unsigned i = 0; i < packet->timer_count; i++)
	{
		auto const& t = packet->timers[i];

		ff::fmt(sink, "  t[{0}]: {{ {1}, {2}, {3} }\n", i, t.value, t.ru_utime, t.ru_stime);
		for (unsigned j = 0; j < t.tag_count; j++)
		{
			auto const name_id = t.tag_name_ids[j];
			auto const value = t.tag_values[j];

			ff::fmt(sink, "    {0}:{1} -> {2}\n", name_id, d->get_word(name_id), value);
		}
		ff::fmt(sink, "\n");
	}
}

template<class D>
static str_packet_t* pinba_request_to_str_packet(Pinba__Request const *r, D *d, struct nmpa_s *nmpa)
{
	auto *p = (str_packet_t*)nmpa_calloc(nmpa, sizeof(str_packet_t)); // NOTE: no ctor is called here!

	auto const copy_string_to_nmpa = [](struct nmpa_s *nmpa, char const *str)
	{
		if (!str)
			return str_ref{};

		size_t const len = strlen(str);
		char *dst = (char *)nmpa_alloc(nmpa, len);
		if (!dst)
			return str_ref{};

		memcpy(dst, str, len);
		return str_ref { dst, len };
	};

	auto const copy_str_ref_to_nmpa = [](struct nmpa_s *nmpa, str_ref str)
	{
		char *dst = (char *)nmpa_alloc(nmpa, str.size());
		if (!dst)
			return str_ref{};

		memcpy(dst, str.data(), str.size());
		return str_ref { dst, str.size() };
	};

	p->host         = copy_str_ref_to_nmpa(nmpa, {(char const*)r->hostname.data, r->hostname.len});
	p->server       = copy_str_ref_to_nmpa(nmpa, {(char const*)r->server_name.data, r->server_name.len});
	p->script       = copy_str_ref_to_nmpa(nmpa, {(char const*)r->script_name.data, r->script_name.len});
	p->schema       = copy_str_ref_to_nmpa(nmpa, {(char const*)r->schema.data, r->schema.len});
	p->status       = copy_str_ref_to_nmpa(nmpa, meow::format::type_tunnel<uint32_t>::call(r->status));
	p->traffic      = r->document_size;
	p->mem_used     = r->memory_footprint;
	p->request_time = duration_from_float(r->request_time);
	p->ru_utime     = duration_from_float(r->ru_utime);
	p->ru_stime     = duration_from_float(r->ru_stime);

	// bloom should be somewhere close to the top
	p->timer_bloom = (timertag_bloom_t*)nmpa_calloc(nmpa, sizeof(timertag_bloom_t)); // NOTE: no ctor is called here!

	// timers
	p->timer_count = r->n_timer_value;
	if (p->timer_count > 0)
	{
		// basic info
		p->timers = (str_timer_t*)nmpa_alloc(nmpa, sizeof(str_timer_t) * r->n_timer_value);
		for_each_timer(r, [&](Pinba__Request const *r, timer_data_t const& timer)
		{
			str_timer_t *t = p->timers + timer.id;
			t->hit_count = timer.hit_count;
			t->value     = timer.value;
			t->ru_utime  = timer.ru_utime;
			t->ru_stime  = timer.ru_stime;
			t->tag_count = timer.tag_count;
		});

		// timer tags
		if (r->n_timer_tag_name > 0)
		{
			// copy tag names / values in one go, have them be contiguous in memory
			uint32_t *timer_tag_name_ids = (uint32_t*)nmpa_alloc(nmpa, sizeof(uint32_t) * r->n_timer_tag_name);
			str_ref *timer_tag_values = (str_ref*)nmpa_alloc(nmpa, sizeof(str_ref) * r->n_timer_tag_value);
			for (unsigned i = 0; i < r->n_timer_tag_name; i++)
			{
				// XXX: we transform tag names to ids without accounting for duplicates, might be able to fix
				auto const& tag_name_str = r->dictionary[r->timer_tag_name[i]];
				uint32_t const tag_name_id = d->get_or_add({(char const*)tag_name_str.data, tag_name_str.len});

				p->timer_bloom->add(tag_name_id);

				timer_tag_name_ids[i] = tag_name_id;

				auto const& tag_value_str = r->dictionary[r->timer_tag_value[i]];
				timer_tag_values[i] = copy_str_ref_to_nmpa(nmpa, {(char const*)tag_value_str.data, tag_value_str.len});
			}

			// fixup base pointers
			unsigned current_tag_offset = 0;
			for (unsigned i = 0; i < r->n_timer_value; i++)
			{
				p->timers[i].tag_name_ids = timer_tag_name_ids + current_tag_offset;
				p->timers[i].tag_values = timer_tag_values + current_tag_offset;
				current_tag_offset += r->timer_tag_count[i];
			}
		}
	}

	// request tags
	p->tag_count = r->n_tag_name;
	if (p->tag_count > 0)
	{
		p->tag_name_ids  = (uint32_t*)nmpa_alloc(nmpa, sizeof(uint32_t) * r->n_tag_name);
		p->tag_values = (str_ref*)nmpa_alloc(nmpa, sizeof(str_ref) * r->n_tag_name);
		for (unsigned i = 0; i < r->n_tag_name; i++)
		{
			auto const& tag_name_str = r->dictionary[r->timer_tag_name[i]];
			p->tag_name_ids[i]  = d->get_or_add({(char const*)tag_name_str.data, tag_name_str.len});

			auto const& tag_value_str = r->dictionary[r->tag_value[i]];
			p->tag_values[i] = copy_str_ref_to_nmpa(nmpa, {(char const*)tag_value_str.data, tag_value_str.len});
		}
	}

	return p;
}

// execute code for each timer in the request given
// can use inside
// r     - passed request
// i     - loop counter
// timer - timer_data_t structure
#define PINBA_PACKER___FOR_EACH_TIMER(r, code)																		\
do {																												\
	unsigned current_tag_offset = 0;																				\
	for (unsigned i = 0; i < r->n_timer_value; i++)																	\
	{																												\
		auto const tag_count = r->timer_tag_count[i];																\
		timer_data_t const timer = {																				\
			.id            = static_cast<uint16_t>(i),																\
			.tag_count     = static_cast<uint16_t>(tag_count),														\
			.hit_count     = r->timer_hit_count[i],																	\
			.value         = duration_from_float(r->timer_value[i]),												\
			.ru_utime      = (i < r->n_timer_ru_utime) ? duration_from_float(r->timer_ru_utime[i]) : duration_t{0}, \
			.ru_stime      = (i < r->n_timer_ru_stime) ? duration_from_float(r->timer_ru_stime[i]) : duration_t{0}, \
			.tag_name_ids  = meow::ref_array(&r->timer_tag_name[current_tag_offset], tag_count),					\
			.tag_value_ids = meow::ref_array(&r->timer_tag_value[current_tag_offset], tag_count),					\
		};																											\
		current_tag_offset += tag_count;																			\
																													\
		code;																										\
	}																												\
} while(0)																											\
/**/


inline str_ref pinba_request_status_to_str_ref(uint32_t status, meow::format::type_tunnel<uint32_t>::buffer_t const& buf = meow::format::type_tunnel<uint32_t>::buffer_t())
{
	switch (status)
	{
	#define CASE(x) case x: return meow::ref_lit(#x)
		CASE(0);   // empty
		CASE(100); // continue
		CASE(101); // switching protocols
		CASE(200); // ok
		CASE(300); // multiple choices
		CASE(301); // moved permanently
		CASE(302); // found
		CASE(304); // not modified
		CASE(400); // bad request
		CASE(401); // unauthorized
		CASE(403); // forbidden
		CASE(404); // not found
		CASE(408); // request timeout
		CASE(500); // server internal error
		CASE(502); // bad gateway
		CASE(503); // service unavailable
		CASE(504); // gateway timeout
	#undef CASE

		default:
			return meow::format::type_tunnel<uint32_t>::call(status, buf);
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

	// dictionary_t d;
	permanent_dictionary_t d;

	if (argc < 2)
		throw std::runtime_error(ff::fmt_str("usage {0} <filename>", argv[0]));

	FILE *f = fopen(argv[1], "r");
	if (NULL == f)
		throw std::runtime_error(ff::fmt_str("open failed: {0}:{1}", errno, strerror(errno)));

	uint8_t buf[64*1024] = {0};
	int const buf_sz = fread(buf, 1, sizeof(buf), f);

	ff::fmt(stdout, "got packet {0} bytes\n", buf_sz);

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

		ff::fmt(stdout, "decoded size {0} bytes, dict_size: {1}\n", nmpa_user_space_used(&local_nmpa), req->n_dictionary);

		return req;
	}(buf, buf_sz);

	// unpack + global dictionary repack + dump
	{
		Pinba__Request *request = pinba__request__unpack(&pba, buf_sz, buf);
		if (request == NULL) {
			throw std::runtime_error("packet decode failed\n");
		}

		dictionary_t g_dictionary;

		packet_t *packet = pinba_request_to_packet(request, &g_dictionary, &nmpa);

		debug_dump_packet(stdout, packet, &g_dictionary, &nmpa);
		nmpa_empty(&nmpa);
	}

	// just unpack + print
	{
		Pinba__Request *request = pinba__request__unpack(&pba, buf_sz, buf);
		if (request == NULL) {
			throw std::runtime_error("packet decode failed\n");
		}

		PINBA_PACKER___FOR_EACH_TIMER(request, {
			ff::fmt(stdout, " t[{0}]: {1}\n", i, timer.value);
		});

		str_packet_t *packet = pinba_request_to_str_packet(request, &d, &nmpa);

		dump_packet(stdout, packet, &d, &nmpa);
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
		ff::fmt(stdout, "{0}; {1} iterations, elapsed: {2}, {3} req/sec\n",
			name, n_iterations, elapsed, (double)n_iterations / timeval_to_double(elapsed));
	};

	auto const run_repack = [&](str_ref name, ProtobufCAllocator *pba)
	{
		dictionary_t g_dictionary;

		meow::stopwatch_t sw;

		for (size_t i = 0; i < n_iterations; i++)
		{
			// str_packet_t *packet = pinba_request_to_str_packet(req, &d, &nmpa);
			packet_t *packet = pinba_request_to_packet(req, &g_dictionary, &nmpa);
			// packet_t *packet = pinba_request_to_packet(req, &d, &nmpa);
			nmpa_empty(&nmpa);
		}

		auto const elapsed = sw.stamp();
		ff::fmt(stdout, "{0}; {1} iterations, elapsed: {2}, {3} req/sec\n",
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

			str_packet_t *packet = pinba_request_to_str_packet(request, &d, &nmpa);

			nmpa_empty(&nmpa);
		}

		auto const elapsed = sw.stamp();
		ff::fmt(stdout, "{0}; {1} iterations, elapsed: {2}, {3} req/sec\n",
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
	ff::fmt(stdout, "error: {0}\n", e.what());
	return 1;
}

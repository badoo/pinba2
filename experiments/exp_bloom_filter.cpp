#include <bitset>
#include <memory>
#include <utility>
#include <initializer_list>

#include <meow/stopwatch.hpp>
#include <meow/str_ref.hpp>
#include <meow/std_unique_ptr.hpp>
#include <meow/format/format_and_namespace.hpp>

#include "t1ha/t1ha.h"

struct number_hasher_t
{
	inline uint64_t operator()(uint32_t key) const
	{
		return t1ha0(&key, sizeof(key), key);
	}
};

using meow::str_ref;
constexpr size_t const bloom_size_bits = 64;

#ifndef rot32
static inline uint32_t rot32(uint32_t v, unsigned s) {
  return (v >> s) | (v << (32 - s));
}
#endif /* rot32 */

struct bloom_t : public std::bitset<bloom_size_bits>
{
};
using bloom_ptr = std::unique_ptr<bloom_t>;

void bloom_add(bloom_t& bloom, uint32_t value)
{
	// std::hash<uint32_t> hasher;
	number_hasher_t hasher;

	bloom.set(hasher(value) % bloom.size());
	bloom.set(hasher(value ^ rot32(value, 13)) % bloom.size());
	bloom.set(hasher(value ^ rot32(value, 25)) % bloom.size());
}

void bloom_add(bloom_t& bloom, std::initializer_list<uint32_t> values)
{
	for (auto const& v : values)
		bloom_add(bloom, v);
}

bool bloom_big_contains_little(bloom_t const& big, bloom_t const& little)
{
	// TODO: sse this
	return (big & little) == little;
}

bloom_ptr make_bloom()
{
	return meow::make_unique<bloom_t>();
}

bloom_ptr make_bloom(std::initializer_list<uint32_t> values)
{
	auto b = meow::make_unique<bloom_t>();
	bloom_add(*b, values);
	return std::move(b);
}

template<class T>
std::string as_string(std::initializer_list<T> values)
{
	std::string result;
	ff::write(result, "{");
	for (auto it = values.begin(); it != values.end(); it++)
		ff::write(result, (it != values.begin()) ? ", " : " ", *it);
	ff::write(result, " }");
	return result;
}

void do_test(str_ref name, std::initializer_list<uint32_t> big_values, std::initializer_list<uint32_t> little_values)
{
	bloom_ptr b_1 = make_bloom(big_values);
	bloom_ptr b_2 = make_bloom(little_values);
	ff::fmt(stdout, "{0}\n  big: {1}\n   {2}\n  lit: {3}\n   {4}\n>> contains: {5}\n\n"
		, name
		, as_string(big_values), b_1->to_string()
		, as_string(little_values), b_2->to_string()
		, bloom_big_contains_little(*b_1, *b_2));
}

int main(int argc, char const *argv[])
{
	do_test("simple", {0,1,2,3}, {0});
	do_test("simple", {0,1,2,3}, {4});
	do_test("simple", {0,1,2,3}, {7});
	do_test("simple", {0,1,2,3}, {31});
	do_test("simple", {0,1,2,3}, {456});

	{
		bloom_ptr b_1 = make_bloom({0,1,2,3});
		bloom_ptr b_2 = make_bloom();

		constexpr uint32_t const n_iterations = 1024 * 1024;
		uint32_t collisions = 0;

		meow::stopwatch_t sw;

		srandom(sw.now().tv_nsec);
		uint32_t const start = random();

		for (uint32_t i = start + 4; i < start + n_iterations; i++)
		{
			b_2->reset();
			bloom_add(*b_2, i);

			bool const contains = bloom_big_contains_little(*b_1, *b_2);
			if (contains)
				++collisions;
		}

		ff::fmt(stdout, "n_iterations: {0}, collisions: {1}, {2}, elapsed: {3}\n"
			, n_iterations, collisions, ff::as_printf("%1.6lf%%", double(collisions)/n_iterations * 100.0), sw.stamp());
	}

	return 0;
}
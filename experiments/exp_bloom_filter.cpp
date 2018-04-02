#include <bitset>
#include <memory>
#include <utility>
#include <initializer_list>

#include <meow/stopwatch.hpp>
#include <meow/str_ref.hpp>
#include <meow/std_unique_ptr.hpp>
#include <meow/format/format_and_namespace.hpp>

#include "pinba/bloom.h"

#include "t1ha/t1ha.h"

using meow::str_ref;

template<size_t N>
struct bloom_tester
{
	using bloom_t = pinba::fixlen_bloom_t<N>;
	using bloom_ptr = std::unique_ptr<bloom_t>;

	static void bloom_add(bloom_t& bloom, uint32_t value)
	{
		bloom.add(value);
	}

	static void bloom_add(bloom_t& bloom, std::initializer_list<uint32_t> values)
	{
		for (auto const& v : values)
			bloom_add(bloom, v);
	}

	static bool bloom_big_contains_little(bloom_t const& big, bloom_t const& little)
	{
		return big.contains(little);
	}

	static bloom_ptr make_bloom()
	{
		return meow::make_unique<bloom_t>();
	}

	static bloom_ptr make_bloom(std::initializer_list<uint32_t> values)
	{
		auto b = meow::make_unique<bloom_t>();
		bloom_add(*b, values);
		return std::move(b);
	}

	template<class T>
	static std::string as_string(std::initializer_list<T> values)
	{
		std::string result;
		ff::write(result, "{");
		for (auto it = values.begin(); it != values.end(); it++)
			ff::write(result, (it != values.begin()) ? ", " : " ", *it);
		ff::write(result, " }");
		return result;
	}

	static void run_test(str_ref name, std::initializer_list<uint32_t> big_values, std::initializer_list<uint32_t> little_values, bool should_pass)
	{
		bloom_ptr b_1 = make_bloom(big_values);
		bloom_ptr b_2 = make_bloom(little_values);

		bool const contains = bloom_big_contains_little(*b_1, *b_2);

		ff::fmt(stdout, "{0}[{1}]\n  big: {2}\n   {3}\n  lit: {4}\n   {5}\n>> contains: {6}\n\n"
			, name, N
			, as_string(big_values), b_1->to_string()
			, as_string(little_values), b_2->to_string()
			, contains);

		if (should_pass && !contains)
		{
			ff::fmt(stdout, "FAILED!");
			exit(1);
		}
	}

	static void run_perf_and_collisions(uint32_t n_big_values, uint32_t n_little_values)
	{
		srandom(os_unix::clock_monotonic_now().tv_nsec);

		bloom_ptr b_1 = make_bloom();

		for (uint32_t i = 0; i < n_big_values; i++)
			b_1->add(random());


		constexpr uint32_t const n_iterations = 1000 * 1000;
		uint32_t collisions = 0;

		srandom(os_unix::clock_monotonic_now().tv_nsec);

		meow::stopwatch_t sw;

		for (uint32_t i = 0; i < n_iterations; i++)
		{
			bloom_t b_2;
			for (uint32_t i = 0; i < n_little_values; i++)
				b_2.add(random());

			bool const contains = b_1->contains(b_2);
			if (contains)
				++collisions;
		}

		ff::fmt(stdout, "{0}[{1}, {2}]: n_iterations: {3}, collisions: {4}, {5}, elapsed: {6}\n"
			, N, n_big_values, n_little_values
			, n_iterations, collisions, ff::as_printf("%1.6lf%%", double(collisions)/n_iterations * 100.0), sw.stamp());
	}
};


void run_test(str_ref name, std::initializer_list<uint32_t> big_values, std::initializer_list<uint32_t> little_values, bool should_pass)
{
	bloom_tester<64>::run_test(name, big_values, little_values, should_pass);
	bloom_tester<128>::run_test(name, big_values, little_values, should_pass);
	bloom_tester<256>::run_test(name, big_values, little_values, should_pass);
}

template<size_t N>
void run_perf_and_collisions(uint32_t n_big_values, uint32_t n_little_values)
{
	for (uint32_t n_big = 1; n_big <= n_big_values; n_big++)
	{
		for (uint32_t n_little = 1; n_little <= n_little_values; n_little++)
		{
			bloom_tester<N>::run_perf_and_collisions(n_big, n_little);
		}
	}
}

int main(int argc, char const *argv[])
{
	run_test("simple", {0,1,2,3}, {0}, true);
	run_test("simple", {0,1,2,3}, {4}, false);
	run_test("simple", {0,1,2,3}, {7}, false);
	run_test("simple", {0,1,2,3}, {31}, false);
	run_test("simple", {0,1,2,3}, {456}, false);

	run_perf_and_collisions<64>(15, 4);
	run_perf_and_collisions<128>(15, 4);
	run_perf_and_collisions<256>(15, 4);

	return 0;
}
#include <thread>
#include <vector>
#include <string>

#include <tsl/robin_map.h>

#include <meow/format/format_and_namespace.hpp>
#include <meow/stopwatch.hpp>

#include "pinba/globals.h"
#include "pinba/dictionary.h"
#include "pinba/repacker_dictionary.h"

int main(int argc, char const *argv[])
{
	constexpr size_t n_threads    = 12;
	constexpr size_t n_repeats    = 20;
	constexpr size_t n_words      = 10 * 1024 * 1024;
	constexpr size_t n_iterations = 1024 * 1024;

	// generate words
	struct word_and_hash_t
	{
		std::string  word;
		uint64_t     hash_value;
	};

	auto const generate_new_word = [&]()
	{
		auto word = ff::fmt_str("{0}204uamf,am /,vqasdlknsad{0}--WSAS;KDNALF{1}", random(), random());
		uint64_t const hash = dictionary_word_hasher_t()(word);

		return word_and_hash_t { std::move(word), hash };
	};

	std::vector<word_and_hash_t> words;
	{
		ff::fmt(stdout, "generating {0} words\n", n_words);

		meow::stopwatch_t sw;

		words.reserve(n_words);

		for (size_t i = 0; i < n_words; i++)
		{
			words.emplace_back(generate_new_word());
		}

		ff::fmt(stdout, "words done, elapsed: {0}s\n", sw.stamp());
	}

	// tests
	auto const run_emplace_test = [&](str_ref name, auto& ht, size_t i_iter)
	{
		srandom(os_unix::gettimeofday_ex().tv_nsec);

		meow::stopwatch_t sw;

		for (size_t i = 0; i < n_iterations; i++)
		{
			ht.insert(words[random()%n_words], i);
		}

		ff::fmt(stdout, "[{0}/{1}] emplace test done, size: {2}, mem: {3}, elapsed: {4}s\n"
			, name, i_iter, ht.size(), ht.mem(), sw.stamp());
	};

	auto const run_find_test = [&](str_ref name, auto& ht, size_t i_iter)
	{
		srandom(os_unix::gettimeofday_ex().tv_nsec);

		meow::stopwatch_t sw;

		for (size_t i = 0; i < n_iterations; i++)
		{
			ht.find(words[random()%n_words]);
		}

		ff::fmt(stdout, "[{0}/{1}] find test done, {1} iterations, elapsed: {3}s\n", name, i_iter, n_iterations, sw.stamp());
	};

	auto const run_erase_and_replace = [&](str_ref name, auto& ht, size_t i_iter)
	{
		srandom(os_unix::gettimeofday_ex().tv_nsec);

		meow::stopwatch_t sw;

		for (size_t i = 0; i < n_iterations; i++)
		{
			size_t offset = random();
			offset %= n_words;

			ht.erase(words[offset]);
			words[offset] = generate_new_word();
		}

		// ff::fmt(stdout, "[robin/{0}] remove test done, size: {1}, elapsed: {2}s\n", i_iter, ht.size(), sw.stamp());
	};


	// robin_map
	// {
	// 	using base_t = tsl::robin_map<
	// 						  str_ref
	// 						, uint64_t
	// 						, dictionary_word_hasher_t
	// 						, std::equal_to<str_ref>
	// 						, std::allocator<std::pair<str_ref, uint64_t>>
	// 						, false>;

	// 	struct word_to_id_hash_t : public base_t
	// 	{
	// 		void insert(word_and_hash_t const& w, uint64_t value)
	// 		{
	// 			this->base_t::emplace(w.word, value);
	// 		}

	// 		void find(word_and_hash_t const& w)
	// 		{
	// 			this->base_t::find(w.word);
	// 		}

	// 		void erase(word_and_hash_t const& w)
	// 		{
	// 			this->base_t::erase(w.word);
	// 		}

	// 		size_t mem() const
	// 		{
	// 			return bucket_count() * sizeof(*begin());
	// 		}
	// 	};

	// 	word_to_id_hash_t ht;

	// 	for (size_t i_iter = 0; i_iter < n_repeats; i_iter++)
	// 	{
	// 		run_emplace_test("robin_nostore", ht, i_iter);
	// 		run_find_test("robin_nostore", ht, i_iter);
	// 		run_erase_and_replace("robin_nostore", ht, i_iter);
	// 	}
	// }

	// robin_map + storehash
	{
		using hashtable_t = tsl::robin_map<
							  str_ref
							, uint64_t
							, dictionary_word_hasher_t
							, std::equal_to<str_ref>
							, std::allocator<std::pair<str_ref, uint64_t>>
							, true>;

		struct word_to_id_hash_t
		{
			hashtable_t ht[128];

			word_to_id_hash_t()
			{
				for (auto& table : ht)
				{
					table.max_load_factor(0.4);
				}
			}

			hashtable_t& table_from_hash(uint64_t hash)
			{
				return ht[hash >> 57];
			}

			void insert(word_and_hash_t const& w, uint64_t value)
			{
				auto& table = table_from_hash(w.hash_value);

				if (table.will_grow_on_next_insert())
				{
					size_t buckets_before = table.bucket_count();
					meow::stopwatch_t sw;
					table.emplace_hash(w.hash_value, w.word, value);
					size_t buckets_after = table.bucket_count();

					if (buckets_after != buckets_before)
						ff::fmt(stdout, "rehash[{0}] {1} -> {2} took {3}s\n", (w.hash_value >> 57), buckets_before, buckets_after, sw.stamp());
				}
				else
				{
					table.emplace_hash(w.hash_value, w.word, value);
				}
			}

			void find(word_and_hash_t const& w)
			{
				table_from_hash(w.hash_value).find(w.word, w.hash_value);
			}

			void erase(word_and_hash_t const& w)
			{
				table_from_hash(w.hash_value).erase(w.word, w.hash_value);
			}

			size_t size() const
			{
				size_t result = 0;

				for (auto const& table : ht)
				{
					result += table.size();
				}

				return result;
			}

			size_t mem() const
			{
				size_t result = 0;

				for (auto const& table : ht)
				{
					result += table.bucket_count() * sizeof(*table.begin());
				}

				return result;
			}
		};

		word_to_id_hash_t ht;

		for (size_t i_iter = 0; i_iter < n_repeats; i_iter++)
		{
			run_emplace_test("robin_store_hash", ht, i_iter);
			run_find_test("robin_store_hash", ht, i_iter);
			run_erase_and_replace("robin_store_hash", ht, i_iter);
		}
	}

	// dense_hash_map
	// {
	// 	using base_t = google::dense_hash_map<str_ref, uint64_t, dictionary_word_hasher_t>;

	// 	struct word_to_id_hash_t : public base_t
	// 	{
	// 		word_to_id_hash_t()
	// 		{
	// 			this->set_empty_key(str_ref{nullptr,size_t{0}});
	// 			this->set_deleted_key(str_ref{nullptr,size_t{1}});
	// 		}

	// 		void insert(word_and_hash_t const& w, uint64_t value)
	// 		{
	// 			this->base_t::insert({ w.word, value });
	// 		}

	// 		void find(word_and_hash_t const& w)
	// 		{
	// 			this->base_t::find(w.word);
	// 		}

	// 		void erase(word_and_hash_t const& w)
	// 		{
	// 			this->base_t::erase(w.word);
	// 		}

	// 		size_t mem() const
	// 		{
	// 			return bucket_count() * sizeof(*begin());
	// 		}
	// 	};

	// 	word_to_id_hash_t ht;

	// 	for (size_t i_iter = 0; i_iter < n_repeats; i_iter++)
	// 	{
	// 		run_emplace_test("dense_hash", ht, i_iter);
	// 		run_find_test("dense_hash", ht, i_iter);
	// 		run_erase_and_replace("dense_hash", ht, i_iter);
	// 	}
	// }

	// unordered
	// {
	// 	using base_t = std::unordered_map<str_ref, uint64_t, dictionary_word_hasher_t>;

	// 	struct word_to_id_hash_t : public base_t
	// 	{
	// 		void insert(word_and_hash_t const& w, uint64_t value)
	// 		{
	// 			this->base_t::insert({ w.word, value });
	// 		}

	// 		void find(word_and_hash_t const& w)
	// 		{
	// 			this->base_t::find(w.word);
	// 		}

	// 		void erase(word_and_hash_t const& w)
	// 		{
	// 			this->base_t::erase(w.word);
	// 		}

	// 		size_t mem() const
	// 		{
	// 			return size() * sizeof(value_type) + bucket_count() * 2 * sizeof(void*);
	// 		}
	// 	};

	// 	word_to_id_hash_t ht;

	// 	for (size_t i_iter = 0; i_iter < n_repeats; i_iter++)
	// 	{
	// 		run_emplace_test("unordered", ht, i_iter);
	// 		run_find_test("unordered", ht, i_iter);
	// 		run_erase_and_replace("unordered", ht, i_iter);

	// 		// regenerate_words(i_iter);
	// 	}
	// }

	return 0;
}
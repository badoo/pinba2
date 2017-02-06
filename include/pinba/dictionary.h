#ifndef PINBA__DICTIONARY_H_
#define PINBA__DICTIONARY_H_

#include <string>
#include <vector>

#include <meow/str_ref.hpp>
#include <meow/hash/hash.hpp>
#include <meow/hash/hash_impl.hpp>

#include "pinba/globals.h"

////////////////////////////////////////////////////////////////////////////////////////////////
#if 0

#include <unordered_map>

struct dictionary_t
{
	typedef std::vector<std::string>                                   words_t;
	typedef std::unordered_map<str_ref, uint32_t, meow::hash<str_ref>> hash_t;

	words_t  words;
	hash_t   hash;

	uint64_t lookup_count;
	uint64_t insert_count;

	dictionary_t()
		: hash(64 * 1024)
		, lookup_count(0)
		, insert_count(0)
	{
	}

	str_ref get_word(uint32_t word_id)
	{
		if (word_id > words.size())
			return {};

		if (word_id == 0)
			return str_ref{};

		return words[word_id-1];
	}

	uint32_t get_or_add(str_ref const word)
	{
		if (!word)
			return 0;

		++this->lookup_count;
		auto const it = hash.find(word);

		if (hash.end() != it)
		{
			return it->second;
		}

		// insert new element
		words.push_back(word.str());

		assert(words.size() < size_t(INT_MAX));

		auto const word_id = static_cast<uint32_t>(words.size()); // start with 1, since 0 is reserved for empty

		++this->insert_count;
		hash.insert({words.back(), word_id});
		return word_id;
	}
};

#else // TODO: add configure/build support for densehash

#include <sparsehash/dense_hash_map>

struct dictionary_t
{
	typedef std::vector<std::string>                                   words_t;
	typedef google::dense_hash_map<str_ref, uint32_t, meow::hash<str_ref>> hash_t;

	words_t  words;
	hash_t   hash;

	uint64_t lookup_count;
	uint64_t insert_count;

	dictionary_t()
		: hash(64 * 1024)
		, lookup_count(0)
		, insert_count(0)
	{
		hash.set_empty_key(str_ref{});
	}

	str_ref get_word(uint32_t word_id) const
	{
		if (word_id == 0)
			return {};

		if (word_id > words.size())
			return {};

		return words[word_id-1];
	}

	uint32_t get_or_add(str_ref const word)
	{
		if (!word)
			return 0;

		++this->lookup_count;
		auto const it = hash.find(word);

		if (hash.end() != it)
		{
			return it->second;
		}

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
#endif
////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__DICTIONARY_H_
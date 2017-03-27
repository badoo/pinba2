#ifndef PINBA__DICTIONARY_H_
#define PINBA__DICTIONARY_H_

#include <string>
#include <deque>

#include <pthread.h> // need rwlock, which is only available since C++14

#include <sparsehash/dense_hash_map>

#include <meow/str_ref.hpp>
#include <meow/hash/hash.hpp>
#include <meow/hash/hash_impl.hpp>

#include "pinba/globals.h"
#include "t1ha/t1ha.h"

////////////////////////////////////////////////////////////////////////////////////////////////

struct rw_mutex_t
{
	rw_mutex_t()
	{
		pthread_rwlock_init(&mtx_, NULL);
	}

	~rw_mutex_t()
	{
		pthread_rwlock_destroy(&mtx_);
	}

	void rd_lock()
	{
		pthread_rwlock_rdlock(&mtx_);
	}

	void wr_lock()
	{
		pthread_rwlock_wrlock(&mtx_);
	}

	void unlock()
	{
		pthread_rwlock_unlock(&mtx_);
	}

private:
	pthread_rwlock_t mtx_;
};

struct scoped_read_lock_t
{
	rw_mutex_t *mtx_;

	scoped_read_lock_t(rw_mutex_t& mtx)
		: mtx_(&mtx)
	{
		mtx_->rd_lock();
	}

	~scoped_read_lock_t()
	{
		mtx_->unlock();
	}

	void upgrade_to_wrlock()
	{
		mtx_->unlock();
		mtx_->wr_lock();
	}
};

////////////////////////////////////////////////////////////////////////////////////////////////

struct dictionary_get_result_t
{
	uint32_t id;
	uint32_t found : 1;
};
static_assert(sizeof(uint64_t) == sizeof(dictionary_get_result_t), "no padding expected");

struct dictionary_word_hasher_t
{
	inline uint64_t operator()(str_ref const& key) const
	{
		return t1ha0(key.data(), key.size(), 0);
	}
};

struct dictionary_t
{
	using words_t = std::deque<std::string>; // deque to save a lil on push_back reallocs
	// using words_t = std::vector<std::string>; // deque to save a lil on push_back reallocs
	// using hash_t  = google::dense_hash_map<str_ref, uint32_t, meow::hash<str_ref>>;
	using hash_t  = google::dense_hash_map<str_ref, uint32_t, dictionary_word_hasher_t>;

	mutable rw_mutex_t mtx_;

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

	uint32_t size() const
	{
		scoped_read_lock_t lock_(mtx_);
		return words.size();
	}

	uint64_t memory_used() const
	{
		uint32_t n_buckets, sz;

		{
			scoped_read_lock_t lock_(mtx_);
			n_buckets = hash.bucket_count();
			// sz       = words.capacity(); // vector
			sz       = words.size(); // deque
		}

		uint64_t const words_mem_sz = sz * sizeof(words_t::value_type);
		uint64_t const hash_mem_sz  = n_buckets * sizeof(hash_t::value_type);

		return words_mem_sz + hash_mem_sz;
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

		// check again, as hash might've changed!
		++this->lookup_count;
		auto const it = hash.find(word);
		if (hash.end() != it)
			return it->second;

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

#endif // PINBA__DICTIONARY_H_
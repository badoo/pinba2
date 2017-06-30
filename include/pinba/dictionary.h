#ifndef PINBA__DICTIONARY_H_
#define PINBA__DICTIONARY_H_

#include <string>
#include <deque>

#include <pthread.h> // need rwlock, which is only available since C++14

#include <sparsehash/dense_hash_map>

#include <meow/intrusive_ptr.hpp>
#include <meow/str_ref.hpp>
#include <meow/hash/hash.hpp>
#include <meow/hash/hash_impl.hpp>

#include "t1ha/t1ha.h"

#include "pinba/globals.h"

////////////////////////////////////////////////////////////////////////////////////////////////

struct rw_mutex_t : private boost::noncopyable
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

struct scoped_read_lock_t : private boost::noncopyable
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

struct scoped_write_lock_t : private boost::noncopyable
{
	rw_mutex_t *mtx_;

	scoped_write_lock_t(rw_mutex_t& mtx)
		: mtx_(&mtx)
	{
		mtx_->wr_lock();
	}

	~scoped_write_lock_t()
	{
		mtx_->unlock();
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

struct dictionary_memory_t
{
	uint64_t hash_bytes;
	uint64_t list_bytes;
	uint64_t strings_bytes;
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

	uint64_t mem_used_by_word_strings;
	uint64_t lookup_count;
	uint64_t insert_count;

	dictionary_t()
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

	dictionary_memory_t memory_used() const
	{
		scoped_read_lock_t lock_(mtx_);

		return dictionary_memory_t {
			.hash_bytes    = hash.bucket_count() * sizeof(*hash.begin()),
			.list_bytes    = words.size() * sizeof(*words.begin()),
			.strings_bytes = mem_used_by_word_strings,
		};
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
// single threaded cache for dictionary_t
// transforms word_id -> word only
// intended to be used in snapshot scans, snapshot data never changes, so we can cache d->size()

struct snapshot_dictionary_t : private boost::noncopyable
{
	using words_t = std::vector<str_ref>;

	mutable words_t    words;
	dictionary_t const *d;
	uint32_t           d_words_size;

	explicit snapshot_dictionary_t(dictionary_t const *dict)
		: d(dict)
	{
		d_words_size = d->size();
		words.resize(d_words_size);
	}

	str_ref get_word(uint32_t word_id) const
	{
		if (word_id == 0)
			return {};

		if (word_id > d_words_size)
			return {};

		str_ref& word = words[word_id-1];

		if (!word) // cache miss
			word = d->get_word(word_id);

		return word;
	}
};

////////////////////////////////////////////////////////////////////////////////////////////////
// single threaded cache for dictionary_t to be used by repacker
// get_or_add only, i.e. str_ref -> uint32_t

struct repacker_dictionary_t : private boost::noncopyable
{
	struct word_id_hasher_t
	{
		inline uint64_t operator()(uint32_t const key) const
		{
			return t1ha0(&key, sizeof(key), key);
		}
	};

	struct word_t : public boost::intrusive_ref_counter<word_t> // TODO: can use non-atomic counter here
	{
		uint32_t const id;
		str_ref  const str;

		word_t(uint32_t i, str_ref s)
			: id(i)
			, str(s)
		{
		}
	};
	using word_ptr = boost::intrusive_ptr<word_t>;

	struct timeslice_t : public meow::ref_counted_t
	{
		struct hash_t : public google::dense_hash_map<uint32_t, word_ptr, word_id_hasher_t>
		{
			hash_t()
			{
				this->set_empty_key(PINBA_INTERNAL___UINT32_MAX);
			}
		};

		hash_t ht;
	};
	using timeslice_ptr = boost::intrusive_ptr<timeslice_t>;

	using word_to_id_hash_t = google::dense_hash_map<str_ref, word_ptr, dictionary_word_hasher_t>;

public: // cba

	dictionary_t       *d;

	word_to_id_hash_t          word_to_id;

	std::deque<timeslice_ptr>  slices;
	timeslice_ptr              curr_slice;

public:

	repacker_dictionary_t(dictionary_t *dict)
		: d(dict)
	{
		word_to_id.set_empty_key(str_ref{});
	}

	uint32_t get_or_add(str_ref const word)
	{
		if (!word)
			return 0;

		// fastpath - local lookup
		auto const it = word_to_id.find(word);
		if (word_to_id.end() != it)
		{
			this->add_to_current_timeslice(it->second.get());
			return it->second->id;
		}

		// cache miss - slowpath
		// can't store `word' in our local word_to_id directly, since it's supposed to be freed quickly by the calling code
		// so dictionary copies the string, and we store str_ref to that copy here
		str_ref real_string = {};
		uint32_t const word_id = d->get_or_add_pessimistic(word, &real_string);

		word_ptr w = meow::make_intrusive<word_t>(word_id, real_string);
		this->add_to_current_timeslice(w.get());

		word_to_id.insert({real_string, w});
		return word_id;
	}

	void add_to_current_timeslice(word_t *w)
	{
		if (!curr_slice)
			curr_slice = meow::make_intrusive<timeslice_t>();

		word_ptr& ts_word = curr_slice->ht[w->id];
		if (!ts_word)
		{
			ts_word.reset(w); // increments refcount here
		}
	}

	void start_new_timeslice()
	{
		auto const ts = meow::make_intrusive<timeslice_t>();
		slices.push_back(ts);
		curr_slice = ts;
	}

	void detach_timeslice(timeslice_ptr ts)
	{
		auto const it = [this, &ts]()
		{
			for (auto it = slices.begin(); it != slices.end(); ++it)
			{
				if (*it == ts)
					return it;
			}

			assert(!"detach_timeslice: must have found the ts");
		}();

		// remove timeslice early, we've got a ref to it in `ts` anyway
		slices.erase(it);

		// now reduce refcounts to all words in there
		for (auto& ts_pair : ts->ht)
		{
			word_t *w = ts_pair.second.get();

			assert(w->use_count() >= 2); // at least `this->word_to_id` and `ts_item` must reference the word
			ts_pair.second.reset();

			if (w->use_count() == 1) // only `this->word_to_id' references the word, should erase
			{
				size_t const erased_count = word_to_id.erase(w->str);
				assert(erased_count == 1);

				// w is invalid here

				// TODO: tell global dictionary that we've erased the word
				//       are we immune to ABA problem here (since word ids are reused by global dictionary!)?
			}
		}
	}
};

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__DICTIONARY_H_

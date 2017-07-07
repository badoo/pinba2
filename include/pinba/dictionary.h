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
	uint64_t wordlist_bytes;
	uint64_t freelist_bytes;
	uint64_t strings_bytes;
};

struct dictionary_t : private boost::noncopyable
{
	struct word_t : private boost::noncopyable
	{
		uint32_t    refcount;
		uint32_t    id;
		std::string str;

		word_t(uint32_t i, str_ref s)
			: refcount(0)
			, id(i)
			, str(s.str())
		{
		}

		void clear()
		{
			refcount = 0;
			id       = 0;
			str      = "";
		}
	};
	static_assert((sizeof(word_t) == (2*sizeof(uint32_t) + sizeof(std::string))), "word_t should have no padding");

	// kind of a hack of accomodate storing special str_refs as deleted/empty keys
	struct hash_str_ref_equal_t
	{
		bool operator()(str_ref l, str_ref r) const // TODO: make this one constexpr
		{
			if ((l.size() == r.size()) && (l.size() == 0))
				return l.data() == r.data();
			return l == r;
		}
	};

	// word -> pointer to elt in `words` array
	using hashtable_t = google::dense_hash_map<str_ref, word_t*, dictionary_word_hasher_t, hash_str_ref_equal_t>;

	struct hash_t : public hashtable_t
	{
		hash_t()
			: hashtable_t(64 * 1024)
		{
			this->set_empty_key(str_ref{});
			this->set_deleted_key(str_ref{(char*)0x1, size_t{0}});
		}
	};

	// id -> word_t,
	// deque is critical here, since `hash` stores pointers to elements,
	// appends must not invalidate them
	struct words_t : public std::deque<word_t>
	{
	};

private:

	mutable rw_mutex_t mtx_;

	hash_t                hash;
	words_t               words;
	std::deque<uint32_t>  freelist;

	uint64_t mem_used_by_word_strings = 0;

public:

	uint32_t size() const
	{
		scoped_read_lock_t lock_(mtx_);
		return words.size();
	}

	dictionary_memory_t memory_used() const
	{
		scoped_read_lock_t lock_(mtx_);

		return dictionary_memory_t {
			.hash_bytes     = hash.bucket_count() * sizeof(*hash.begin()),
			.wordlist_bytes = words.size() * sizeof(*words.begin()),
			.freelist_bytes = freelist.size() * sizeof(*freelist.begin()),
			.strings_bytes  = mem_used_by_word_strings,
		};
	}

public:

	// get transient work, caller must make sure it stays valid while using
	str_ref get_word___noref(uint32_t word_id) const
	{
		if (word_id == 0)
			return {};

		// can use only READ lock, here, since word refcount is not incremented
		scoped_read_lock_t lock_(mtx_);

		assert((word_id <= words.size()) && "word_id > wordlist.size(), bad word_id reference");

		word_t const *w = &words[word_id - 1];
		assert(!w->str.empty() && "got empty word ptr from wordlist, dangling word_id reference");

		return str_ref { w->str }; // TODO: optimize pointer deref here (string::size(), etc.)
	}

	void erase_word___ref(uint32_t word_id) // pair to get_or_add___ref()
	{
		if (word_id == 0)
			return; // allow for some leeway

		// TODO: not worth using rlock just for quick assert that should never fire
		//       but might be worth using it for refcount check (in case it's atomic) and upgrade only after
		scoped_write_lock_t lock_(mtx_);

		assert((word_id <= words.size()) && "word_id > wordlist.size(), bad word_id reference");

		uint32_t const word_offset = word_id - 1;

		word_t *w = &words[word_offset];
		assert(!w->str.empty() && "got empty word ptr from wordlist, dangling word_id reference");

		if (0 == --w->refcount)
		{
			size_t const n_erased = hash.erase(str_ref { w->str });
			assert((n_erased == 1) && "must have erased something here");

			freelist.push_back(word_offset);
			w->clear();
		}
	}

	// get or add a word that might get removed with erase_word___ref() later
	word_t const* get_or_add___permanent(str_ref const word)
	{
		if (!word)
			return {};

		// fastpath
		scoped_read_lock_t lock_(mtx_);
		{
			auto it = hash.find(word);
			if (hash.end() != it)
			{
				// no need to increment here, refcount should be >= 2 already
				// and caller MUST NOT try erasing this word without getting it ref'd
				return it->second;
			}
		}

		// slowpath
		lock_.upgrade_to_wrlock();

		word_t *w = this->get_or_add___wrlocked(word);
		w->refcount += 2;

		return w;
	}

	// compatibility wrapper, allows using dictionary_t and repacker_dictionary_t without changing code much
	uint32_t get_or_add(str_ref const word)
	{
		return this->get_or_add___permanent(word)->id;
	}

	// get or add a word that is never supposed to be removed
	word_t const* get_or_add___ref(str_ref const word)
	{
		if (!word)
			return {};

		scoped_write_lock_t lock_(mtx_);

		word_t *w = this->get_or_add___wrlocked(word);
		w->refcount += 1;

		return w;
	}

private:

	// get or create a word, REFCOUNT IS NOT MODIFIED, i.e. even if just created -> refcount == 0
	word_t* get_or_add___wrlocked(str_ref const word)
	{
		// re-check word existence, might've appeated while upgrading the lock
		auto it = hash.find(word);
		if (hash.end() != it)
			return it->second;

		// need to insert, going to be really slow, mon

		mem_used_by_word_strings += word.size();

		// word_id starts with 1, since 0 is reserved for empty
		assert((words.size() + 1) < size_t(INT_MAX));
		auto const word_id = static_cast<uint32_t>(words.size() + 1);

		// insert new element
		words.emplace_back(word_id, word);

		// ptr to constructed word, with word->str that's going to be alive for long
		word_t *w = &words.back();

		hash.insert({ str_ref { w->str }, w });

		return w;
	}
};

////////////////////////////////////////////////////////////////////////////////////////////////
// single threaded cache for dictionary_t
// transforms word_id -> word only
// intended to be used in snapshot scans, snapshot data never changes, so we can cache d->size()

// struct snapshot_dictionary_t : private boost::noncopyable
// {
// 	using words_t = std::vector<str_ref>;

// 	mutable words_t    words;
// 	dictionary_t const *d;
// 	uint32_t           d_words_size;

// 	explicit snapshot_dictionary_t(dictionary_t const *dict)
// 		: d(dict)
// 	{
// 		d_words_size = d->size();
// 		words.resize(d_words_size);
// 	}

// 	str_ref get_word(uint32_t word_id) const
// 	{
// 		if (word_id == 0)
// 			return {};

// 		if (word_id > d_words_size)
// 			return {};

// 		str_ref& word = words[word_id-1];

// 		if (!word) // cache miss
// 			word = d->get_word(word_id);

// 		return word;
// 	}
// };

////////////////////////////////////////////////////////////////////////////////////////////////
// single threaded cache for dictionary_t to be used by repacker
// get_or_add only, i.e. str_ref -> uint32_t

struct repacker_dictionary_t : private boost::noncopyable
{
	struct word_t : public boost::intrusive_ref_counter<word_t> // TODO: can use non-atomic counter here?
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

	struct wordslice_t : public meow::ref_counted_t
	{
		struct word_id_hasher_t
		{
			inline uint64_t operator()(uint32_t const key) const
			{
				return t1ha0(&key, sizeof(key), key);
			}
		};

		using hashtable_t = google::dense_hash_map<uint32_t, word_ptr, word_id_hasher_t>;

		struct hash_t : public hashtable_t
		{
			hash_t()
				: hashtable_t(64 * 1024) // XXX: reasonable default for a timed slice, i guess?
			{
				this->set_empty_key(PINBA_INTERNAL___UINT32_MAX);
			}
		};

		repacker_dictionary_t *const d;
		hash_t                       ht;

		explicit wordslice_t(repacker_dictionary_t *dict)
			: d(dict)
		{
		}
	};
	using wordslice_ptr = boost::intrusive_ptr<wordslice_t>;

	// kind of a hack of accomodate storing special str_refs as deleted/empty keys
	struct word_hash_equal_t
	{
		bool operator()(str_ref l, str_ref r) const
		{
			if ((l.size() == r.size()) && (l.size() == 0))
				return l.data() == r.data();
			return l == r;
		}
	};

	struct word_to_id_hash_t : public google::dense_hash_map<str_ref, word_ptr, dictionary_word_hasher_t, word_hash_equal_t>
	{
		word_to_id_hash_t()
		{
			this->set_empty_key(str_ref{});
			this->set_deleted_key(str_ref{(char*)0x1, size_t{0}});
		}
	};

public: // cba

	dictionary_t       *d;

	word_to_id_hash_t          word_to_id;

	std::deque<wordslice_ptr>  slices;
	wordslice_ptr              curr_slice;

public:

	repacker_dictionary_t(dictionary_t *dict)
		: d(dict)
	{
	}

	uint32_t get_or_add(str_ref const word)
	{
		if (!word)
			return 0;

		// fastpath - local lookup
		auto const it = word_to_id.find(word);
		if (word_to_id.end() != it)
		{
			this->add_to_current_wordslice(it->second.get());
			return it->second->id;
		}

		// cache miss - slowpath
		word_ptr w = [&]() {
			dictionary_t::word_t const *dict_word = d->get_or_add___ref(word);
			return meow::make_intrusive<word_t>(dict_word->id, str_ref { dict_word->str });
		}();

		word_to_id.insert({ str_ref { w->str }, w});
		this->add_to_current_wordslice(w.get());

		return w->id;
	}

	void add_to_current_wordslice(word_t *w)
	{
		if (!curr_slice)
			curr_slice = meow::make_intrusive<wordslice_t>(this);

		word_ptr& ts_word = curr_slice->ht[w->id];
		if (!ts_word)
		{
			ts_word.reset(w); // increments refcount here
		}
	}

	wordslice_t* current_wordslice()
	{
		if (!curr_slice)
			curr_slice = meow::make_intrusive<wordslice_t>(this);

		return curr_slice.get();
	}

	void start_new_wordslice()
	{
		auto const ts = meow::make_intrusive<wordslice_t>(this);

		if (curr_slice)
			slices.push_back(curr_slice);

		curr_slice = ts;
	}

	void release_wordslice(wordslice_t *ts)
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

using repacker_dslice_t   = repacker_dictionary_t::wordslice_t;
using repacker_dslice_ptr = repacker_dictionary_t::wordslice_ptr;

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__DICTIONARY_H_

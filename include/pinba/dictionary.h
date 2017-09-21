#ifndef PINBA__DICTIONARY_H_
#define PINBA__DICTIONARY_H_

#include <array>
#include <string>
#include <deque>
#include <unordered_map>

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
/*
// TODO: rebuild sharding with this struct, instead of handmade bit-fiddling
	union word_id_t
	{
		struct {
			uint32_t shard_id : 4;
			uint32_t word_id  : 28;
		};

		uint32_t value;

		word_id_t(uint32_t v)
			: value(v)
		{
		}
	};
	static_assert(sizeof(word_id_t) == sizeof(uint32_t), "no padding expected");
*/

	static constexpr uint32_t const shard_count   = 32;
	static constexpr uint32_t const shard_id_bits = 5;          // number of bits in mask below
	static constexpr uint32_t const shard_id_mask = 0xF8000000; // shard_id = top bits
	static constexpr uint32_t const word_id_mask  = 0x07FFFFFF; // word_id  = lower bits

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

		void set(uint32_t i, str_ref s)
		{
			id = i;
			str = s.str();
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

	// unordered_map handles intense delete-s better than dense hash
	struct hash_with_ok_erase_t : public std::unordered_map<str_ref, word_t*, dictionary_word_hasher_t>
	{
	};

	// id -> word_t,
	// deque is critical here, since `hash` stores pointers to elements,
	// appends must not invalidate them
	struct words_t : public std::deque<word_t>
	{
	};

private:

	struct shard_t
	{
		mutable rw_mutex_t    mtx;

		uint32_t              id;
		hash_t                hash;
		words_t               words;
		std::deque<uint32_t>  freelist;

		uint64_t              mem_used_by_word_strings;
	};

	mutable std::array<shard_t, shard_count> shards_;

public:

	dictionary_t()
	{
		for (uint32_t i = 0; i < shard_count; ++i)
			shards_[i].id = i;
	}

	uint32_t size() const
	{
		uint32_t result = 0;

		for (auto const& shard : shards_)
		{
			scoped_read_lock_t lock_(shard.mtx);
			result += shard.words.size();
		}
		return result;
	}

	dictionary_memory_t memory_used() const
	{
		dictionary_memory_t result = {};

		for (auto const& shard : shards_)
		{
			scoped_read_lock_t lock_(shard.mtx);

			result.hash_bytes     += shard.hash.bucket_count() * sizeof(*shard.hash.begin());
			result.wordlist_bytes += shard.words.size() * sizeof(*shard.words.begin());
			result.freelist_bytes += shard.freelist.size() * sizeof(*shard.freelist.begin());
			result.strings_bytes  += shard.mem_used_by_word_strings;
		}

		return result;
	}

public:

	// get transient work, caller must make sure it stays valid while using
	str_ref get_word(uint32_t word_id) const
	{
		if (word_id == 0)
			return {};

		shard_t const *shard   = get_shard_for_word_id(word_id);
		uint32_t const word_offset = (word_id & word_id_mask) - 1;

		// can use only READ lock, here, since word refcount is not incremented
		scoped_read_lock_t lock_(shard->mtx);

		assert((word_offset < shard->words.size()) && "word_offset >= wordlist.size(), bad word_id reference");

		word_t const *w = &shard->words[word_offset];
		assert((w && !w->str.empty()) && "got empty word ptr from wordlist, dangling word_id reference");

		return str_ref { w->str }; // TODO: optimize pointer deref here (string::size(), etc.)
	}

	void erase_word___ref(uint32_t word_id) // pair to get_or_add___ref()
	{
		if (word_id == 0)
			return; // allow for some leeway

		shard_t *shard = get_shard_for_word_id(word_id);
		uint32_t const word_offset = (word_id & word_id_mask) - 1;

		// TODO: not worth using rlock just for quick assert that should never fire
		//       but might be worth using it for refcount check (in case it's atomic) and upgrade only after
		scoped_write_lock_t lock_(shard->mtx);

		assert((word_offset < shard->words.size()) && "word_offset >= wordlist.size(), bad word_id reference");

		word_t *w = &shard->words[word_offset];
		assert(w->id == word_id);
		assert(!w->str.empty() && "got empty word ptr from wordlist, dangling word_id reference");

		// LOG_DEBUG(PINBA_LOOGGER_, "{0}; erasing {1} {2} {3}", __func__, w->str, w->id, w->refcount);

		if (0 == --w->refcount)
		{
			size_t const n_erased = shard->hash.erase(str_ref { w->str });
			assert((n_erased == 1) && "must have erased something here");

			shard->mem_used_by_word_strings -= w->str.size();

			shard->freelist.push_back(word_id);
			w->clear();
		}
	}

	// get or add a word that might get removed with erase_word___ref() later
	word_t const* get_or_add___permanent(str_ref const word)
	{
		if (!word)
			return {};

		shard_t *shard = get_shard_for_word(word);

		// fastpath
		scoped_read_lock_t lock_(shard->mtx);
		{
			auto it = shard->hash.find(word);
			if (shard->hash.end() != it)
			{
				// no need to increment here, refcount should be >= 2 already
				// and caller MUST NOT try erasing this word without getting it ref'd
				return it->second;
			}
		}

		// slowpath
		lock_.upgrade_to_wrlock();

		word_t *w = this->get_or_add___wrlocked(shard, word);
		w->refcount += 2;

		return w;
	}

	// compatibility wrapper, allows using dictionary_t and repacker_dictionary_t without changing code much
	uint32_t get_or_add(str_ref const word)
	{
		if (!word)
			return 0;

		return this->get_or_add___permanent(word)->id;
	}

	// get or add a word that is never supposed to be removed
	word_t const* get_or_add___ref(str_ref const word)
	{
		if (!word)
			return {};

		// TODO: not sure how to build fastpath here, since we need to increment refcount on return

		shard_t *shard = get_shard_for_word(word);

		scoped_write_lock_t lock_(shard->mtx);

		word_t *w = this->get_or_add___wrlocked(shard, word);
		w->refcount += 1;

		return w;
	}

private:

	shard_t* get_shard_for_word_id(uint32_t word_id) const
	{
		return &shards_[(word_id & shard_id_mask) >> (32 - shard_id_bits)];
	}

	shard_t* get_shard_for_word(str_ref word) const
	{
		uint64_t const hash_value = dictionary_word_hasher_t()(word);
		return &shards_[hash_value % shard_count];
	}

	// get or create a word, REFCOUNT IS NOT MODIFIED, i.e. even if just created -> refcount == 0
	word_t* get_or_add___wrlocked(shard_t *shard, str_ref const word)
	{
		// re-check word existence, might've appeated while upgrading the lock
		auto it = shard->hash.find(word);
		if (shard->hash.end() != it)
			return it->second;

		// need to insert, going to be really slow, mon

		shard->mem_used_by_word_strings += word.size();

		word_t *w = [&]()
		{
			if (!shard->freelist.empty())
			{
				uint32_t const word_id = shard->freelist.back();
				shard->freelist.pop_back();

				// remember to subtract 1 from word_id, mon
				uint32_t const word_offset = (word_id & word_id_mask) - 1;
				word_t *w = &shard->words[word_offset];

				assert((w->id == 0) && "word must be empty, while present in freelist");
				assert((w->str.empty()) && "word must be empty, while present in freelist");

				w->set(word_id, word);
				return w;
			}
			else
			{
				// word_id starts with 1, since 0 is reserved for empty
				assert(((shard->words.size() + 1) & word_id_mask) != 0);
				uint32_t const word_id = static_cast<uint32_t>(shard->words.size() + 1) | (shard->id << (32 - shard_id_bits));

				shard->words.emplace_back(word_id, word);
				return &shard->words.back();

			}
		}();

		shard->hash.insert({ str_ref { w->str }, w });

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
			PINBA_STATS_(objects).n_repacker_dict_words++;
		}

		~word_t()
		{
			PINBA_STATS_(objects).n_repacker_dict_words--;
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
				// setting large-ish default size eats way too much memory (megabytes per slice)
				// : hashtable_t(64 * 1024) // XXX: reasonable default for a timed slice, i guess?
			{
				this->set_empty_key(PINBA_INTERNAL___UINT32_MAX);
			}
		};

		hash_t ht;

		explicit wordslice_t()
		{
			PINBA_STATS_(objects).n_repacker_dict_ws++;
		}

		~wordslice_t()
		{
			PINBA_STATS_(objects).n_repacker_dict_ws--;
		}
	};
	using wordslice_ptr = boost::intrusive_ptr<wordslice_t>;

	// kind of a hack of accomodate storing special str_refs as deleted/empty keys
	// struct word_hash_equal_t
	// {
	// 	bool operator()(str_ref l, str_ref r) const
	// 	{
	// 		if ((l.size() == r.size()) && (l.size() == 0))
	// 			return l.data() == r.data();
	// 		return l == r;
	// 	}
	// };

	// struct word_to_id_hash_t : public google::dense_hash_map<str_ref, word_ptr, dictionary_word_hasher_t, word_hash_equal_t>
	// {
	// 	word_to_id_hash_t()
	// 	{
	// 		this->set_empty_key(str_ref{});
	// 		this->set_deleted_key(str_ref{(char*)0x1, size_t{0}});
	// 	}
	// };

	struct word_to_id_hash_t : public std::unordered_map<str_ref, word_ptr, dictionary_word_hasher_t>
	{
	};

public: // FIXME

	dictionary_t               *d;

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

		// TODO: can optimize this, by storing a `is this word already in current wordslice' bit in `word'
		//       will need to reset those bits on current wordslice change
		//       basically trading extra hash lookup on every call for hash scan every wordslice change

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

	wordslice_ptr current_wordslice()
	{
		if (!curr_slice)
			curr_slice = meow::make_intrusive<wordslice_t>();

		return curr_slice;
	}

	void start_new_wordslice()
	{
		if (!curr_slice)
		{
			curr_slice = meow::make_intrusive<wordslice_t>();
			return;
		}

		if (!curr_slice->ht.empty())
		{
			slices.emplace_back(std::move(curr_slice));
			curr_slice = meow::make_intrusive<wordslice_t>();
		}
	}

	struct reap_stats_t
	{
		uint64_t reaped_slices;
		uint64_t reaped_words_local;
		uint64_t reaped_words_global;
	};

	// reaps all wordslices that are only referenced from this object
	reap_stats_t reap_unused_wordslices()
	{
		reap_stats_t result = {};

		// move all wordslices that are only referenced from `slices' to the end of the range
		auto const erased_begin = std::partition(slices.begin(), slices.end(), [](wordslice_ptr& ws)
		{
			// LOG_DEBUG(PINBA_LOOGGER_, "{0}; ws: {1}, uc: {2}", __func__, ws.get(), ws->use_count());
			assert(ws->use_count() >= 1); // sanity
			return (ws->use_count() != 1);
		});

		// fastpath exit if nothing to do
		if (erased_begin == slices.end())
			return result;

		// gather the list of words to erase from upstream dictionary
		auto const words_to_erase = [&]() -> std::vector<word_ptr>
		{
			std::vector<word_ptr> words_to_erase;

			for (auto it = erased_begin; it != slices.end(); ++it)
			{
				wordslice_ptr& ws = *it;

				result.reaped_slices      +=1;
				result.reaped_words_local += ws->ht.size();

				// reduce refcounts to all words in this slice
				for (auto& ws_pair : ws->ht)
				{
					// this class is single-threaded, so noone is modifying refcount while we're on it
					word_ptr& w = ws_pair.second;
					assert(w->use_count() >= 2); // at least `this->word_to_id` and `ws_pair.second` must reference the word

					// LOG_DEBUG(PINBA_LOOGGER_, "{0}; '{1}' {2} {3}", __func__, w->str, w->id, w->use_count());

					if (w->use_count() == 2)     // can erase, since ONLY `this->word_to_id` and `ws_pair.second` reference the word
					{
						result.reaped_words_global += 1;

						// LOG_DEBUG(PINBA_LOOGGER_, "reap_unused_wordslices; erase global '{0}' {1} {2}", w->str, w->id, w->use_count());

						// erase from local hash
						size_t const erased_count = word_to_id.erase(w->str);
						assert(erased_count == 1);

						// try moving, to avoid jerking refcount back-n-forth
						assert(w->use_count() == 1);
						words_to_erase.emplace_back(std::move(w));
					}
					else
					{
						word_t *wptr = w.get();          // save
						w.reset();                       // just deref as usual, if no delete is needed
						assert(wptr->use_count() >= 2);  // some other wordslice must still be holding a ref
					}
				}
			}

			return words_to_erase;
		}();

		// remember to erase wordslice, since we don't need them anymore
		slices.erase(erased_begin, slices.end());

		// TODO: code from here and on does NOT depend on `this' anymore,
		//       so it can be run in separate thread to reduce blocking

		// now deref in upstream dictionary
		for (auto const& w : words_to_erase)
		{
			// tell global dictionary that we've erased the word
			// XXX: are we immune to ABA problem here (since word ids are reused by global dictionary!)?
			//      should be, since `word_id`s are unique in this hash
			d->erase_word___ref(w->id);
		}

		// here `words_to_erase' is destroyed and all word_ptr's are released, freeing memory

		return result;
	}

private:

	void add_to_current_wordslice(word_t *w)
	{
		if (!curr_slice)
			curr_slice = meow::make_intrusive<wordslice_t>();

		word_ptr& ts_word = curr_slice->ht[w->id];
		if (!ts_word)
		{
			ts_word.reset(w); // increments refcount here
		}
	}
};

using repacker_dslice_t   = repacker_dictionary_t::wordslice_t;
using repacker_dslice_ptr = repacker_dictionary_t::wordslice_ptr;

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__DICTIONARY_H_

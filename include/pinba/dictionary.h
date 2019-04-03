#ifndef PINBA__DICTIONARY_H_
#define PINBA__DICTIONARY_H_

#include <array>
#include <string>
#include <deque>
#include <unordered_map>

#include <pthread.h>

#include <sparsehash/dense_hash_map>

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

	// unordered_map handles intense MUCH delete-s better than dense hash (but significantly slower on lookups)
	struct hash_with_ok_erase_t : public std::unordered_map<str_ref, word_t*, dictionary_word_hasher_t>
	{
		hash_with_ok_erase_t()
		{
			// try speed up lookups by having a sparser map
			this->max_load_factor(0.3);
		}
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
		// hash_with_ok_erase_t  hash;
		words_t               words;
		std::deque<uint32_t>  freelist;

		uint64_t              mem_used_by_word_strings = 0;
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

	// get transient word, caller must make sure it stays valid while using
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

	// get or add a word that is never supposed to be removed
	word_t const* get_or_add___permanent(str_ref const word)
	{
		if (!word)
			return {};

		shard_t *shard = get_shard_for_word(word);

		// MUST make work permanent here (aka increment refcount) -> no fastpath
		// as word might've been non-permanent (word from traffic before report creation for example)
		//
		// also if word already exists as permanent, we still increment refcount by 2
		// this is not an issue, since permanent words are not to be removed anyway (any refcount would work)

		scoped_write_lock_t lock_(shard->mtx);

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

	// get or add a word that might get removed with erase_word___ref() later
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
		// can't just insert here, since the key is str_ref that should refer to word (created below)
		//  and not the incoming str_ref (that might be transient and just become invalid after this func returns)
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

		// XXX(antoxa): if this throws -> the word created above is basically 'leaked'
		shard->hash.insert({ str_ref { w->str }, w });

		return w;
	}
};

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__DICTIONARY_H_

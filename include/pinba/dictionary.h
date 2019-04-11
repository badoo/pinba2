#ifndef PINBA__DICTIONARY_H_
#define PINBA__DICTIONARY_H_

#include <array>
#include <string>
#include <deque>
#include <unordered_map>

#include <pthread.h>

#include <tsl/robin_map.h>

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
		uint64_t    hash;
		std::string str;

		word_t(uint32_t i, str_ref s, uint64_t h)
			: refcount(0)
			, id(i)
			, hash(h)
			, str(s.str())
		{
		}

		word_t(word_t&& other) noexcept
		{
			(*this) = std::move(other); // call move operator=()
		}

		word_t& operator=(word_t&& other) noexcept
		{
			refcount = other.refcount;
			id       = other.id;
			hash     = other.hash;
			str      = std::move(other.str);

			return *this;
		}

		void set(uint32_t i, str_ref s, uint64_t h)
		{
			id   = i;
			hash = h;
			str  = s.str();
		}

		void clear() noexcept
		{
			refcount = 0;
			id       = 0;
			hash     = 0;
			str      = "";
		}
	};
	static_assert((sizeof(word_t) == (2*sizeof(uint32_t) + sizeof(uint64_t) + sizeof(std::string))), "word_t should have no padding");

	// str_ref key   - references words_t content
	// word_t* value - references the same word as the key
	// this hashtable should be ok with frequent deletes
	// since it's important for some load profiles (aka highly unique 'encrypted' nginx urls)
	// see benchmarks in experiments/exp_dictionary_perf.cpp
	using hashtable_t = tsl::robin_map<
							  str_ref
							, word_t*
							, dictionary_word_hasher_t
							, std::equal_to<str_ref>
							, std::allocator<std::pair<str_ref, word_t*>>
							, /*StoreHash=*/ true>;

	// word -> word_ptr
	struct hash_t : public hashtable_t
	{
	};

	// id -> word_t,
	// deque is critical here, since `hash` stores pointers to it's elements,
	// appends must not invalidate them
	struct words_t : public std::deque<word_t>
	{
	};

	// just a list of free ids in words_t
	struct freelist_t : public std::deque<uint32_t>
	{
	};

private:

	struct shard_t
	{
		mutable rw_mutex_t    mtx;

		uint32_t    id;
		hash_t      hash;       // TODO: shard this as well, to amortize the cost of rehash
		words_t     words;
		freelist_t  freelist;

		uint64_t    mem_used_by_word_strings = 0;
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
			size_t const n_erased = shard->hash.erase(str_ref { w->str }, w->hash);
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

		uint64_t const word_hash = hash_dictionary_word(word);
		shard_t *shard = get_shard_for_word_hash(word_hash);

		// MUST make work permanent here (aka increment refcount) -> no fastpath
		// as word might've been non-permanent (word from traffic before report creation for example)
		//
		// also if word already exists as permanent, we still increment refcount by 2
		// this is not an issue, since permanent words are not to be removed anyway (any refcount would work)

		scoped_write_lock_t lock_(shard->mtx);

		word_t *w = this->get_or_add___wrlocked(shard, word, word_hash);
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

		return this->get_or_add___ref(word, hash_dictionary_word(word));
	}

	// same as above, but with precalculated word_hash
	word_t const* get_or_add___ref(str_ref const word, uint64_t word_hash)
	{
		if (!word)
			return {};

		// TODO: not sure how to build fastpath here, since we need to increment refcount on return

		shard_t *shard = get_shard_for_word_hash(word_hash);

		scoped_write_lock_t lock_(shard->mtx);

		word_t *w = this->get_or_add___wrlocked(shard, word, word_hash);
		w->refcount += 1;

		return w;
	}

private:

	static uint64_t hash_dictionary_word(str_ref word)
	{
		return dictionary_word_hasher_t()(word);
	}

	shard_t* get_shard_for_word_id(uint32_t word_id) const
	{
		return &shards_[(word_id & shard_id_mask) >> (32 - shard_id_bits)];
	}

	shard_t* get_shard_for_word_hash(uint64_t word_hash) const
	{
		// NOTE: do NOT take lower bits here
		//  since hashtable_t will store lower 32 bits for rehash speedup
		//  and we don't want all words in this shard to have same lower bits
		// SO take higher order bits of our 64 bit hash for shard number
		// TODO: when hashtable sharding is implemented - MUST edit this
		return &shards_[word_hash >> (64 - shard_id_bits)];
	}

	// get or create a word, REFCOUNT IS NOT MODIFIED, i.e. even if just created -> refcount == 0
	word_t* get_or_add___wrlocked(shard_t *shard, str_ref const word, uint64_t word_hash)
	{
		// re-check word existence, might've appeated while upgrading the lock
		// can't just insert here, since the key is str_ref that should refer to word (created below)
		//  and not the incoming str_ref (that might be transient and just become invalid after this func returns)
		auto it = shard->hash.find(word, word_hash);
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

				w->set(word_id, word, word_hash);
				return w;
			}
			else
			{
				// word_id starts with 1, since 0 is reserved for empty
				assert(((shard->words.size() + 1) & word_id_mask) != 0);
				uint32_t const word_id = static_cast<uint32_t>(shard->words.size() + 1) | (shard->id << (32 - shard_id_bits));

				shard->words.emplace_back(word_id, word, word_hash);
				return &shard->words.back();
			}
		}();

		// XXX(antoxa): if this throws -> the word created above is basically 'leaked'
		shard->hash.emplace_hash(word_hash, str_ref { w->str }, w);

		return w;
	}
};

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__DICTIONARY_H_

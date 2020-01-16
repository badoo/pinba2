#ifndef PINBA__DICTIONARY_H_
#define PINBA__DICTIONARY_H_

#include <array>
#include <string>
#include <deque>

#include <pthread.h>

#include <tsl/robin_map.h>

#include "t1ha/t1ha.h"

#include <meow/intrusive_ptr.hpp>

#include "pinba/globals.h"
#include "pinba/hash.h"

////////////////////////////////////////////////////////////////////////////////////////////////

struct rw_mutex_t : private boost::noncopyable
{
	rw_mutex_t(bool writer_priority = false)
	{
		pthread_rwlockattr_init(&attr_);

		if (writer_priority)
		{
			pthread_rwlockattr_setkind_np(&attr_, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
		}

		pthread_rwlock_init(&mtx_, &attr_);
	}

	~rw_mutex_t()
	{
		pthread_rwlock_destroy(&mtx_);
		pthread_rwlockattr_destroy(&attr_);
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
	pthread_rwlock_t     mtx_;
	pthread_rwlockattr_t attr_;
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

inline uint64_t hash_dictionary_word(str_ref word)
{
	return dictionary_word_hasher_t()(word);
}

struct dictionary_memory_t
{
	uint64_t hash_bytes;
	uint64_t wordlist_bytes;
	uint64_t freelist_bytes;
	uint64_t strings_bytes;
};


struct nameword_dictionary_t : public meow::ref_counted_t
{
	struct nameword_t
	{
		uint32_t id       = 0;
		uint64_t id_hash  = 0;
		uint64_t str_hash = 0;
	};
	static_assert(std::is_nothrow_move_constructible<nameword_t>::value);

	struct nameword_equal_t
	{
		using is_transparent = void;

		inline bool operator()(std::string const& l, str_ref const& r) const
		{
			return str_ref {l} == r;
		}

		inline bool operator()(str_ref const& l, std::string const& r) const
		{
			return l == str_ref{r};
		}

		inline bool operator()(std::string const& l, std::string const& r) const
		{
			return l == r;
		}
	};

	using hashtable_t = tsl::robin_map<
							  std::string
							, nameword_t
							, dictionary_word_hasher_t
							, nameword_equal_t
							, std::allocator<std::pair<std::string, nameword_t>>
							, /*StoreHash=*/ true>;

	hashtable_t          hash;
	uint64_t             mem_used_by_word_strings;

public:

	nameword_dictionary_t()
		: mem_used_by_word_strings(0)
	{
	}

	void clone_from(nameword_dictionary_t const& other)
	{
		hash = other.hash;
		mem_used_by_word_strings = other.mem_used_by_word_strings;
	}

	size_t size() const
	{
		return hash.size();
	}

	dictionary_memory_t memory_used() const
	{
		return dictionary_memory_t {
			.hash_bytes     = hash.bucket_count() * sizeof(*hash.begin()),
			.wordlist_bytes = 0,
			.freelist_bytes = 0,
			.strings_bytes  = mem_used_by_word_strings,
		};
	}

	// get a word without synchronisation
	// note that this function returns a pointer,
	// so you must retain a reference to this dictionary for the desired lifetime of the word
	nameword_t const* get(str_ref word) const
	{
		uint64_t const word_hash = hash_dictionary_word(word);

		auto const it = hash.find(word, word_hash);
		if (it == hash.end())
			return {};

		return &it->second;
	}

	// inserts a word into the dictionary,
	// WARNING: can't be called concurrently with get() on the same instance, call clone() first
	// WARNING: returns a pointer like get, be careful with lifetime
	nameword_t const* insert_with_external_locking(str_ref word)
	{
		uint64_t const word_hash = hash_dictionary_word(word);

		uint32_t const word_id = hash.size() + 1;

		nameword_t nw = {
			.id       = word_id,
			.id_hash  = pinba::hash_number(word_id),
			.str_hash = word_hash,
		};

		auto const inserted_pair = hash.emplace_hash(word_hash, word.str(), nw);
		auto const& it = inserted_pair.first;

		if (inserted_pair.second) // inserted
			mem_used_by_word_strings += word.size();

		return &it->second;
	}
};
using nameword_dictionary_ptr = boost::intrusive_ptr<nameword_dictionary_t>;

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
		union {
			uint32_t  refcount;
			uint32_t  next_freelist_offset;
		};

		uint32_t    id;
		uint64_t    hash;
		std::string str;

		word_t() noexcept
			: refcount(0)
			, id(0)
			, hash(0)
			, str()
		{
		}

		word_t(word_t&& other) = default;
		word_t& operator=(word_t&& other) = default;
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

private:

	struct shard_t
	{
		mutable rw_mutex_t    mtx;

		uint32_t      id;
		uint32_t      freelist_head; // (offset+1) of the first elt in freelist, aka 0 -> unset, 1 -> offset == 0
		hash_t        hash;          // TODO: shard this as well, to amortize the cost of rehash
		words_t       words;

		uint64_t      mem_used_by_word_strings = 0;
	};

	mutable std::array<shard_t, shard_count> shards_;

public:

	dictionary_t()
	{
		for (uint32_t i = 0; i < shard_count; ++i)
		{
			shard_t *shard = &shards_[i];

			shard->id            = i;
			shard->freelist_head = 0;
		}
	}

	uint32_t size() const
	{
		uint32_t result = 0;

		for (auto const& shard : shards_)
		{
			scoped_read_lock_t lock_(shard.mtx);
			result += shard.words.size();
		}

		{
			auto const nwd_sz = load_nameword_dict()->size();
			result += nwd_sz;
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
			result.strings_bytes  += shard.mem_used_by_word_strings;
		}

		{
			auto const nwd_mu = load_nameword_dict()->memory_used();
			result.hash_bytes += nwd_mu.hash_bytes;
			result.strings_bytes += nwd_mu.strings_bytes;
		}

		return result;
	}

private:

	mutable nameword_dictionary_ptr  nameword_dictionary_;
	mutable std::mutex               nameword_update_mtx_;
	mutable std::mutex               nameword_load_and_store_mtx_;

	void store_nameword_dict(nameword_dictionary_ptr nwd)
	{
		std::lock_guard<std::mutex> lock_(nameword_load_and_store_mtx_); // sync with load_nameword_dict()
		nameword_dictionary_ = nwd;
	}

public:

	nameword_dictionary_t::nameword_t add_nameword(str_ref word)
	{
		std::lock_guard<std::mutex> lock_(nameword_update_mtx_); // sync with other writers

		auto const existing_nwd = this->load_nameword_dict();

		auto nwd = meow::make_intrusive<nameword_dictionary_t>();
		nwd->clone_from(*existing_nwd);

		nameword_dictionary_t::nameword_t const *nw = nwd->insert_with_external_locking(word);

		this->store_nameword_dict(nwd);

		return *nw;
	}

	nameword_dictionary_ptr load_nameword_dict() const
	{
		std::lock_guard<std::mutex> lock_(nameword_load_and_store_mtx_); // sync with store_nameword_dict()

		// FIXME: move this to ctor
		//  this assignment should be fin in a 'read-type' function, due to full mutex here
		if (!nameword_dictionary_)
			nameword_dictionary_ = meow::make_intrusive<nameword_dictionary_t>();

		return nameword_dictionary_;
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

		// a tmp string to use in case we're freeing the word
		// the idea is to free memory outside of lock in that case
		std::string to_release_tmp;

		// TODO: not worth using rlock just for quick assert that should never fire
		//       but might be worth using it for refcount check (in case it's atomic) and upgrade only after
		{
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

				// clear the word, and put it to shard's freelist
				w->next_freelist_offset = shard->freelist_head;
				shard->freelist_head    = word_offset + 1;

				w->id       = 0;
				w->hash     = 0;
				w->str.swap(to_release_tmp);
			}
		}

		// to_release_tmp is destroyed, freeing memory
	}

	// get or add a word that is never supposed to be removed
	word_t const* get_or_add___permanent(str_ref const word)
	{
		if (!word)
			return {};

		uint64_t const word_hash = hash_dictionary_word(word);
		shard_t *shard = get_shard_for_word_hash(word_hash);

		// MUST make word permanent here (aka increment refcount) -> no fastpath
		// as word might've been non-permanent (word from traffic before report creation for example)
		//
		// also if word already exists as permanent, we still increment refcount by 2
		// this is not an issue, since permanent words are not to be removed anyway (any refcount would work)

		// pre-copy the word for locked insert
		std::string word_str = word.str();


		scoped_write_lock_t lock_(shard->mtx);

		word_t *w = this->get_or_add___wrlocked(shard, std::move(word_str), word_hash);
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

		shard_t *shard = get_shard_for_word_hash(word_hash);

		// TODO: not sure how to build fastpath here, since we need to increment refcount on return

		// NOTE: now this is very likely to be an insert (as we're called from repacker here on it's cache-miss)
		//  so to reduce the amount of time spent under write lock, we'll do some hax here
		std::string word_str = word.str();



		scoped_write_lock_t lock_(shard->mtx);

		word_t *w = this->get_or_add___wrlocked(shard, std::move(word_str), word_hash);
		w->refcount += 1;

		return w;
	}

private:

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
	word_t* get_or_add___wrlocked(shard_t *shard, std::string word, uint64_t word_hash)
	{
		// potential SLOW things here (like alloc/free)
		//  1. wordlist push_back (should be rare in steady state, freelist should be non-empty)
		//  2. freelist pop_back (possible, and probably the most frequent one)
		//      TODO: try pop_front here, to amortize the cost of alloc/free to once per chunk
		//  3. hash growth (should be very rare in steady state) - but this is SUPER SLOW
		//  4. std::string move doing alloc+copy (no sane impl would do this, but small string optimization is fine)

		// to avoid extra hash lookup (find) - do some hax
		//
		// just insert right away, with the word we've got
		// it's going to "stay alive", as we'll move it into the final word object
		// we've got no word yet, so just insert nullptr for now
		auto insert_res = shard->hash.emplace_hash(word_hash, str_ref { word }, nullptr);
		auto& it = insert_res.first;

		// word already exists
		if (!insert_res.second)
			return it->second;

		// slower path, need to actually fix newly inserted word
		shard->mem_used_by_word_strings += word.size();

		word_t *w = [&]()
		{
			if (shard->freelist_head != 0)
			{
				uint32_t const word_offset = shard->freelist_head - 1;
				uint32_t const word_id = shard->freelist_head | (shard->id << (32 - shard_id_bits));

				word_t *w = &shard->words[word_offset];

				shard->freelist_head = w->next_freelist_offset; // remove from freelist
				w->next_freelist_offset = 0;                    // remove freelist marker

				w->id = word_id;

				return w;
			}
			else
			{
				// word_id starts with 1, since 0 is reserved for empty
				assert(((shard->words.size() + 1) & word_id_mask) != 0);
				uint32_t const word_id = static_cast<uint32_t>(shard->words.size() + 1) | (shard->id << (32 - shard_id_bits));

				// XXX(antoxa): if this throws, we're screwed - hash value (the inconsistent one at that :) )  is not removed
				shard->words.emplace_back();
				word_t *w = &shard->words.back();

				w->next_freelist_offset = 0; // never in freelist
				w->id = word_id;

				return w;
			}
		}();

		// finish initializing word
		w->hash = word_hash;
		w->str  = std::move(word);

		// fixup the key to point to long-living data now
		// this is not always required, but needed to for small string optimization for example
		str_ref& key_ref = const_cast<str_ref&>(it->first);
		key_ref = str_ref { w->str };

		// commit value
		it.value() = w;

		return w;
	}
};

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__DICTIONARY_H_

#ifndef PINBA__REPACKER_DICTIONARY_H_
#define PINBA__REPACKER_DICTIONARY_H_

#include <string>
#include <deque>
#include <unordered_map>

#include <t1ha/t1ha.h>
#include <tsl/robin_map.h>

#include <meow/intrusive_ptr.hpp>

#include "pinba/globals.h"
#include "pinba/dictionary.h"

////////////////////////////////////////////////////////////////////////////////////////////////
// single threaded cache for dictionary_t to be used by repacker
// get_or_add only, i.e. str_ref -> uint32_t

struct repacker_dictionary_t : private boost::noncopyable
{
	// this class is single threaded, so can use non-atomic counter here
	struct word_t : public boost::intrusive_ref_counter<word_t, boost::thread_unsafe_counter>
	{
		uint32_t const id;
		// no compiler padding here, since our base class has single 'int' member
		uint64_t const hash;
		str_ref  const str;

		word_t(uint32_t i, str_ref s, uint64_t h)
			: id(i)
			, hash(h)
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

	// a list of words, referencing this dictionary, that can be given away
	// to reports and/or selects to keep those words from being removed from the dictionary
	// while report or select is still using them
	//
	// words are ogranized into slices, to allow for bulk removal in a time-series manner
	struct wordslice_t : public meow::ref_counted_t
	{
		struct word_id_hasher_t
		{
			inline uint64_t operator()(uint32_t const key) const
			{
				return key; // prime growth will nicely use of shard_id bits at the top
				// return t1ha0(&key, sizeof(key), key);
			}
		};

		// NOTE: we're indexing by word_id here for a TODO idea reason :)
		//  when select is done, we could just merge all the wordslices
		//  and get a dictionary cache for that specific select
		struct hash_t : public tsl::robin_map<
											  uint32_t
											, word_ptr
											, word_id_hasher_t
											, std::equal_to<uint32_t>
											, std::allocator<std::pair<uint32_t, word_ptr>>
											// don't need StoreHash, hash is cheap to compute,
											// and it can't be used on rehash anyway with prime growth
											, /*StoreHash=*/ false
											// this is absolutely critical
											, tsl::rh::prime_growth_policy>
		{
		};
		static_assert(sizeof(hash_t::value_type) == 16, "4 bytes of padding is expected in hash_t");

		hash_t ht;

		wordslice_t()
		{
			PINBA_STATS_(objects).n_repacker_dict_ws++;
		}

		~wordslice_t()
		{
			PINBA_STATS_(objects).n_repacker_dict_ws--;
		}
	};
	using wordslice_ptr = boost::intrusive_ptr<wordslice_t>;

	struct wordinfo_t
	{
		uint32_t  id;
		// uint32_t padding__;
		word_ptr  ptr;
	};

	// a cache str_ref -> word_ptr
	// word_ptr is a local object, does not reference global dictionary
	//  but references the (immutable) word string, stored in global dictionary
	//
	// this hashtable should support efficient deletion
	struct word_to_id_hash_t : public tsl::robin_map<
											  str_ref
											, wordinfo_t
											, dictionary_word_hasher_t
											, std::equal_to<str_ref>
											, std::allocator<std::pair<str_ref, wordinfo_t>>
											, /*StoreHash=*/ true>
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
		this->start_new_wordslice();
	}

	// // FIXME: test only
	// str_ref get_word(uint32_t word_id) const
	// {
	// 	return d->get_word(word_id);
	// }

	dictionary_t::nameword_t get_nameword(str_ref word) const
	{
		return d->get_nameword(word);
	}

	dictionary_t::nameword_t add_nameword(str_ref word)
	{
		return d->add_nameword(word);
	}

	uint32_t get_or_add(str_ref const word)
	{
		if (!word)
			return 0;

		uint64_t const word_hash = dictionary_word_hasher_t()(word);

		// NOTE(antoxa): a hack, to avoid extra hash lookup *on slowpath*
		//  (and use emplace with precomputed hash, that operator[] does not support)
		//  the point here, is that we can't insert with `word` as key,
		//  since `word` might reference temporary memory but we do it anyway,
		//  and later, if insert was actually successful - we're going to replace the key,
		//  so that it references the equivalent string from actual upstream dictionary word (with proper lifetime)
		auto inserted_pair = word_to_id.emplace_hash(word_hash, word, wordinfo_t{});
		auto& it = inserted_pair.first;

		// fastpath: no insert, word is already there
		if (!inserted_pair.second)
		{
			this->add_to_current_wordslice(it->second);
			return it->second.id;
		}

		// slowpath
		// 0. this is going to be global-dictionary write-locked for the most part
		// 1. maybe (highly-likely) insert the word to global dictionary
		// 2. insert the newly-acquired word locally (this is where we'd save from having 'it' already computed)

		word_ptr w = [&]() {
			dictionary_t::word_t const *dict_word = d->get_or_add___ref(word, word_hash);
			return meow::make_intrusive<word_t>(dict_word->id, str_ref { dict_word->str }, word_hash);
		}();

		// fixup the key to point to long-living (in the global-dictionary) word str now
		str_ref& key_ref = const_cast<str_ref&>(it->first);
		key_ref = w->str;

		// commit local value
		it.value() = wordinfo_t { w->id, w };

		// remember to add to wordslice for lifetime tracking
		this->add_to_current_wordslice(it->second);
		return it->second.id;
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
		auto const words_to_erase = [&]() -> std::deque<word_ptr>
		{
			std::deque<word_ptr> words_to_erase;

			for (auto wordslice_it = erased_begin; wordslice_it != slices.end(); ++wordslice_it)
			{
				wordslice_ptr& ws = *wordslice_it;

				result.reaped_slices      += 1;
				result.reaped_words_local += ws->ht.size();

				// check refcounts to all words in this slice
				// and maybe delete from local dictionary if only this wordslice and local dictionary reference the word
				// this class is single-threaded, so noone is modifying anything while we're on it
				for(auto ht_it = ws->ht.begin(); ht_it != ws->ht.end(); ++ht_it)
				{
					// this class is single-threaded, so noone is modifying refcount while we're on it
					word_ptr& w = ht_it.value();
					assert(w->use_count() >= 2); // at least `this->word_to_id` and `w` must reference the word

					// LOG_DEBUG(PINBA_LOOGGER_, "{0}; '{1}' {2} {3}", __func__, w->str, w->id, w->use_count());

					if (w->use_count() == 2) // can erase, since ONLY `this->word_to_id` and `w` reference the word
					{
						result.reaped_words_global += 1;

						// LOG_DEBUG(PINBA_LOOGGER_, "reap_unused_wordslices; erase global '{0}' {1} {2}", w->str, w->id, w->use_count());

						// erase from local hash
						size_t const erased_count = word_to_id.erase(w->str, w->hash);
						assert(erased_count == 1);

						// save the word for later erase_ref from upstream dictionary
						// interusive_ptr doesn't support moves and `w` is a 'const&' anyway
						assert(w->use_count() == 1);
						words_to_erase.emplace_back(w);
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

	void add_to_current_wordslice(wordinfo_t const& wi)
	{
		// put the word into wordslice
		// try and avoid constructing word_ptr here (as it incurs a cache miss to increment word refcount)
		// NOTE: can't use w->hash here, since it's hashed w->str and not w->id
		auto inserted_pair = curr_slice->ht.emplace(wi.id, word_ptr{nullptr});
		auto& it = inserted_pair.first;

		// fastpath: word is already there
		if (!inserted_pair.second)
			return;

		// ok, word wasn't there, put it in, incrementing refcount
		it.value() = wi.ptr;
	}
};

using repacker_dslice_t   = repacker_dictionary_t::wordslice_t;
using repacker_dslice_ptr = repacker_dictionary_t::wordslice_ptr;

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__REPACKER_DICTIONARY_H_

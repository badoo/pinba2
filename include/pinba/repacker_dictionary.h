#ifndef PINBA__REPACKER_DICTIONARY_H_
#define PINBA__REPACKER_DICTIONARY_H_

#include <string>
#include <vector>
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
		// uint32_t const compiler_padding_here___;
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

	struct wordslice_t : public meow::ref_counted_t
	{
		struct word_id_hasher_t
		{
			inline uint64_t operator()(uint32_t const key) const
			{
				return t1ha0(&key, sizeof(key), key);
			}
		};

		struct hash_t : public tsl::robin_map<
											  uint32_t
											, word_ptr
											, word_id_hasher_t // TODO: try identity hash function here, should be ok
											, std::equal_to<uint32_t>
											, std::allocator<std::pair<uint32_t, word_ptr>>
											, /*StoreHash=*/ true>
		{
		};

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

	// hashtable should support efficient deletion
	// but dense_hash_map degrades ridiculously under high number of erases
	// so standard chaining-based unoredered_map is used
	struct word_to_id_hash_t : public tsl::robin_map<
											  str_ref
											, word_ptr
											, dictionary_word_hasher_t
											, std::equal_to<str_ref>
											, std::allocator<std::pair<str_ref, word_ptr>>
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
	}

	// // FIXME: test only
	// str_ref get_word(uint32_t word_id) const
	// {
	// 	return d->get_word(word_id);
	// }

	uint32_t get_or_add(str_ref const word)
	{
		if (!word)
			return 0;

		uint64_t const word_hash = dictionary_word_hasher_t()(word);

		// fastpath - local lookup
		auto const it = word_to_id.find(word, word_hash);
		if (word_to_id.end() != it)
		{
			this->add_to_current_wordslice(it->second.get());
			return it->second->id;
		}

		// cache miss - slowpath
		word_ptr w = [&]() {
			dictionary_t::word_t const *dict_word = d->get_or_add___ref(word);
			return meow::make_intrusive<word_t>(dict_word->id, str_ref { dict_word->str }, word_hash);
		}();

		word_to_id.emplace_hash(w->hash, w->str, w);
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
		auto const words_to_erase = [&]() -> std::vector<word_ptr> // FIXME: replace with deque here ? insert performance is important
		{
			std::vector<word_ptr> words_to_erase;

			for (auto wordslice_it = erased_begin; wordslice_it != slices.end(); ++wordslice_it)
			{
				wordslice_ptr& ws = *wordslice_it;

				result.reaped_slices      += 1;
				result.reaped_words_local += ws->ht.size();

				// reduce refcounts to all words in this slice
				for(auto ht_it = ws->ht.begin(); ht_it != ws->ht.end(); ++ht_it)
				{
					// this class is single-threaded, so noone is modifying refcount while we're on it
					word_ptr& w = ht_it.value();
					assert(w->use_count() >= 2); // at least `this->word_to_id` and `w` must reference the word

					// LOG_DEBUG(PINBA_LOOGGER_, "{0}; '{1}' {2} {3}", __func__, w->str, w->id, w->use_count());

					if (w->use_count() == 2)     // can erase, since ONLY `this->word_to_id` and `w` reference the word
					{
						result.reaped_words_global += 1;

						// LOG_DEBUG(PINBA_LOOGGER_, "reap_unused_wordslices; erase global '{0}' {1} {2}", w->str, w->id, w->use_count());

						// erase from local hash
						size_t const erased_count = word_to_id.erase(w->str, w->hash);
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

		// put the word into wordslice
		// NOTE: can't use w->hash here, since it's hashed w->str and not w->id
		auto inserted_pair = curr_slice->ht.emplace(w->id, w);

		// regardless of insert success, it's fine
		// if inserted: we're good, word is now referenced by wordslice
		// if not:      also good, the word was already there and referenced
		(void)inserted_pair;

		return;


		// word_ptr& ts_word = curr_slice->ht[w->id];
		// if (!ts_word)
		// {
		// 	ts_word.reset(w); // increments refcount here
		// }
	}
};

using repacker_dslice_t   = repacker_dictionary_t::wordslice_t;
using repacker_dslice_ptr = repacker_dictionary_t::wordslice_ptr;

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__REPACKER_DICTIONARY_H_

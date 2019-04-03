#ifndef PINBA__REPACKER_DICTIONARY_H_
#define PINBA__REPACKER_DICTIONARY_H_

#include <string>
#include <vector>
#include <unordered_map>

#include <meow/intrusive_ptr.hpp>

#include "t1ha/t1ha.h"

#include "pinba/globals.h"
#include "pinba/dictionary.h"

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
	struct word_to_id_hash_t : public std::unordered_map<str_ref, word_ptr, dictionary_word_hasher_t>
	{
		word_to_id_hash_t()
		{
			// at large map sizes (millions) and large string churn rates (mostly unique strings)
			// lookups become pretty slow (well, chaining)
			// so try and counter this with larger bucket counts
			//
			// gcc8 default is 1.0, which kinda feels too high
			// clang7 default is 1.0 (libc++)
			this->max_load_factor(0.3);
		}
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

				result.reaped_slices      += 1;
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

#endif // PINBA__REPACKER_DICTIONARY_H_

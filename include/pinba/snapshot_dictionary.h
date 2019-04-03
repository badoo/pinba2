#ifndef PINBA__SNAPSHOT_DICTIONARY_H_
#define PINBA__SNAPSHOT_DICTIONARY_H_

#include <sparsehash/dense_hash_map>

#include "t1ha/t1ha.h"

#include "pinba/globals.h"
#include "pinba/dictionary.h"

////////////////////////////////////////////////////////////////////////////////////////////////
// single threaded cache for dictionary_t
// transforms word_id -> word only
// intended to be used in snapshot scans and save on global dictionary locking, by caching stuff locally
// should be very efficient for wide reports with many repeating values
// XXX(antoxa): maybe move this out of global header, somewhere close to report_snapshot impls

struct snapshot_dictionary_t : private boost::noncopyable
{
	struct word_id_hasher_t
	{
		inline uint64_t operator()(uint32_t const key) const
		{
			return t1ha0(&key, sizeof(key), key);
		}
	};

	using hashtable_t = google::dense_hash_map<uint32_t, str_ref, word_id_hasher_t>;

	struct id_to_word_hash_t : public hashtable_t
	{
		id_to_word_hash_t()
			// pre-allocate to avoid some resizes,
			// this allocates (24 bytes per node) * 2^17 = 1.5Mb
			// -1 is important, due to how dense_hash calculates real bucket_count (< vs <=)
			: hashtable_t(32 * 1024 - 1)
		{
			this->set_empty_key(PINBA_INTERNAL___UINT32_MAX);
		}
	};

private:

	mutable id_to_word_hash_t  words_ht;
	dictionary_t const         *d;

public:

	explicit snapshot_dictionary_t(dictionary_t const *dict)
		: d(dict)
	{
		assert(d != nullptr);
	}

	// NOTE(antoxa): it's important to understand word lifetimes using this function
	// for report snapshots (where this is supposed to be used) - we rely on repacker_state to keep words alive
	str_ref get_word(uint32_t word_id) const
	{
		if (word_id == 0)
			return {};

		// fastpath: local lookup
		str_ref& value_ref = this->words_ht[word_id];
		if (!value_ref.empty()) // found
			return value_ref;

		// slowpath: remote lookup with read lock
		str_ref const remote_value = d->get_word(word_id);
		assert(!remote_value.empty() && "got empty word for non-zero word_id");

		value_ref = remote_value;
		return value_ref;
	}
};

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__SNAPSHOT_DICTIONARY_H_

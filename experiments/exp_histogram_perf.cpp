#include <vector>
#include <algorithm> // fill

#include <meow/stopwatch.hpp>

#include "pinba/globals.h"
#include "pinba/histogram.h"

#if 1 // HDR_HISTOGRAM_ORIGINAL_ENABLED

#include "hdr_histogram/hdr_histogram.h"

#else

struct hdr_histogram
{
	int64_t total_count;     // TODO: maybe make this 32 bit (configureable really)
	int64_t sub_bucket_mask; // TODO: make this 32bit (configureable really)
	// 16 bytes boundary

	int32_t counts_len;             // TODO: this should be unsigned
	int32_t sub_bucket_half_count;  // TODO: this should be unsigned
	uint8_t sub_bucket_half_count_magnitude;
	uint8_t unit_magnitude;
	uint16_t padding____;
	uint32_t padding____2;
	// 32 bytes boundary

	int64_t *counts;        // TODO: this should be unsigned, and 32bit (configureable really)

	// these are only used for informational purposes
	int64_t lowest_trackable_value;
	int64_t highest_trackable_value;
	int32_t significant_figures;
	// int32_t sub_bucket_half_count_magnitude;
	// int32_t sub_bucket_half_count;
	// int64_t sub_bucket_mask;

	int32_t sub_bucket_count; // kinda sometimes used for aux purposes
	// int32_t bucket_count; // this one is only used for informational purposes

	// this one is basically unused, always set to zero in ctor
	// int32_t normalizing_index_offset;

	// NOTE: these two are weird (min > max) and we're not going to use them anyway
	// int64_t min_value;
	// int64_t max_value;
	// int32_t normalizing_index_offset;
	// double conversion_ratio; // this one is unused in main code
	// int32_t counts_len;
	// int64_t total_count;
	// int64_t counts[0];
};

struct hdr_histogram_bucket_config
{
	int64_t lowest_trackable_value;
	int64_t highest_trackable_value;
	int64_t unit_magnitude;
	int64_t significant_figures;
	int32_t sub_bucket_half_count_magnitude;
	int32_t sub_bucket_half_count;
	int64_t sub_bucket_mask;
	int32_t sub_bucket_count;
	int32_t bucket_count;
	int32_t counts_len;
};

struct hdr_iter_percentiles
{
	bool seen_last_value;
	int32_t ticks_per_half_distance;
	double percentile_to_iterate_to;
	double percentile;
};

struct hdr_iter_recorded
{
	int64_t count_added_in_this_iteration_step;
};

struct hdr_iter_linear
{
	int64_t value_units_per_bucket;
	int64_t count_added_in_this_iteration_step;
	int64_t next_value_reporting_level;
	int64_t next_value_reporting_level_lowest_equivalent;
};

struct hdr_iter_log
{
	double log_base;
	int64_t count_added_in_this_iteration_step;
	int64_t next_value_reporting_level;
	int64_t next_value_reporting_level_lowest_equivalent;
};

/**
 * The basic iterator.  This is a generic structure
 * that supports all of the types of iteration.  Use
 * the appropriate initialiser to get the desired
 * iteration.
 *
 * @
 */
struct hdr_iter
{
	const struct hdr_histogram* h;
	/** raw index into the counts array */
	int32_t counts_index;
	/** snapshot of the length at the time the iterator is created */
	int32_t total_count;
	/** value directly from array for the current counts_index */
	int64_t count;
	/** sum of all of the counts up to and including the count at this index */
	int64_t cumulative_count;
	/** The current value based on counts_index */
	int64_t value;
	int64_t highest_equivalent_value;
	int64_t lowest_equivalent_value;
	int64_t median_equivalent_value;
	int64_t value_iterated_from;
	int64_t value_iterated_to;

	union
	{
		struct hdr_iter_percentiles percentiles;
		struct hdr_iter_recorded recorded;
		struct hdr_iter_linear linear;
		struct hdr_iter_log log;
	} specifics;

	bool (* _next_fp)(struct hdr_iter* iter);
};

typedef enum
{
	CLASSIC,
	CSV
} format_type;


//  ######   #######  ##     ## ##    ## ########  ######
// ##    ## ##     ## ##     ## ###   ##    ##    ##    ##
// ##       ##     ## ##     ## ####  ##    ##    ##
// ##       ##     ## ##     ## ## ## ##    ##     ######
// ##       ##     ## ##     ## ##  ####    ##          ##
// ##    ## ##     ## ##     ## ##   ###    ##    ##    ##
//  ######   #######   #######  ##    ##    ##     ######

// inline int32_t normalize_index(const struct hdr_histogram* h, int32_t index)
// {
// 	if (h->normalizing_index_offset == 0)
// 	{
// 		return index;
// 	}

// 	int32_t normalized_index = index - h->normalizing_index_offset;
// 	int32_t adjustment = 0;

// 	if (normalized_index < 0)
// 	{
// 		adjustment = h->counts_len;
// 	}
// 	else if (normalized_index >= h->counts_len)
// 	{
// 		adjustment = -h->counts_len;
// 	}

// 	return normalized_index + adjustment;
// }

inline int64_t counts_get_direct(const struct hdr_histogram* h, int32_t index)
{
	return h->counts[index];
}

inline int64_t counts_get_normalised(const struct hdr_histogram* h, int32_t index)
{
	// return counts_get_direct(h, normalize_index(h, index));
	return counts_get_direct(h, index);
}

inline void counts_inc_normalised(struct hdr_histogram* h, int32_t index, int64_t value)
{
	// int32_t normalised_index = normalize_index(h, index);
	// h->counts[normalised_index] += value;
	h->counts[index] += value;
	h->total_count += value;
}

// inline void update_min_max(struct hdr_histogram* h, int64_t value)
// {
// 	h->min_value = (value < h->min_value && value != 0) ? value : h->min_value;
// 	h->max_value = (value > h->max_value) ? value : h->max_value;
// }

// ##     ## ######## #### ##       #### ######## ##    ##
// ##     ##    ##     ##  ##        ##     ##     ##  ##
// ##     ##    ##     ##  ##        ##     ##      ####
// ##     ##    ##     ##  ##        ##     ##       ##
// ##     ##    ##     ##  ##        ##     ##       ##
// ##     ##    ##     ##  ##        ##     ##       ##
//  #######     ##    #### ######## ####    ##       ##

inline int64_t power(int64_t base, int64_t exp)
{
	int64_t result = 1;
	while(exp)
	{
		result *= base; exp--;
	}
	return result;
}

inline int32_t get_bucket_index(const struct hdr_histogram* h, int64_t value)
{
	int32_t pow2ceiling = 64 - __builtin_clzll(value | h->sub_bucket_mask); // smallest power of 2 containing value
	return pow2ceiling - h->unit_magnitude - (h->sub_bucket_half_count_magnitude + 1);
}

inline int32_t get_sub_bucket_index(int64_t value, int32_t bucket_index, int32_t unit_magnitude)
{
	return (int32_t)(value >> (bucket_index + unit_magnitude));
}

inline int32_t counts_index(const struct hdr_histogram* h, int32_t bucket_index, int32_t sub_bucket_index)
{
	// Calculate the index for the first entry in the bucket:
	// (The following is the equivalent of ((bucket_index + 1) * subBucketHalfCount) ):
	int32_t bucket_base_index = (bucket_index + 1) << h->sub_bucket_half_count_magnitude;
	// Calculate the offset in the bucket:
	int32_t offset_in_bucket = sub_bucket_index - h->sub_bucket_half_count;
	// The following is the equivalent of ((sub_bucket_index  - subBucketHalfCount) + bucketBaseIndex;
	return bucket_base_index + offset_in_bucket;
}

inline int32_t counts_index_for(const struct hdr_histogram* h, int64_t value)
{
	int32_t bucket_index     = get_bucket_index(h, value);
	int32_t sub_bucket_index = get_sub_bucket_index(value, bucket_index, h->unit_magnitude);

	return counts_index(h, bucket_index, sub_bucket_index);
}

inline int64_t value_from_index(int32_t bucket_index, int32_t sub_bucket_index, int32_t unit_magnitude)
{
	return ((int64_t) sub_bucket_index) << (bucket_index + unit_magnitude);
}

int64_t hdr_value_at_index(const struct hdr_histogram *h, int32_t index)
{
    int32_t bucket_index = (index >> h->sub_bucket_half_count_magnitude) - 1;
    int32_t sub_bucket_index = (index & (h->sub_bucket_half_count - 1)) + h->sub_bucket_half_count;

    if (bucket_index < 0)
    {
        sub_bucket_index -= h->sub_bucket_half_count;
        bucket_index = 0;
    }

    return value_from_index(bucket_index, sub_bucket_index, h->unit_magnitude);
}

inline int64_t hdr_size_of_equivalent_value_range(const struct hdr_histogram* h, int64_t value)
{
	int32_t bucket_index     = get_bucket_index(h, value);
	int32_t sub_bucket_index = get_sub_bucket_index(value, bucket_index, h->unit_magnitude);
	int32_t adjusted_bucket  = (sub_bucket_index >= h->sub_bucket_count) ? (bucket_index + 1) : bucket_index;
	return INT64_C(1) << (h->unit_magnitude + adjusted_bucket);
}

inline int64_t lowest_equivalent_value(const struct hdr_histogram* h, int64_t value)
{
	int32_t bucket_index     = get_bucket_index(h, value);
	int32_t sub_bucket_index = get_sub_bucket_index(value, bucket_index, h->unit_magnitude);
	return value_from_index(bucket_index, sub_bucket_index, h->unit_magnitude);
}

inline int64_t hdr_next_non_equivalent_value(const struct hdr_histogram *h, int64_t value)
{
	return lowest_equivalent_value(h, value) + hdr_size_of_equivalent_value_range(h, value);
}

inline int64_t highest_equivalent_value(const struct hdr_histogram* h, int64_t value)
{
	return hdr_next_non_equivalent_value(h, value) - 1;
}

inline int64_t hdr_median_equivalent_value(const struct hdr_histogram *h, int64_t value)
{
	return lowest_equivalent_value(h, value) + (hdr_size_of_equivalent_value_range(h, value) >> 1);
}

// inline int64_t non_zero_min(const struct hdr_histogram* h)
// {
// 	if (INT64_MAX == h->min_value)
// 	{
// 		return INT64_MAX;
// 	}

// 	return lowest_equivalent_value(h, h->min_value);
// }

inline void hdr_reset_internal_counters(struct hdr_histogram* h)
{
	int min_non_zero_index = -1;
	int max_index = -1;
	int64_t observed_total_count = 0;
	int i;

	for (i = 0; i < h->counts_len; i++)
	{
		int64_t count_at_index;

		if ((count_at_index = counts_get_direct(h, i)) > 0)
		{
			observed_total_count += count_at_index;
			max_index = i;
			if (min_non_zero_index == -1 && i != 0)
			{
				min_non_zero_index = i;
			}
		}
	}

	// if (max_index == -1)
	// {
	// 	h->max_value = 0;
	// }
	// else
	// {
	// 	int64_t max_value = hdr_value_at_index(h, max_index);
	// 	h->max_value = highest_equivalent_value(h, max_value);
	// }

	// if (min_non_zero_index == -1)
	// {
	// 	h->min_value = INT64_MAX;
	// }
	// else
	// {
	// 	h->min_value = hdr_value_at_index(h, min_non_zero_index);
	// }

	h->total_count = observed_total_count;
}

inline int32_t buckets_needed_to_cover_value(int64_t value, int32_t sub_bucket_count, int32_t unit_magnitude)
{
	int64_t smallest_untrackable_value = ((int64_t) sub_bucket_count) << unit_magnitude;
	int32_t buckets_needed = 1;
	while (smallest_untrackable_value <= value)
	{
		if (smallest_untrackable_value > INT64_MAX / 2)
		{
			return buckets_needed + 1;
		}
		smallest_untrackable_value <<= 1;
		buckets_needed++;
	}

	return buckets_needed;
}

inline bool hdr_values_are_equivalent(const struct hdr_histogram* h, int64_t a, int64_t b)
{
	return lowest_equivalent_value(h, a) == lowest_equivalent_value(h, b);
}

inline int64_t hdr_lowest_equivalent_value(const struct hdr_histogram* h, int64_t value)
{
	return lowest_equivalent_value(h, value);
}

inline int64_t hdr_count_at_value(const struct hdr_histogram* h, int64_t value)
{
	return counts_get_normalised(h, counts_index_for(h, value));
}

inline int64_t hdr_count_at_index(const struct hdr_histogram* h, int32_t index)
{
	return counts_get_normalised(h, index);
}

// ##     ## ######## ##     ##  #######  ########  ##    ##
// ###   ### ##       ###   ### ##     ## ##     ##  ##  ##
// #### #### ##       #### #### ##     ## ##     ##   ####
// ## ### ## ######   ## ### ## ##     ## ########     ##
// ##     ## ##       ##     ## ##     ## ##   ##      ##
// ##     ## ##       ##     ## ##     ## ##    ##     ##
// ##     ## ######## ##     ##  #######  ##     ##    ##

int hdr_calculate_bucket_config(
		int64_t lowest_trackable_value,
		int64_t highest_trackable_value,
		int significant_figures,
		struct hdr_histogram_bucket_config* cfg)
{
	if (lowest_trackable_value < 1 ||
			significant_figures < 1 || 5 < significant_figures)
	{
		return EINVAL;
	}
	else if (lowest_trackable_value * 2 > highest_trackable_value)
	{
		return EINVAL;
	}

	cfg->lowest_trackable_value = lowest_trackable_value;
	cfg->significant_figures = significant_figures;
	cfg->highest_trackable_value = highest_trackable_value;

	int64_t largest_value_with_single_unit_resolution = 2 * power(10, significant_figures);
	int32_t sub_bucket_count_magnitude = (int32_t) ceil(log((double)largest_value_with_single_unit_resolution) / log(2));
	cfg->sub_bucket_half_count_magnitude = ((sub_bucket_count_magnitude > 1) ? sub_bucket_count_magnitude : 1) - 1;

	cfg->unit_magnitude = (int32_t) floor(log((double)lowest_trackable_value) / log(2));

	cfg->sub_bucket_count      = (int32_t) pow(2, (cfg->sub_bucket_half_count_magnitude + 1));
	cfg->sub_bucket_half_count = cfg->sub_bucket_count / 2;
	cfg->sub_bucket_mask       = ((int64_t) cfg->sub_bucket_count - 1) << cfg->unit_magnitude;

	// determine exponent range needed to support the trackable value with no overflow:
	cfg->bucket_count = buckets_needed_to_cover_value(highest_trackable_value, cfg->sub_bucket_count, (int32_t)cfg->unit_magnitude);
	cfg->counts_len = (cfg->bucket_count + 1) * (cfg->sub_bucket_count / 2);

	return 0;
}

void hdr_init_preallocated(struct hdr_histogram* h, struct hdr_histogram_bucket_config* cfg)
{
	h->lowest_trackable_value          = cfg->lowest_trackable_value;
	h->highest_trackable_value         = cfg->highest_trackable_value;
	h->unit_magnitude                  = (int32_t)cfg->unit_magnitude;
	h->significant_figures             = (int32_t)cfg->significant_figures;
	h->sub_bucket_half_count_magnitude = cfg->sub_bucket_half_count_magnitude;
	h->sub_bucket_half_count           = cfg->sub_bucket_half_count;
	h->sub_bucket_mask                 = cfg->sub_bucket_mask;
	h->sub_bucket_count                = cfg->sub_bucket_count;
	// h->min_value                       = INT64_MAX;
	// h->max_value                       = 0;
	// h->normalizing_index_offset        = 0;
	// h->conversion_ratio                = 1.0;
	// h->bucket_count                    = cfg->bucket_count;
	h->counts_len                      = cfg->counts_len;
	h->total_count                     = 0;
}

int hdr_init(
		int64_t lowest_trackable_value,
		int64_t highest_trackable_value,
		int significant_figures,
		struct hdr_histogram** result)
{
	struct hdr_histogram_bucket_config cfg;

	int r = hdr_calculate_bucket_config(lowest_trackable_value, highest_trackable_value, significant_figures, &cfg);
	if (r)
	{
		return r;
	}

	size_t histogram_size           = sizeof(struct hdr_histogram) + cfg.counts_len * sizeof(int64_t);
	struct hdr_histogram* histogram = (struct hdr_histogram*) malloc(histogram_size);

	if (!histogram)
	{
		return ENOMEM;
	}

	// memset will ensure that all of the function pointers are null.
	memset((void*) histogram, 0, histogram_size);

	hdr_init_preallocated(histogram, &cfg);
	*result = histogram;

	return 0;
}


int hdr_alloc(int64_t highest_trackable_value, int significant_figures, struct hdr_histogram** result)
{
	return hdr_init(1, highest_trackable_value, significant_figures, result);
}

// reset a histogram to zero.
void hdr_reset(struct hdr_histogram *h)
{
	 h->total_count = 0;
	 // h->min_value = INT64_MAX;
	 // h->max_value = 0;
	 memset((void *) &h->counts, 0, (sizeof(int64_t) * h->counts_len));
	 return;
}

size_t hdr_get_memory_size(struct hdr_histogram *h)
{
	return sizeof(struct hdr_histogram) + h->counts_len * sizeof(int64_t);
}

// #### ######## ######## ########     ###    ########  #######  ########   ######
//  ##     ##    ##       ##     ##   ## ##      ##    ##     ## ##     ## ##    ##
//  ##     ##    ##       ##     ##  ##   ##     ##    ##     ## ##     ## ##
//  ##     ##    ######   ########  ##     ##    ##    ##     ## ########   ######
//  ##     ##    ##       ##   ##   #########    ##    ##     ## ##   ##         ##
//  ##     ##    ##       ##    ##  ##     ##    ##    ##     ## ##    ##  ##    ##
// ####    ##    ######## ##     ## ##     ##    ##     #######  ##     ##  ######


bool hdr_iter_next(struct hdr_iter* iter)
{
	return iter->_next_fp(iter);
}

static bool has_buckets(struct hdr_iter* iter)
{
	return iter->counts_index < iter->h->counts_len;
}

static bool has_next(struct hdr_iter* iter)
{
	return iter->cumulative_count < iter->total_count;
}

static bool move_next(struct hdr_iter* iter)
{
	iter->counts_index++;

	if (!has_buckets(iter))
	{
		return false;
	}

	iter->count = counts_get_normalised(iter->h, iter->counts_index);
	iter->cumulative_count += iter->count;

	iter->value = hdr_value_at_index(iter->h, iter->counts_index);
	iter->highest_equivalent_value = highest_equivalent_value(iter->h, iter->value);
	iter->lowest_equivalent_value = lowest_equivalent_value(iter->h, iter->value);
	iter->median_equivalent_value = hdr_median_equivalent_value(iter->h, iter->value);

	return true;
}

static int64_t peek_next_value_from_index(struct hdr_iter* iter)
{
	return hdr_value_at_index(iter->h, iter->counts_index + 1);
}

static bool next_value_greater_than_reporting_level_upper_bound(
	struct hdr_iter *iter, int64_t reporting_level_upper_bound)
{
	if (iter->counts_index >= iter->h->counts_len)
	{
		return false;
	}

	return peek_next_value_from_index(iter) > reporting_level_upper_bound;
}

static bool _basic_iter_next(struct hdr_iter *iter)
{
	if (!has_next(iter) || iter->counts_index >= iter->h->counts_len)
	{
		return false;
	}

	move_next(iter);

	return true;
}

static void _update_iterated_values(struct hdr_iter* iter, int64_t new_value_iterated_to)
{
	iter->value_iterated_from = iter->value_iterated_to;
	iter->value_iterated_to = new_value_iterated_to;
}

static bool _all_values_iter_next(struct hdr_iter* iter)
{
	bool result = move_next(iter);

	if (result)
	{
		_update_iterated_values(iter, iter->value);
	}

	return result;
}

void hdr_iter_init(struct hdr_iter* iter, const struct hdr_histogram* h)
{
	iter->h = h;

	iter->counts_index = -1;
	iter->total_count = h->total_count;
	iter->count = 0;
	iter->cumulative_count = 0;
	iter->value = 0;
	iter->highest_equivalent_value = 0;
	iter->value_iterated_from = 0;
	iter->value_iterated_to = 0;

	iter->_next_fp = _all_values_iter_next;
}

// ########  ######## ########   ######  ######## ##    ## ######## #### ##       ########  ######
// ##     ## ##       ##     ## ##    ## ##       ###   ##    ##     ##  ##       ##       ##    ##
// ##     ## ##       ##     ## ##       ##       ####  ##    ##     ##  ##       ##       ##
// ########  ######   ########  ##       ######   ## ## ##    ##     ##  ##       ######    ######
// ##        ##       ##   ##   ##       ##       ##  ####    ##     ##  ##       ##             ##
// ##        ##       ##    ##  ##    ## ##       ##   ###    ##     ##  ##       ##       ##    ##
// ##        ######## ##     ##  ######  ######## ##    ##    ##    #### ######## ########  ######

static bool _percentile_iter_next(struct hdr_iter* iter)
{
	struct hdr_iter_percentiles* percentiles = &iter->specifics.percentiles;

	if (!has_next(iter))
	{
		if (percentiles->seen_last_value)
		{
			return false;
		}

		percentiles->seen_last_value = true;
		percentiles->percentile = 100.0;

		return true;
	}

	if (iter->counts_index == -1 && !_basic_iter_next(iter))
	{
		return false;
	}

	do
	{
		double current_percentile = (100.0 * (double) iter->cumulative_count) / iter->h->total_count;
		if (iter->count != 0 &&
				percentiles->percentile_to_iterate_to <= current_percentile)
		{
			_update_iterated_values(iter, highest_equivalent_value(iter->h, iter->value));

			percentiles->percentile = percentiles->percentile_to_iterate_to;
			int64_t temp = (int64_t)(log(100 / (100.0 - (percentiles->percentile_to_iterate_to))) / log(2)) + 1;
			int64_t half_distance = (int64_t) pow(2, (double) temp);
			int64_t percentile_reporting_ticks = percentiles->ticks_per_half_distance * half_distance;
			percentiles->percentile_to_iterate_to += 100.0 / percentile_reporting_ticks;

			return true;
		}
	}
	while (_basic_iter_next(iter));

	return true;
}

void hdr_iter_percentile_init(struct hdr_iter* iter, const struct hdr_histogram* h, int32_t ticks_per_half_distance)
{
	iter->h = h;

	hdr_iter_init(iter, h);

	iter->specifics.percentiles.seen_last_value          = false;
	iter->specifics.percentiles.ticks_per_half_distance  = ticks_per_half_distance;
	iter->specifics.percentiles.percentile_to_iterate_to = 0.0;
	iter->specifics.percentiles.percentile               = 0.0;

	iter->_next_fp = _percentile_iter_next;
}

// ########  ########  ######   #######  ########  ########  ######## ########
// ##     ## ##       ##    ## ##     ## ##     ## ##     ## ##       ##     ##
// ##     ## ##       ##       ##     ## ##     ## ##     ## ##       ##     ##
// ########  ######   ##       ##     ## ########  ##     ## ######   ##     ##
// ##   ##   ##       ##       ##     ## ##   ##   ##     ## ##       ##     ##
// ##    ##  ##       ##    ## ##     ## ##    ##  ##     ## ##       ##     ##
// ##     ## ########  ######   #######  ##     ## ########  ######## ########


static bool _recorded_iter_next(struct hdr_iter* iter)
{
	while (_basic_iter_next(iter))
	{
		if (iter->count != 0)
		{
			_update_iterated_values(iter, iter->value);

			iter->specifics.recorded.count_added_in_this_iteration_step = iter->count;
			return true;
		}
	}

	return false;
}

void hdr_iter_recorded_init(struct hdr_iter* iter, const struct hdr_histogram* h)
{
	hdr_iter_init(iter, h);

	iter->specifics.recorded.count_added_in_this_iteration_step = 0;

	iter->_next_fp = _recorded_iter_next;
}

// ##       #### ##    ## ########    ###    ########
// ##        ##  ###   ## ##         ## ##   ##     ##
// ##        ##  ####  ## ##        ##   ##  ##     ##
// ##        ##  ## ## ## ######   ##     ## ########
// ##        ##  ##  #### ##       ######### ##   ##
// ##        ##  ##   ### ##       ##     ## ##    ##
// ######## #### ##    ## ######## ##     ## ##     ##


static bool _iter_linear_next(struct hdr_iter* iter)
{
	struct hdr_iter_linear* linear = &iter->specifics.linear;

	linear->count_added_in_this_iteration_step = 0;

	if (has_next(iter) ||
		next_value_greater_than_reporting_level_upper_bound(
			iter, linear->next_value_reporting_level_lowest_equivalent))
	{
		do
		{
			if (iter->value >= linear->next_value_reporting_level_lowest_equivalent)
			{
				_update_iterated_values(iter, linear->next_value_reporting_level);

				linear->next_value_reporting_level += linear->value_units_per_bucket;
				linear->next_value_reporting_level_lowest_equivalent =
					lowest_equivalent_value(iter->h, linear->next_value_reporting_level);

				return true;
			}

			if (!move_next(iter))
			{
				return true;
			}

			linear->count_added_in_this_iteration_step += iter->count;
		}
		while (true);
	}

	return false;
}


void hdr_iter_linear_init(struct hdr_iter* iter, const struct hdr_histogram* h, int64_t value_units_per_bucket)
{
	hdr_iter_init(iter, h);

	iter->specifics.linear.count_added_in_this_iteration_step = 0;
	iter->specifics.linear.value_units_per_bucket = value_units_per_bucket;
	iter->specifics.linear.next_value_reporting_level = value_units_per_bucket;
	iter->specifics.linear.next_value_reporting_level_lowest_equivalent = lowest_equivalent_value(h, value_units_per_bucket);

	iter->_next_fp = _iter_linear_next;
}

// ##        #######   ######      ###    ########  #### ######## ##     ## ##     ## ####  ######
// ##       ##     ## ##    ##    ## ##   ##     ##  ##     ##    ##     ## ###   ###  ##  ##    ##
// ##       ##     ## ##         ##   ##  ##     ##  ##     ##    ##     ## #### ####  ##  ##
// ##       ##     ## ##   #### ##     ## ########   ##     ##    ######### ## ### ##  ##  ##
// ##       ##     ## ##    ##  ######### ##   ##    ##     ##    ##     ## ##     ##  ##  ##
// ##       ##     ## ##    ##  ##     ## ##    ##   ##     ##    ##     ## ##     ##  ##  ##    ##
// ########  #######   ######   ##     ## ##     ## ####    ##    ##     ## ##     ## ####  ######

static bool _log_iter_next(struct hdr_iter *iter)
{
	struct hdr_iter_log* logarithmic = &iter->specifics.log;

	logarithmic->count_added_in_this_iteration_step = 0;

	if (has_next(iter) ||
		next_value_greater_than_reporting_level_upper_bound(
			iter, logarithmic->next_value_reporting_level_lowest_equivalent))
	{
		do
		{
			if (iter->value >= logarithmic->next_value_reporting_level_lowest_equivalent)
			{
				_update_iterated_values(iter, logarithmic->next_value_reporting_level);

				logarithmic->next_value_reporting_level *= (int64_t)logarithmic->log_base;
				logarithmic->next_value_reporting_level_lowest_equivalent = lowest_equivalent_value(iter->h, logarithmic->next_value_reporting_level);

				return true;
			}

			if (!move_next(iter))
			{
				return true;
			}

			logarithmic->count_added_in_this_iteration_step += iter->count;
		}
		while (true);
	}

	return false;
}

void hdr_iter_log_init(
		struct hdr_iter* iter,
		const struct hdr_histogram* h,
		int64_t value_units_first_bucket,
		double log_base)
{
	hdr_iter_init(iter, h);
	iter->specifics.log.count_added_in_this_iteration_step = 0;
	iter->specifics.log.log_base = log_base;
	iter->specifics.log.next_value_reporting_level = value_units_first_bucket;
	iter->specifics.log.next_value_reporting_level_lowest_equivalent = lowest_equivalent_value(h, value_units_first_bucket);

	iter->_next_fp = _log_iter_next;
}

// ##     ## ########  ########     ###    ######## ########  ######
// ##     ## ##     ## ##     ##   ## ##      ##    ##       ##    ##
// ##     ## ##     ## ##     ##  ##   ##     ##    ##       ##
// ##     ## ########  ##     ## ##     ##    ##    ######    ######
// ##     ## ##        ##     ## #########    ##    ##             ##
// ##     ## ##        ##     ## ##     ##    ##    ##       ##    ##
//  #######  ##        ########  ##     ##    ##    ########  ######


bool hdr_record_values(struct hdr_histogram* h, int64_t value, int64_t count)
{
	if (__builtin_expect((value < 0), 0))
	{
		return false;
	}

	int32_t counts_index = counts_index_for(h, value);

	if (__builtin_expect((counts_index < 0 || h->counts_len <= counts_index), 0))
	{
		return false;
	}

	counts_inc_normalised(h, counts_index, count);

	// FIXME: some other code relies on this, but nothing major it seems (some iterators?)
	// update_min_max(h, value);

	return true;
}

bool hdr_record_value(struct hdr_histogram* h, int64_t value)
{
	return hdr_record_values(h, value, 1);
}


bool hdr_record_corrected_values(struct hdr_histogram* h, int64_t value, int64_t count, int64_t expected_interval)
{
	if (!hdr_record_values(h, value, count))
	{
		return false;
	}

	if (expected_interval <= 0 || value <= expected_interval)
	{
		return true;
	}

	int64_t missing_value = value - expected_interval;
	for (; missing_value >= expected_interval; missing_value -= expected_interval)
	{
		if (!hdr_record_values(h, missing_value, count))
		{
			return false;
		}
	}

	return true;
}

bool hdr_record_corrected_value(struct hdr_histogram* h, int64_t value, int64_t expected_interval)
{
	return hdr_record_corrected_values(h, value, 1, expected_interval);
}

int64_t hdr_add(struct hdr_histogram* h, const struct hdr_histogram* from)
{
	struct hdr_iter iter;
	hdr_iter_recorded_init(&iter, from);
	int64_t dropped = 0;

	while (hdr_iter_next(&iter))
	{
		int64_t value = iter.value;
		int64_t count = iter.count;

		if (!hdr_record_values(h, value, count))
		{
			dropped += count;
		}
	}

	return dropped;
}

int64_t hdr_add_while_correcting_for_coordinated_omission(
		struct hdr_histogram* h, struct hdr_histogram* from, int64_t expected_interval)
{
	struct hdr_iter iter;
	hdr_iter_recorded_init(&iter, from);
	int64_t dropped = 0;

	while (hdr_iter_next(&iter))
	{
		int64_t value = iter.value;
		int64_t count = iter.count;

		if (!hdr_record_corrected_values(h, value, count, expected_interval))
		{
			dropped += count;
		}
	}

	return dropped;
}

// ##     ##    ###    ##       ##     ## ########  ######
// ##     ##   ## ##   ##       ##     ## ##       ##    ##
// ##     ##  ##   ##  ##       ##     ## ##       ##
// ##     ## ##     ## ##       ##     ## ######    ######
//  ##   ##  ######### ##       ##     ## ##             ##
//   ## ##   ##     ## ##       ##     ## ##       ##    ##
//    ###    ##     ## ########  #######  ########  ######


// int64_t hdr_max(const struct hdr_histogram* h)
// {
// 	if (0 == h->max_value)
// 	{
// 		return 0;
// 	}

// 	return highest_equivalent_value(h, h->max_value);
// }

// int64_t hdr_min(const struct hdr_histogram* h)
// {
// 	if (0 < hdr_count_at_index(h, 0))
// 	{
// 		return 0;
// 	}

// 	return non_zero_min(h);
// }

int64_t hdr_value_at_percentile(const struct hdr_histogram* h, double percentile)
{
	struct hdr_iter iter;
	hdr_iter_init(&iter, h);

	double requested_percentile = percentile < 100.0 ? percentile : 100.0;
	int64_t count_at_percentile =
		(int64_t) (((requested_percentile / 100) * h->total_count) + 0.5);
	count_at_percentile = count_at_percentile > 1 ? count_at_percentile : 1;
	int64_t total = 0;

	while (hdr_iter_next(&iter))
	{
		total += iter.count;

		if (total >= count_at_percentile)
		{
			int64_t value_from_index = iter.value;
			return highest_equivalent_value(h, value_from_index);
		}
	}

	return 0;
}

double hdr_mean(const struct hdr_histogram* h)
{
	struct hdr_iter iter;
	int64_t total = 0;

	hdr_iter_init(&iter, h);

	while (hdr_iter_next(&iter))
	{
		if (0 != iter.count)
		{
			total += iter.count * hdr_median_equivalent_value(h, iter.value);
		}
	}

	return (total * 1.0) / h->total_count;
}

double hdr_stddev(const struct hdr_histogram* h)
{
	double mean = hdr_mean(h);
	double geometric_dev_total = 0.0;

	struct hdr_iter iter;
	hdr_iter_init(&iter, h);

	while (hdr_iter_next(&iter))
	{
		if (0 != iter.count)
		{
			double dev = (hdr_median_equivalent_value(h, iter.value) * 1.0) - mean;
			geometric_dev_total += (dev * dev) * iter.count;
		}
	}

	return sqrt(geometric_dev_total / h->total_count);
}

#endif // HDR_HISTOGRAM_ORIGINAL_ENABLED

////////////////////////////////////////////////////////////////////////////////////////////////

struct hdr_histogram_conf_t
{
	// hot
	int32_t sub_bucket_half_count_magnitude;
	int32_t sub_bucket_half_count;
	int32_t sub_bucket_count;
	int32_t counts_len;
	int64_t unit_magnitude;
	int64_t sub_bucket_mask;

	// information only
	int64_t lowest_trackable_value; // FIXME: this needs to be able to take 0 as value (which becomes -inf)
	int64_t highest_trackable_value;
	int64_t significant_figures;
	int32_t bucket_count;
};

int32_t hdr___buckets_needed_to_cover_value(int64_t value, int32_t sub_bucket_count, int32_t unit_magnitude)
{
	int64_t smallest_untrackable_value = ((int64_t) sub_bucket_count) << unit_magnitude;
	int32_t buckets_needed = 1;
	while (smallest_untrackable_value <= value)
	{
		if (smallest_untrackable_value > INT64_MAX / 2)
		{
			return buckets_needed + 1;
		}
		smallest_untrackable_value <<= 1;
		buckets_needed++;
	}

	return buckets_needed;
}

int hdr_histogram_conf___init(
	struct hdr_histogram_conf_t* cfg,
	int64_t lowest_trackable_value,
	int64_t highest_trackable_value,
	int significant_bits
)
{
	if (significant_bits < 1 || significant_bits > 16)
	{
		return EINVAL;
	}

	if (lowest_trackable_value < 1)
	{
		return EINVAL;
	}

	if (lowest_trackable_value * 2 > highest_trackable_value)
	{
		return EINVAL;
	}

	cfg->lowest_trackable_value  = lowest_trackable_value;
	cfg->significant_figures     = significant_bits;
	cfg->highest_trackable_value = highest_trackable_value;

	int64_t largest_value_with_single_unit_resolution = 2 * (1 << significant_bits);
	int32_t sub_bucket_count_magnitude = (int32_t) ceil(log((double)largest_value_with_single_unit_resolution) / log(2));
	cfg->sub_bucket_half_count_magnitude = ((sub_bucket_count_magnitude > 1) ? sub_bucket_count_magnitude : 1) - 1;

	cfg->unit_magnitude = (int32_t) floor(log((double)lowest_trackable_value) / log(2));

	cfg->sub_bucket_count      = (int32_t) pow(2, (cfg->sub_bucket_half_count_magnitude + 1));
	cfg->sub_bucket_half_count = cfg->sub_bucket_count / 2;
	cfg->sub_bucket_mask       = ((int64_t) cfg->sub_bucket_count - 1) << cfg->unit_magnitude;

	// determine exponent range needed to support the trackable value with no overflow:
	cfg->bucket_count = hdr___buckets_needed_to_cover_value(highest_trackable_value, cfg->sub_bucket_count, (int32_t)cfg->unit_magnitude);
	cfg->counts_len = (cfg->bucket_count + 1) * (cfg->sub_bucket_count / 2);

	return 0;
}


struct hdr_histogram_t : private boost::noncopyable
{
	using config_t = hdr_histogram_conf_t;


	hdr_histogram_t(config_t const& conf)
	{
		negative_inf_ = 0;
		positive_inf_ = 0;
		total_count_  = 0;
		counts_len_   = conf.counts_len;

		counts_.reset(new int64_t[counts_len_]);
		std::fill(counts_.get(), counts_.get() + counts_len_, 0);
	}

	hdr_histogram_t(hdr_histogram_t&& other) = delete; // FIXME: disallow move for now

public: // reads

	uint32_t negative_inf() const { return negative_inf_; }
	uint32_t positive_inf() const { return positive_inf_; }
	uint32_t total_count() const { return total_count_; }

	using counts_range_t = meow::string_ref<int64_t const>;

	counts_range_t get_counts_range() const
	{
		return counts_range_t { this->counts_.get(), this->counts_len_ };
	}

	uint64_t get_allocated_size() const
	{
		return sizeof(counts_range_t::value_type) * this->counts_len_;
	}

public:

	bool increment(config_t const& conf, int64_t value, uint32_t increment_by = 1)
	{
		// copy-pasted comment from histogram_t::increment on boundary inclusion
		//
		// buckets are open on the left, and closed on the right side, i.e. "(from, to]"
		// [negative_inf]   -> (-inf, min_value]
		// [0]              -> (min_value, min_value+bucket_d]
		// [1]              -> (min_value+bucket_d, min_value+bucket_d*2]
		// ....
		// [bucket_count-1] -> (min_value+bucket_d*(bucket_count-1), min_value+bucket_d*bucket_count]
		// [positive_inf]   -> (max_value, +inf)

		if (__builtin_expect(value < conf.lowest_trackable_value, 0))
		{
			this->negative_inf_ += increment_by;
		}
		else if (__builtin_expect(value > conf.highest_trackable_value, 0))
		{
			this->positive_inf_ += increment_by;
		}
		else {
			int32_t const counts_index = counts_index_for(conf, value);
			this->counts_[counts_index] += increment_by;
		}

		this->total_count_ += increment_by;
		return true;
	}

public:

	static inline int64_t value_at_index(config_t const& conf, int32_t index)
	{
		int32_t bucket_index = (index >> conf.sub_bucket_half_count_magnitude) - 1;
		int32_t sub_bucket_index = (index & (conf.sub_bucket_half_count - 1)) + conf.sub_bucket_half_count;

		if (bucket_index < 0)
		{
			sub_bucket_index -= conf.sub_bucket_half_count;
			bucket_index = 0;
		}

		return value_from_index(bucket_index, sub_bucket_index, conf.unit_magnitude);
	}

	static inline int64_t size_of_equivalent_value_range(config_t const& conf, int64_t value)
	{
		int32_t const bucket_index     = get_bucket_index(conf, value);
		int32_t const sub_bucket_index = get_sub_bucket_index(value, bucket_index, conf.unit_magnitude);
		int32_t const adjusted_bucket  = (sub_bucket_index >= conf.sub_bucket_count) ? (bucket_index + 1) : bucket_index;
		return int64_t(1) << (conf.unit_magnitude + adjusted_bucket);
	}

	static inline int64_t next_non_equivalent_value(config_t const& conf, int64_t value)
	{
		return lowest_equivalent_value(conf, value) + size_of_equivalent_value_range(conf, value);
	}

	static inline int64_t lowest_equivalent_value(config_t const& conf, int64_t value)
	{
		int32_t const bucket_index     = get_bucket_index(conf, value);
		int32_t const sub_bucket_index = get_sub_bucket_index(value, bucket_index, conf.unit_magnitude);
		return value_from_index(bucket_index, sub_bucket_index, conf.unit_magnitude);
	}

	static inline int64_t highest_equivalent_value(config_t const& conf, int64_t value)
	{
		return next_non_equivalent_value(conf, value) - 1;
	}

public: // utilities

	static inline int64_t value_from_index(int32_t bucket_index, int32_t sub_bucket_index, int32_t unit_magnitude)
	{
		return ((int64_t) sub_bucket_index) << (bucket_index + unit_magnitude);
	}

	static inline int32_t get_bucket_index(config_t const& conf, int64_t value)
	{
		int32_t const pow2ceiling = 64 - __builtin_clzll(value | conf.sub_bucket_mask); // smallest power of 2 containing value
		return pow2ceiling - conf.unit_magnitude - (conf.sub_bucket_half_count_magnitude + 1);
	}

	static inline int32_t get_sub_bucket_index(int64_t value, int32_t bucket_index, int32_t unit_magnitude)
	{
		return (int32_t)(value >> (bucket_index + unit_magnitude));
	}

	static inline int32_t counts_index(config_t const& conf, int32_t bucket_index, int32_t sub_bucket_index)
	{
		// Calculate the index for the first entry in the bucket:
		// (The following is the equivalent of ((bucket_index + 1) * subBucketHalfCount) ):
		int32_t const bucket_base_index = (bucket_index + 1) << conf.sub_bucket_half_count_magnitude;
		// Calculate the offset in the bucket:
		int32_t const offset_in_bucket = sub_bucket_index - conf.sub_bucket_half_count;
		// The following is the equivalent of ((sub_bucket_index  - subBucketHalfCount) + bucketBaseIndex;
		return bucket_base_index + offset_in_bucket;
	}

	static inline int32_t counts_index_for(config_t const& conf, int64_t value)
	{
		int32_t const bucket_index     = get_bucket_index(conf, value);
		int32_t const sub_bucket_index = get_sub_bucket_index(value, bucket_index, conf.unit_magnitude);

		return counts_index(conf, bucket_index, sub_bucket_index);
	}

private:
	uint32_t   negative_inf_;
	uint32_t   positive_inf_;

	uint32_t   total_count_;
	uint32_t   counts_len_;

	std::unique_ptr<int64_t[]> counts_; // TODO: make type (64 or 32 bits) configureable here
};

// inline duration_t get_percentile(hdr_histogram_t const& hv, hdr_histogram_t::config_t const& conf, double percentile)
inline uint32_t get_percentile(hdr_histogram_t const& hv, hdr_histogram_t::config_t const& conf, double percentile)
{
	if (percentile == 0.)
		return conf.lowest_trackable_value;

	if (hv.total_count() == 0) // no values in histogram, nothing to do
		return conf.lowest_trackable_value;

	uint32_t required_sum = [&]()
	{
		uint32_t const res = std::ceil(hv.total_count() * percentile / 100.0);
		return (res > hv.total_count()) ? hv.total_count() : res;
	}();

	// ff::fmt(stdout, "{0}({1}); total: {2}, required: {3}\n", __func__, percentile, hv.total_count(), required_sum);

	// fastpath - are we in negative_inf?, <= here !
	if (required_sum <= hv.negative_inf())
		return conf.lowest_trackable_value;

	// fastpath - are we going to hit positive_inf?
	if (required_sum > (hv.total_count() - hv.positive_inf()))
		return conf.highest_trackable_value;

	// already past negative_inf, adjust
	required_sum -= hv.negative_inf();


	// slowpath - shut up and calculate
	uint32_t current_sum = 0;

	// for (uint32_t i = 0; i < hv.counts_len_; i++)
	auto const counts_r = hv.get_counts_range();

	for (uint32_t i = 0; i < counts_r.size(); i++)
	{
		uint32_t const bucket_id       = i;
		uint32_t const next_has_values = counts_r[i];
		uint32_t const need_values     = required_sum - current_sum;

		if (next_has_values < need_values) // take bucket and move on
		{
			current_sum += next_has_values;
			// ff::fmt(stdout, "[{0}] current_sum +=; {1} -> {2}\n", bucket_id, next_has_values, current_sum);
			continue;
		}

		if (next_has_values == need_values) // complete bucket, return upper time bound for this bucket
		{
			// ff::fmt(stdout, "[{0}] full; current_sum +=; {1} -> {2}\n", bucket_id, next_has_values, current_sum);
			// return conf.min_value + conf.bucket_d * (bucket_id + 1);
			auto const result = hv.highest_equivalent_value(conf, hv.value_at_index(conf, bucket_id));
			return (result < conf.highest_trackable_value)
					? result
					: conf.highest_trackable_value;
		}

		// incomplete bucket, but still take upper bound, to be like hdr_histogram
		// {
		// 	return hv.highest_equivalent_value(conf, hv.value_at_index(conf, bucket_id));
		// }

		// incomplete bucket, interpolate, assuming flat time distribution within bucket
		{
			int64_t const d = hv.size_of_equivalent_value_range(conf, bucket_id) * need_values / next_has_values;

			// ff::fmt(stdout, "[{0}] last, has: {1}, taking: {2}, {3}\n", bucket_id, next_has_values, need_values, d);
			auto const result = hv.lowest_equivalent_value(conf, hv.value_at_index(conf, bucket_id)) + d;
			return (result < conf.highest_trackable_value)
					? result
					: conf.highest_trackable_value;
		}
	}

	// dump hv contents to stderr, as we're going to die anyway
	// {
	// 	ff::fmt(stderr, "{0} internal failure, dumping histogram\n", __func__);
	// 	ff::fmt(stderr, "{0} neg_inf: {1}, pos_inf: {2}, value_count: {3}, hv_size: {4}\n",
	// 		__func__, hv.negative_inf, hv.positive_inf, hv.value_count, hv.values.size());

	// 	for (auto const& item : hv.values)
	// 		ff::fmt(stderr, "[{0}] -> {1}\n", item.bucket_id, item.value);
	// }

	assert(!"must not be reached");
}

////////////////////////////////////////////////////////////////////////////////////////////////

int hdr_calculate_bucket_config___sig_bits(
		int64_t lowest_trackable_value,
		int64_t highest_trackable_value,
		int significant_bits,
		struct hdr_histogram_bucket_config* cfg)
{
	if (lowest_trackable_value < 1 ||
			significant_bits < 1 || 16 < significant_bits)
	{
		return EINVAL;
	}
	else if (lowest_trackable_value * 2 > highest_trackable_value)
	{
		return EINVAL;
	}

	cfg->lowest_trackable_value = lowest_trackable_value;
	cfg->significant_figures = significant_bits;
	cfg->highest_trackable_value = highest_trackable_value;

	// int64_t largest_value_with_single_unit_resolution = 2 * power(10, significant_figures);
	int64_t largest_value_with_single_unit_resolution = 2 * (1 << significant_bits);
	int32_t sub_bucket_count_magnitude = (int32_t) ceil(log((double)largest_value_with_single_unit_resolution) / log(2));
	cfg->sub_bucket_half_count_magnitude = ((sub_bucket_count_magnitude > 1) ? sub_bucket_count_magnitude : 1) - 1;

	cfg->unit_magnitude = (int32_t) floor(log((double)lowest_trackable_value) / log(2));

	cfg->sub_bucket_count      = (int32_t) pow(2, (cfg->sub_bucket_half_count_magnitude + 1));
	cfg->sub_bucket_half_count = cfg->sub_bucket_count / 2;
	cfg->sub_bucket_mask       = ((int64_t) cfg->sub_bucket_count - 1) << cfg->unit_magnitude;

	// determine exponent range needed to support the trackable value with no overflow:
	cfg->bucket_count = hdr___buckets_needed_to_cover_value(highest_trackable_value, cfg->sub_bucket_count, (int32_t)cfg->unit_magnitude);
	cfg->counts_len = (cfg->bucket_count + 1) * (cfg->sub_bucket_count / 2);

	return 0;
}

////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char const *argv[])
{
	constexpr size_t n_iterations = 1 * 1000 * 1000;

	double hash_d = 1.0;
	double hdr_d;
	double flat_d;
	double seq_d;
	double rnd_d;

	histogram_conf_t hv_conf = {
		.bucket_count = 60000000,
		.bucket_d     = 1 * d_millisecond,
		.min_value    = 0 * d_millisecond,
	};

	{
		histogram_t hv;

		meow::stopwatch_t sw;
		srandom(sw.now().tv_nsec);

		for (size_t i = 0; i < n_iterations; i++)
		{
			// hv.increment(hv_conf, (random() % hv_conf.bucket_count) * d_millisecond);
			hv.increment(hv_conf, ((uint32_t)i % hv_conf.bucket_count) * d_millisecond);
		}

		hash_d = timeval_to_double(sw.stamp());
		ff::fmt(stdout, "hash: added {0} values, elapsed: {1}, \t{2} inserts/sec, mem: {3}\n"
			, n_iterations, hash_d, (double)n_iterations / hash_d, hv.map_cref().bucket_count() * sizeof(hv.map_cref().begin()));

		ff::fmt(stdout, "  p50: {0}\n", get_percentile(hv, hv_conf, 50));
		ff::fmt(stdout, "  p75: {0}\n", get_percentile(hv, hv_conf, 75));
		ff::fmt(stdout, "  p95: {0}\n", get_percentile(hv, hv_conf, 95));
		ff::fmt(stdout, "  p99: {0}\n", get_percentile(hv, hv_conf, 99));
		ff::fmt(stdout, "  p100: {0}\n", get_percentile(hv, hv_conf, 100));

		auto const flat_hv = histogram___convert_ht_to_flat(hv);
		ff::fmt(stdout, "[flat_hv]\n");
		ff::fmt(stdout, "  p50: {0}\n", get_percentile(flat_hv, hv_conf, 50));
		ff::fmt(stdout, "  p75: {0}\n", get_percentile(flat_hv, hv_conf, 75));
		ff::fmt(stdout, "  p95: {0}\n", get_percentile(flat_hv, hv_conf, 95));
		ff::fmt(stdout, "  p99: {0}\n", get_percentile(flat_hv, hv_conf, 99));
		ff::fmt(stdout, "  p100: {0}\n", get_percentile(flat_hv, hv_conf, 100));

	}

	struct hdr_histogram_bucket_config cfg;

	{
		int r = hdr_calculate_bucket_config(1, hv_conf.bucket_count, 2, &cfg);
		// int r = hdr_calculate_bucket_config___sig_bits(1, hv_conf.bucket_count, 9, &cfg);
		if (0 != r)
			throw std::runtime_error(ff::fmt_str("hdr_calculate_bucket_config error: {0}", r));
	}

	{
		using hdr_histogram_t = struct hdr_histogram;
		hdr_histogram_t *h;

		{
			{
				size_t histogram_size = sizeof(struct hdr_histogram) + cfg.counts_len * sizeof(int64_t);
				h                     = (struct hdr_histogram*)malloc(histogram_size);

			    if (!h)
			    	throw std::runtime_error(ff::fmt_str("hdr_init error: {0}", ENOMEM));

			    // memset will ensure that all of the function pointers are null.
			    memset((void*) h, 0, histogram_size);

			    hdr_init_preallocated(h, &cfg);
			}

			// int const r = hdr_init(1, hv_conf.bucket_count, 2, &h);
			// if (0 != r)
			// 	throw std::runtime_error(ff::fmt_str("hdr_init error: {0}", r));

			ff::fmt(stdout, "lowest_trackable_value          = {0}\n", h->lowest_trackable_value);
			ff::fmt(stdout, "highest_trackable_value         = {0}\n", h->highest_trackable_value);
			ff::fmt(stdout, "unit_magnitude                  = {0}\n", h->unit_magnitude);
			ff::fmt(stdout, "significant_figures             = {0}\n", h->significant_figures);
			ff::fmt(stdout, "sub_bucket_half_count_magnitude = {0}\n", h->sub_bucket_half_count_magnitude);
			ff::fmt(stdout, "sub_bucket_half_count           = {0}\n", h->sub_bucket_half_count);
			ff::fmt(stdout, "sub_bucket_mask                 = {0}\n", h->sub_bucket_mask);
			ff::fmt(stdout, "sub_bucket_count                = {0}\n", h->sub_bucket_count);
			ff::fmt(stdout, "bucket_count                    = {0}\n", h->bucket_count);
			ff::fmt(stdout, "counts_len                      = {0}\n", h->counts_len);
		}

		meow::stopwatch_t sw;
		srandom(sw.now().tv_nsec);

		size_t failed_count = 0;

		for (size_t i = 0; i < n_iterations; i++)
		{
			// hdr_record_values(h, (random() % hv_conf.bucket_count), 1);
			bool const ok = hdr_record_values(h, (i % (hv_conf.bucket_count + 1)), 1);
			if (!ok)
				failed_count++;
		}

		auto const hdr_d = timeval_to_double(sw.stamp());
		ff::fmt(stdout, "hdr: added {0} values, elapsed: {1}, \t{2} inserts/sec. speedup: {3}, mem: {4}, failed: {5}\n"
			, n_iterations, hdr_d, (double)n_iterations / hdr_d, hash_d / hdr_d, hdr_get_memory_size(h), failed_count);

		// range [1 - 60000], precision: 2, total: 1*256 + 8*128 buckets = 1280
		// lowest_trackable_value          = 1
		// highest_trackable_value         = 60000
		// unit_magnitude                  = 0
		// significant_figures             = 2
		// sub_bucket_half_count_magnitude = 7
		// sub_bucket_half_count           = 128
		// sub_bucket_mask                 = 255
		// sub_bucket_count                = 256
		// bucket_count                    = 9
		// counts_len                      = 1280
		// [1, 256)       -> 1    (0 bits) // 256
		// [256, 512)     -> 2    (1)      // 128
		// [512, 1024)    -> 4    (2)      // 128
		// [1024, 2048)   -> 8    (3)      // 128
		// [2048, 4096)   -> 16   (4)      // 128
		// [4096, 8192)   -> 32   (5)      // 128
		// [8192, 16384)  -> 64   (6)      // 128
		// [16384, 32768) -> 128  (7)      // 128
		// [32768, 65536) -> 256  (8)      // 128
		//
		// range [1 - 60000], precision: 3, total: 1*2048 + 5*1024 = 7168
		// lowest_trackable_value          = 1
		// highest_trackable_value         = 60000
		// unit_magnitude                  = 0
		// significant_figures             = 3
		// sub_bucket_half_count_magnitude = 10
		// sub_bucket_half_count           = 1024
		// sub_bucket_mask                 = 2047
		// sub_bucket_count                = 2048
		// bucket_count                    = 6
		// counts_len                      = 7168
		// [1, 2048)       -> 1   (0 bits) // 2048
		// [2048, 4096)    -> 2   (1)      // 1024
		// [4096, 8192)    -> 4   (2)      // 1024
		// [8192, 16384)   -> 8   (3)      // 1024
		// [16384, 32768)  -> 16  (4)      // 1024
		// [32768, 65536)  -> 32  (5)      // 1024
		// for (size_t i = 0; i < hv_conf.bucket_count; /**/)
		// {
		// 	ff::fmt(stdout, "{0} -> {1} ({2})\n", i, hdr_next_non_equivalent_value(h, i), hdr_size_of_equivalent_value_range(h, i));
		// 	i = hdr_next_non_equivalent_value(h, i);
		// }

		ff::fmt(stdout, "  p50: {0}\n", hdr_value_at_percentile(h, 50));
		ff::fmt(stdout, "  p75: {0}\n", hdr_value_at_percentile(h, 75));
		ff::fmt(stdout, "  p95: {0}\n", hdr_value_at_percentile(h, 95));
		ff::fmt(stdout, "  p99: {0}\n", hdr_value_at_percentile(h, 99));
		ff::fmt(stdout, "  p100: {0}\n", hdr_value_at_percentile(h, 100));

		// iterate plain values, for hdr -> flat conversion
		// {
		// 	struct hdr_iter it;
		// 	hdr_iter_recorded_init(&it, h);

		// 	while (hdr_iter_next(&it))
		// 	{
		// 		ff::fmt(stdout, "[{0}, {1}) -> {2}\n", it.lowest_equivalent_value, it.highest_equivalent_value, it.count);
		// 	}
		// }
	}

	{
		hdr_histogram_t::config_t conf = {
			.sub_bucket_half_count_magnitude = cfg.sub_bucket_half_count_magnitude,
			.sub_bucket_half_count           = cfg.sub_bucket_half_count,
			.sub_bucket_count                = cfg.sub_bucket_count,
			.counts_len                      = cfg.counts_len,
			.unit_magnitude                  = cfg.unit_magnitude,
			.sub_bucket_mask                 = cfg.sub_bucket_mask,
			.lowest_trackable_value          = cfg.lowest_trackable_value,
			.highest_trackable_value         = cfg.highest_trackable_value,
			.significant_figures             = cfg.significant_figures,
			.bucket_count                    = cfg.bucket_count,
		};

		hdr_histogram_t hv(conf);

		meow::stopwatch_t sw;
		srandom(sw.now().tv_nsec);

		size_t failed_count = 0;

		for (size_t i = 0; i < n_iterations; i++)
		{
			// hdr_record_values(h, (random() % hv_conf.bucket_count), 1);
			bool const ok = hv.increment(conf, (i % (hv_conf.bucket_count + 1)), 1);
			if (!ok)
				failed_count++;
		}

		auto const hdr_hv_d = timeval_to_double(sw.stamp());
		ff::fmt(stdout, "hdr_hv: added {0} values, elapsed: {1}, \t{2} inserts/sec. speedup: {3}, mem: {4}, failed: {5}\n"
			, n_iterations, hdr_hv_d, (double)n_iterations / hdr_hv_d, hash_d / hdr_hv_d, hv.get_allocated_size(), failed_count);

		ff::fmt(stdout, "  p50: {0}\n", get_percentile(hv, conf, 50));
		ff::fmt(stdout, "  p75: {0}\n", get_percentile(hv, conf, 75));
		ff::fmt(stdout, "  p95: {0}\n", get_percentile(hv, conf, 95));
		ff::fmt(stdout, "  p99: {0}\n", get_percentile(hv, conf, 99));
		ff::fmt(stdout, "  p100: {0}\n", get_percentile(hv, conf, 100));
	}

	// {
	// 	meow::stopwatch_t sw;
	// 	srandom(sw.now().tv_nsec);

	// 	uint32_t h[hv_conf.bucket_count];

	// 	for (size_t i = 0; i < n_iterations; i++)
	// 	{
	// 		h[(random() % hv_conf.bucket_count)] += 1;
	// 	}

	// 	auto const flat_d = timeval_to_double(sw.stamp());
	// 	ff::fmt(stdout, "flat: added {0} values, elapsed: {1}, \t{2} inserts/sec. speedup: {3}, mem: {4}\n"
	// 		, n_iterations, flat_d, (double)n_iterations / flat_d, hash_d / flat_d, sizeof(h));
	// }

	// {
	// 	meow::stopwatch_t sw;

	// 	uint32_t h[hv_conf.bucket_count];

	// 	for (size_t i = 0; i < n_iterations; i++)
	// 	{
	// 		h[(i % hv_conf.bucket_count)] += 1;
	// 	}

	// 	auto const seq_d = timeval_to_double(sw.stamp());
	// 	ff::fmt(stdout, "seq: added {0} values, elapsed: {1}, \t{2} inserts/sec. speedup: {3}, mem: {4}\n"
	// 		, n_iterations, seq_d, (double)n_iterations / seq_d, hash_d / seq_d, sizeof(h));
	// }

	// {
	// 	meow::stopwatch_t sw;
	// 	srandom(sw.now().tv_nsec);

	// 	uint32_t n = 0;

	// 	for (size_t i = 0; i < n_iterations; i++)
	// 	{
	// 		n = random();
	// 	}

	// 	auto const rnd_d = timeval_to_double(sw.stamp());
	// 	ff::fmt(stdout, "rnd: random for {0} values, elapsed: {1}, \t{2} calls/sec. speedup: {3}\n"
	// 		, n_iterations, rnd_d, (double)n_iterations / rnd_d, hash_d / rnd_d);
	// }

	return 0;
}
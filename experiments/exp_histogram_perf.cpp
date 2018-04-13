#include <vector>

#include <meow/stopwatch.hpp>

#include "pinba/globals.h"
#include "pinba/histogram.h"

// #include "hdr_histogram/hdr_histogram.h"

struct hdr_histogram
{
    int64_t lowest_trackable_value;
    int64_t highest_trackable_value;
    int32_t unit_magnitude;
    int32_t significant_figures;
    int32_t sub_bucket_half_count_magnitude;
    int32_t sub_bucket_half_count;
    int64_t sub_bucket_mask;
    int32_t sub_bucket_count;
    int32_t bucket_count;
    int64_t min_value;
    int64_t max_value;
    int32_t normalizing_index_offset;
    double conversion_ratio;
    int32_t counts_len;
    int64_t total_count;
    int64_t counts[0];
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


static int64_t power(int64_t base, int64_t exp)
{
    int64_t result = 1;
    while(exp)
    {
        result *= base; exp--;
    }
    return result;
}

static int32_t get_bucket_index(const struct hdr_histogram* h, int64_t value)
{
    int32_t pow2ceiling = 64 - __builtin_clzll(value | h->sub_bucket_mask); // smallest power of 2 containing value
    return pow2ceiling - h->unit_magnitude - (h->sub_bucket_half_count_magnitude + 1);
}

static int32_t get_sub_bucket_index(int64_t value, int32_t bucket_index, int32_t unit_magnitude)
{
    return (int32_t)(value >> (bucket_index + unit_magnitude));
}

static int32_t normalize_index(const struct hdr_histogram* h, int32_t index)
{
    if (h->normalizing_index_offset == 0)
    {
        return index;
    }

    int32_t normalized_index = index - h->normalizing_index_offset;
    int32_t adjustment = 0;

    if (normalized_index < 0)
    {
        adjustment = h->counts_len;
    }
    else if (normalized_index >= h->counts_len)
    {
        adjustment = -h->counts_len;
    }

    return normalized_index + adjustment;
}

static int32_t counts_index(const struct hdr_histogram* h, int32_t bucket_index, int32_t sub_bucket_index)
{
    // Calculate the index for the first entry in the bucket:
    // (The following is the equivalent of ((bucket_index + 1) * subBucketHalfCount) ):
    int32_t bucket_base_index = (bucket_index + 1) << h->sub_bucket_half_count_magnitude;
    // Calculate the offset in the bucket:
    int32_t offset_in_bucket = sub_bucket_index - h->sub_bucket_half_count;
    // The following is the equivalent of ((sub_bucket_index  - subBucketHalfCount) + bucketBaseIndex;
    return bucket_base_index + offset_in_bucket;
}

int32_t counts_index_for(const struct hdr_histogram* h, int64_t value)
{
    int32_t bucket_index     = get_bucket_index(h, value);
    int32_t sub_bucket_index = get_sub_bucket_index(value, bucket_index, h->unit_magnitude);

    return counts_index(h, bucket_index, sub_bucket_index);
}

static void counts_inc_normalised(
    struct hdr_histogram* h, int32_t index, int64_t value)
{
    int32_t normalised_index = normalize_index(h, index);
    h->counts[normalised_index] += value;
    h->total_count += value;
}

static void update_min_max(struct hdr_histogram* h, int64_t value)
{
    h->min_value = (value < h->min_value && value != 0) ? value : h->min_value;
    h->max_value = (value > h->max_value) ? value : h->max_value;
}


static int32_t buckets_needed_to_cover_value(int64_t value, int32_t sub_bucket_count, int32_t unit_magnitude)
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

bool hdr_record_values(struct hdr_histogram* h, int64_t value, int64_t count)
{
    if (value < 0)
    {
        return false;
    }

    int32_t counts_index = counts_index_for(h, value);

    if (counts_index < 0 || h->counts_len <= counts_index)
    {
        return false;
    }

    counts_inc_normalised(h, counts_index, count);
    update_min_max(h, value);

    return true;
}

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
    h->min_value                       = INT64_MAX;
    h->max_value                       = 0;
    h->normalizing_index_offset        = 0;
    h->conversion_ratio                = 1.0;
    h->bucket_count                    = cfg->bucket_count;
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
    struct hdr_histogram* histogram = (struct hdr_histogram*)malloc(histogram_size);

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


size_t hdr_get_memory_size(struct hdr_histogram *h)
{
    return sizeof(struct hdr_histogram) + h->counts_len * sizeof(int64_t);
}


////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char const *argv[])
{
	constexpr size_t n_iterations = 5 * 1000 * 1000;

	double hash_d;
	double hdr_d;
	double flat_d;

	{
		histogram_t hv;

		histogram_conf_t hv_conf = {
			.bucket_count = 10000,
			.bucket_d     = 1 * d_millisecond,
			.min_value    = 0 * d_millisecond,
		};

		meow::stopwatch_t sw;
		srandom(sw.now().tv_nsec);

		for (size_t i = 0; i < n_iterations; i++)
		{
			hv.increment(hv_conf, (random() % hv_conf.bucket_count) * d_millisecond);
			// hv.increment(hv_conf, (random() % 10000) * d_millisecond);
		}

		hash_d = timeval_to_double(sw.stamp());
		ff::fmt(stdout, "hash: added {0} values, elapsed: {1}, \t{2} inserts/sec, mem: {3}\n"
			, n_iterations, hash_d, (double)n_iterations / hash_d, hv.map_cref().bucket_count() * sizeof(hv.map_cref().begin()));
	}

	{
		using hdr_histogram_t = struct hdr_histogram;
		hdr_histogram_t *h;
		{
			int const r = hdr_init(1, 10000, 3, &h);
			if (0 != r)
				throw std::runtime_error(ff::fmt_str("hdr_init error: {0}", r));
		}

		meow::stopwatch_t sw;
		srandom(sw.now().tv_nsec);

		for (size_t i = 0; i < n_iterations; i++)
		{
			hdr_record_values(h, (random() % 10000), 1);
		}

		auto const hdr_d = timeval_to_double(sw.stamp());
		ff::fmt(stdout, "hdr: added {0} values, elapsed: {1}, \t{2} inserts/sec. speedup: {3}, mem: {4}\n"
			, n_iterations, hdr_d, (double)n_iterations / hdr_d, hash_d / hdr_d, hdr_get_memory_size(h));
	}

	{
		meow::stopwatch_t sw;
		srandom(sw.now().tv_nsec);

		uint32_t h[10000];

		for (size_t i = 0; i < n_iterations; i++)
		{
			h[(random() % 10000)] += 1;
		}

		auto const flat_d = timeval_to_double(sw.stamp());
		ff::fmt(stdout, "hdr: added {0} values, elapsed: {1}, \t{2} inserts/sec. speedup: {3}, mem: {4}\n"
			, n_iterations, flat_d, (double)n_iterations / flat_d, hash_d / flat_d, sizeof(h));
	}

	return 0;
}
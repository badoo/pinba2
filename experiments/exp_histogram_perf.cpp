#include <vector>
#include <limits>
#include <algorithm> // fill

#include <meow/stopwatch.hpp>
#include <meow/error.hpp>

#include "pinba/globals.h"

// #include "hdr_histogram/hdr_histogram.h"

////////////////////////////////////////////////////////////////////////////////////////////////

// values at [1, 600sec] range, 1 microsecond resolution, 16 significant bits
// hv.lowest_trackable_value          = 1
// hv.highest_trackable_value         = 600000000
// hv.unit_magnitude                  = 0
// hv.significant_bits                = 16
// hv.sub_bucket_half_count_magnitude = 16
// hv.sub_bucket_half_count           = 65536
// hv.sub_bucket_mask                 = 131071
// hv.sub_bucket_count                = 131072
// hv.bucket_count                    = 14
// hv.counts_len                      = 983040
//
// same at 15 bits resolution
// hv.lowest_trackable_value          = 1
// hv.highest_trackable_value         = 600000000
// hv.unit_magnitude                  = 0
// hv.significant_bits                = 15
// hv.sub_bucket_half_count_magnitude = 15
// hv.sub_bucket_half_count           = 32768
// hv.sub_bucket_mask                 = 65535
// hv.sub_bucket_count                = 65536
// hv.bucket_count                    = 15
// hv.counts_len                      = 524288
//
// same at 14 bits resolution (4 decimal points, 2^14 = 16384)
// hv.lowest_trackable_value          = 1
// hv.highest_trackable_value         = 600000000
// hv.unit_magnitude                  = 0
// hv.significant_bits                = 14
// hv.sub_bucket_half_count_magnitude = 14
// hv.sub_bucket_half_count           = 16384
// hv.sub_bucket_mask                 = 32767
// hv.sub_bucket_count                = 32768
// hv.bucket_count                    = 16
// hv.counts_len                      = 278528
struct hdr_histogram_conf_t
{
	// internal bits
	uint16_t sub_bucket_count;
	uint16_t sub_bucket_half_count;
	uint16_t sub_bucket_mask;
	uint8_t unit_magnitude;
	uint8_t sub_bucket_half_count_magnitude;

	// internal bits (old)
	// int32_t sub_bucket_half_count_magnitude;
	// int32_t sub_bucket_half_count;
	// int32_t sub_bucket_count;
	// int32_t counts_len;
	// int64_t unit_magnitude;
	// int64_t sub_bucket_mask;

	// information only
	int64_t lowest_trackable_value;
	int64_t highest_trackable_value;
	int64_t significant_bits;
	int32_t bucket_count;
	int32_t counts_len;
};

static int32_t hdr___buckets_needed_to_cover_value(int64_t value, int32_t sub_bucket_count, int32_t unit_magnitude)
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

static int64_t hdr___int_power(int64_t base, int64_t exp)
{
	int64_t result = 1;
	while(exp)
	{
		result *= base;
		exp--;
	}
	return result;
};

meow::error_t hdr_histogram_configure(
		hdr_histogram_conf_t *cfg,
		int64_t lowest_trackable_value,
		int64_t highest_trackable_value,
		int significant_bits)
{
	if (lowest_trackable_value <= 0)
	{
		return ff::fmt_err("lowest_trackable_value must be > 0, {0} given", lowest_trackable_value);
	}

	if (lowest_trackable_value * 2 > highest_trackable_value)
	{
		return ff::fmt_err("lowest_trackable_value * 2 must be <= highest_trackable_value, {0}*2 > {1}",
			lowest_trackable_value, highest_trackable_value);
	}

	if (significant_bits < 1 || significant_bits > 14)
	{
		return ff::fmt_err("significant_bits must be in range [1, 14], {0} given", significant_bits);
	}

	cfg->lowest_trackable_value = lowest_trackable_value;
	cfg->significant_bits = significant_bits;
	cfg->highest_trackable_value = highest_trackable_value;

	int64_t const largest_value_with_single_unit_resolution = 2 * (1 << significant_bits);

	int32_t const sub_bucket_count_magnitude = (int32_t) ceil(log((double)largest_value_with_single_unit_resolution) / log(2));
	assert(sub_bucket_count_magnitude < UINT8_MAX);
	cfg->sub_bucket_half_count_magnitude = (uint8_t)((sub_bucket_count_magnitude > 1) ? sub_bucket_count_magnitude : 1) - 1;

	int64_t const unit_magnitude = (int32_t) floor(log((double)lowest_trackable_value) / log(2));
	assert(unit_magnitude < UINT8_MAX);
	cfg->unit_magnitude = (uint8_t)unit_magnitude;

	int32_t const sub_bucket_count      = (int32_t) pow(2, (cfg->sub_bucket_half_count_magnitude + 1));
	assert(sub_bucket_count < UINT16_MAX);
	cfg->sub_bucket_count = (uint16_t)sub_bucket_count;
	cfg->sub_bucket_half_count = cfg->sub_bucket_count / 2;
	cfg->sub_bucket_mask       = ((int16_t) cfg->sub_bucket_count - 1) << cfg->unit_magnitude;

	// determine exponent range needed to support the trackable value with no overflow:
	cfg->bucket_count = hdr___buckets_needed_to_cover_value(highest_trackable_value, cfg->sub_bucket_count, (int32_t)cfg->unit_magnitude);
	cfg->counts_len = (cfg->bucket_count + 1) * (cfg->sub_bucket_count / 2);

	return {};
}

meow::error_t hdr_histogram_configure___sig_figures(
		hdr_histogram_conf_t* cfg,
		int64_t lowest_trackable_value,
		int64_t highest_trackable_value,
		int significant_figures)
{
	// smallerst power of 2, to cover given power of 10
	int const sig_bits = 64 - __builtin_clzll(hdr___int_power(10, significant_figures));
	return hdr_histogram_configure(cfg, lowest_trackable_value, highest_trackable_value, sig_bits);
}


template<class CounterT>
struct hdr_histogram_t
{
	using self_t    = hdr_histogram_t;
	using counter_t = CounterT;
	using config_t  = hdr_histogram_conf_t;

	static_assert(std::is_integral<counter_t>::value, "T must be an integral type");

public:

	hdr_histogram_t(config_t const& conf)
	{
		negative_inf_                   = 0;
		positive_inf_                   = 0;
		total_count_                    = 0;
		counts_len_                     = conf.counts_len;

		sub_bucket_count                = conf.sub_bucket_count;
		sub_bucket_half_count           = conf.sub_bucket_half_count;
		sub_bucket_mask                 = conf.sub_bucket_mask;
		unit_magnitude                  = conf.unit_magnitude;
		sub_bucket_half_count_magnitude = conf.sub_bucket_half_count_magnitude;

		counts_.reset(new counter_t[counts_len_]);
		std::fill(counts_.get(), counts_.get() + counts_len_, 0);
	}

	hdr_histogram_t(hdr_histogram_t const& other)
	{
		negative_inf_                   = other.negative_inf_;
		positive_inf_                   = other.positive_inf_;
		total_count_                    = other.total_count_;
		counts_len_                     = other.counts_len_;

		sub_bucket_count                = other.sub_bucket_count;
		sub_bucket_half_count           = other.sub_bucket_half_count;
		sub_bucket_mask                 = other.sub_bucket_mask;
		unit_magnitude                  = other.unit_magnitude;
		sub_bucket_half_count_magnitude = other.sub_bucket_half_count_magnitude;

		counts_.reset(new counter_t[counts_len_]);
		std::copy(other.counts_.get(), other.counts_.get() + counts_len_, counts_.get());
	}

	hdr_histogram_t(hdr_histogram_t&& other)
	{
		negative_inf_                   = other.negative_inf_;
		positive_inf_                   = other.positive_inf_;
		total_count_                    = other.total_count_;
		counts_len_                     = other.counts_len_;

		sub_bucket_count                = other.sub_bucket_count;
		sub_bucket_half_count           = other.sub_bucket_half_count;
		sub_bucket_mask                 = other.sub_bucket_mask;
		unit_magnitude                  = other.unit_magnitude;
		sub_bucket_half_count_magnitude = other.sub_bucket_half_count_magnitude;

		counts_ = std::move(other.counts_);
	}

public: // reads

	uint64_t negative_inf() const { return negative_inf_; }
	uint64_t positive_inf() const { return positive_inf_; }
	uint64_t total_count() const { return total_count_; }

	using counts_range_t    = meow::string_ref<counter_t const>;
	using counts_range_nc_t = meow::string_ref<counter_t>;

	counts_range_t get_counts_range() const
	{
		return { this->counts_.get(), this->counts_len_ };
	}

	counts_range_nc_t mutable_counts_range()
	{
		return { this->counts_.get(), this->counts_len_ };
	}

	uint64_t get_allocated_size() const
	{
		return sizeof(counter_t) * this->counts_len_;
	}

public:

	bool increment(config_t const& conf, int64_t value, counter_t increment_by = 1)
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
			int32_t const counts_index = counts_index_for(value);
			assert((counts_index >= 0) && ((uint32_t)counts_index < this->counts_len_));
			this->counts_[counts_index] += increment_by;
		}

		this->total_count_ += increment_by;
		return true;
	}

	void merge_other_with_same_conf(hdr_histogram_t const& other, config_t const& conf)
	{
		assert(this->counts_len_ == other.counts_len_);

		for (uint32_t i = 0; i < other.counts_len_; i++)
		{
			this->counts_[i] += other.counts_[i];
		}

		this->negative_inf_ += other.negative_inf_;
		this->positive_inf_ += other.positive_inf_;
		this->total_count_ += other.total_count_;
	}

public:

	inline int64_t get_percentile(config_t const& conf, double percentile) const
	{
		if (percentile == 0.)
			return conf.lowest_trackable_value;

		if (this->total_count() == 0) // no values in histogram, nothing to do
			return conf.lowest_trackable_value;

		uint64_t required_sum = [&]()
		{
			uint64_t const res = std::ceil(this->total_count() * percentile / 100.0);
			uint64_t const total = (uint64_t)this->total_count();
			return (res > total) ? total : res;
		}();

		// ff::fmt(stderr, "{0}({1}); neg_inf: {2}, pos_inf: {3}\n", __func__, percentile, this->negative_inf(), this->positive_inf());
		// ff::fmt(stderr, "{0}({1}); total: {2}, required: {3}\n", __func__, percentile, this->total_count(), required_sum);

		// fastpath - are we in negative_inf?, <= here !
		if (required_sum <= (uint64_t)this->negative_inf())
			return conf.lowest_trackable_value;

		// fastpath - are we going to hit positive_inf?
		if (required_sum > (uint64_t)(this->total_count() - this->positive_inf()))
			return conf.highest_trackable_value;

		// already past negative_inf, adjust
		required_sum -= this->negative_inf();


		// slowpath - shut up and calculate
		uint64_t current_sum = 0;

		auto const counts_r = this->get_counts_range();

		for (uint32_t i = 0; i < counts_r.size(); i++)
		{
			uint32_t const bucket_id       = i;
			uint64_t const next_has_values = counts_r[i];
			uint64_t const need_values     = required_sum - current_sum;

			if (next_has_values < need_values) // take bucket and move on
			{
				current_sum += next_has_values;
				// ff::fmt(stderr, "[{0}] current_sum +=; {1} -> {2}\n", bucket_id, next_has_values, current_sum);
				continue;
			}

			if (next_has_values == need_values) // complete bucket, return upper time bound for this bucket
			{
				// ff::fmt(stderr, "[{0}] full; current_sum +=; {1} -> {2}\n", bucket_id, next_has_values, current_sum);
				int64_t const result = this->highest_equivalent_value(this->value_at_index(bucket_id));
				return (result < conf.highest_trackable_value)
						? result
						: conf.highest_trackable_value;
			}

			// incomplete bucket, interpolate, assuming flat time distribution within bucket
			{
				int64_t const d = this->size_of_equivalent_value_range(bucket_id) * need_values / next_has_values;

				// ff::fmt(stderr, "[{0}] last, has: {1}, taking: {2}, {3}\n", bucket_id, next_has_values, need_values, d);
				int64_t const result = this->lowest_equivalent_value(this->value_at_index(bucket_id)) + d;
				return (result < conf.highest_trackable_value)
						? result
						: conf.highest_trackable_value;
			}
		}

		// dump hv contents to stderr, as we're going to die anyway
		{
			ff::fmt(stderr, "{0} internal failure, dumping histogram\n", __func__);
			ff::fmt(stderr, "{0} neg_inf: {1}, pos_inf: {2}, total_count: {3}, hv_size: {4}\n",
				__func__, this->negative_inf(), this->positive_inf(), this->total_count(), this->counts_len_);

			for (uint32_t i = 0; i < counts_r.size(); i++)
				ff::fmt(stderr, "[{0}] -> {1}\n", this->value_at_index(i), this->count_at_index(i));
		}

		assert(!"must not be reached");
	}

public:

	inline counter_t count_at_index(int32_t index) const
	{
		return this->counts_[index];
	}

	inline int64_t value_at_index(int32_t index) const
	{
		int32_t bucket_index = (index >> this->sub_bucket_half_count_magnitude) - 1;
		int32_t sub_bucket_index = (index & (this->sub_bucket_half_count - 1)) + this->sub_bucket_half_count;

		if (bucket_index < 0)
		{
			sub_bucket_index -= this->sub_bucket_half_count;
			bucket_index = 0;
		}

		return value_from_index(bucket_index, sub_bucket_index, this->unit_magnitude);
	}

	inline int64_t size_of_equivalent_value_range(int64_t value) const
	{
		int32_t const bucket_index     = get_bucket_index(value);
		int32_t const sub_bucket_index = get_sub_bucket_index(value, bucket_index, this->unit_magnitude);
		int32_t const adjusted_bucket  = (sub_bucket_index >= this->sub_bucket_count) ? (bucket_index + 1) : bucket_index;
		return int64_t(1) << (this->unit_magnitude + adjusted_bucket);
	}

	inline int64_t next_non_equivalent_value(int64_t value) const
	{
		return lowest_equivalent_value(value) + size_of_equivalent_value_range(value);
	}

	inline int64_t lowest_equivalent_value(int64_t value) const
	{
		int32_t const bucket_index     = get_bucket_index(value);
		int32_t const sub_bucket_index = get_sub_bucket_index(value, bucket_index, this->unit_magnitude);
		return value_from_index(bucket_index, sub_bucket_index, this->unit_magnitude);
	}

	inline int64_t highest_equivalent_value(int64_t value) const
	{
		return next_non_equivalent_value(value) - 1;
	}

public: // utilities

	static inline int64_t value_from_index(int32_t bucket_index, int32_t sub_bucket_index, int32_t unit_magnitude)
	{
		return ((int64_t) sub_bucket_index) << (bucket_index + unit_magnitude);
	}

	static inline int32_t get_sub_bucket_index(int64_t value, int32_t bucket_index, int32_t unit_magnitude)
	{
		return (int32_t)(value >> (bucket_index + unit_magnitude));
	}

	inline int32_t get_bucket_index(int64_t value) const
	{
		// get smallest power of 2 containing value
		int32_t const pow2ceiling = (sizeof(unsigned long long) * 8) - __builtin_clzll(value | this->sub_bucket_mask);
		// adjust for unit bitsize and bucket size
		return pow2ceiling - this->unit_magnitude - (this->sub_bucket_half_count_magnitude + 1);
	}

	inline int32_t counts_index( int32_t bucket_index, int32_t sub_bucket_index) const
	{
		// Calculate the index for the first entry in the bucket:
		// (The following is the equivalent of ((bucket_index + 1) * subBucketHalfCount) ):
		int32_t const bucket_base_index = (bucket_index + 1) << this->sub_bucket_half_count_magnitude;
		// Calculate the offset in the bucket:
		int32_t const offset_in_bucket = sub_bucket_index - this->sub_bucket_half_count;
		// The following is the equivalent of ((sub_bucket_index  - subBucketHalfCount) + bucketBaseIndex;
		return bucket_base_index + offset_in_bucket;
	}

	inline int32_t counts_index_for(int64_t value) const
	{
		int32_t const bucket_index     = get_bucket_index(value);
		int32_t const sub_bucket_index = get_sub_bucket_index(value, bucket_index, this->unit_magnitude);

		return counts_index(bucket_index, sub_bucket_index);
	}

private:

	uint16_t   sub_bucket_count;
	uint16_t   sub_bucket_half_count;
	uint16_t   sub_bucket_mask;
	uint8_t    unit_magnitude;
	uint8_t    sub_bucket_half_count_magnitude;
	// 8

	uint64_t   total_count_;
	// 16

	uint32_t   counts_len_;
	// 24

	std::unique_ptr<counter_t[]> counts_; // field order is important to avoid padding
	// 32

	uint64_t   negative_inf_;
	uint64_t   positive_inf_;
	// 48
};

template<class Histogram>
inline int64_t get_percentile(Histogram const& hv, typename Histogram::config_t const& conf, double percentile)
{
	return hv.get_percentile(conf, percentile);
}


template<class SinkT, class Histogram>
inline void debug_dump_histogram(SinkT& sink, Histogram const& hv, meow::str_ref func_name)
{
	auto const counts_r = hv.get_counts_range();

	ff::fmt(stderr, "{0} internal failure, dumping histogram\n", func_name);
	ff::fmt(stderr, "{0} neg_inf: {1}, pos_inf: {2}, total_count: {3}, hv_size: {4}\n",
		func_name, hv.negative_inf(), hv.positive_inf(), hv.total_count(), counts_r.size());

	for (uint32_t i = 0; i < counts_r.size(); i++)
		ff::fmt(stderr, "  [{0}] -> {1}\n", hv.value_at_index(i), hv.count_at_index(i));
}

////////////////////////////////////////////////////////////////////////////////////////////////

struct histogram_conf_t
{
	duration_t  min_value;       // >= 0
	duration_t  max_value;       // >= 0, >= min_value*2
	duration_t  unit_size;       // (a microsecond or millisecond usually)
	int         precision_bits;  // bucket precision (7 bits = ~1%, 10 bits ~0.1%, 14 bits ~0.01%)

	// for flat_histogram_t
	duration_t bucket_d;     // bucket width

	hdr_histogram_conf_t hdr;
};

meow::error_t hdr_histogram_configure(hdr_histogram_conf_t *conf, histogram_conf_t const& hv_conf)
{
	int64_t low  = (hv_conf.min_value / hv_conf.unit_size).nsec;
	if (low <= 0)
		low = 1;

	int64_t const high = (hv_conf.max_value / hv_conf.unit_size).nsec;
	return hdr_histogram_configure(conf, low, high, hv_conf.precision_bits);
}

////////////////////////////////////////////////////////////////////////////////////////////////
// plain sorted-array histograms, should be faster to merge and calculate percentiles from

struct histogram_value_t
{
	uint32_t bucket_id;
	uint32_t value;
};
static_assert(sizeof(histogram_value_t) == sizeof(uint64_t), "histogram_value_t must have no padding");

inline constexpr bool operator<(histogram_value_t const& l, histogram_value_t const& r)
{
	return l.bucket_id < r.bucket_id;
}

typedef std::vector<histogram_value_t> histogram_values_t;

struct flat_histogram_t
{
	histogram_values_t  values;
	uint32_t            total_count;
	uint32_t            negative_inf;
	uint32_t            positive_inf;
};
static_assert(sizeof(flat_histogram_t) == (sizeof(histogram_values_t)+4*sizeof(uint32_t)), "flat_histogram_t must have no padding");

inline duration_t get_percentile(flat_histogram_t const& hv, histogram_conf_t const& conf, double percentile)
{
	if (percentile == 0.)
		return conf.min_value;

	if (hv.total_count == 0) // no values in histogram, nothing to do
		return conf.min_value;

	uint32_t required_sum = [&]()
	{
		uint32_t const res = std::ceil(hv.total_count * percentile / 100.0);
		return (res > hv.total_count) ? hv.total_count : res;
	}();

	// ff::fmt(stdout, "{0}({1}); total: {2}, required: {3}\n", __func__, percentile, hv.total_count, required_sum);

	// fastpath - very low percentile, nothing to do
	if (required_sum == 0)
		return conf.min_value;

	// fastpath - are we going to hit negative infinity?
	if (required_sum <= hv.negative_inf)
		return conf.min_value;

	// fastpath - are we going to hit positive infinity?
	if (required_sum > (hv.total_count - hv.positive_inf))
		return conf.max_value;

	// already past negative_inf, adjust
	required_sum -= hv.negative_inf;


	// slowpath - shut up and calculate
	uint32_t current_sum = 0;

	for (auto const& item : hv.values)
	{
		uint32_t const bucket_id       = item.bucket_id;
		uint32_t const next_has_values = item.value;
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
			return conf.min_value + conf.bucket_d * (bucket_id + 1);
		}

		// incomplete bucket, interpolate, assuming flat time distribution within bucket
		{
			duration_t const d = conf.bucket_d * need_values / next_has_values;

			// ff::fmt(stdout, "[{0}] last, has: {1}, taking: {2}, {3}\n", bucket_id, next_has_values, need_values, d);
			return conf.min_value + conf.bucket_d * bucket_id + d;
		}
	}

	// dump hv contents to stderr, as we're going to die anyway
	{
		ff::fmt(stderr, "{0} internal failure, dumping histogram\n", __func__);
		ff::fmt(stderr, "{0} neg_inf: {1}, pos_inf: {2}, total_count: {3}, hv_size: {4}\n",
			__func__, hv.negative_inf, hv.positive_inf, hv.total_count, hv.values.size());

		for (auto const& item : hv.values)
			ff::fmt(stderr, "[{0}] -> {1}\n", item.bucket_id, item.value);
	}

	assert(!"must not be reached");
}

template<class Histogram>
inline void histogram___convert_hdr_to_flat(Histogram const& hv, typename Histogram::config_t const& conf, flat_histogram_t *flat)
{
	flat->total_count  = hv.total_count();
	flat->negative_inf = hv.negative_inf();
	flat->positive_inf = hv.positive_inf();

	auto const counts_r = hv.get_counts_range();

	flat->values.clear();
	flat->values.reserve(counts_r.size());

	for (size_t i = 0; i < counts_r.size(); i++)
	{
		// gotta cast here, since flat_histogram_t uses uint32_t
		uint32_t const bucket_id = (uint32_t) hv.value_at_index(i);
		uint32_t const value     = (uint32_t) hv.count_at_index(i);
		flat->values.push_back({ .bucket_id = bucket_id, .value = value });
	}
}

template<class Histogram>
inline flat_histogram_t histogram___convert_hdr_to_flat(Histogram const& hv, typename Histogram::config_t const& conf)
{
	flat_histogram_t result;
	histogram___convert_hdr_to_flat(hv, conf, &result);
	return result;
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
		.min_value      =  0 * d_microsecond,
		.max_value      = 60 * d_second,
		.unit_size      =  1 * d_microsecond,
		.precision_bits = 7,
		.bucket_d       =  1 * d_microsecond,
		.hdr            = {},
	};

#if 0
	{
		histogram_conf_t hv_conf = {
			.bucket_count = (uint32_t)((hv_conf.max_value - hv_conf.min_value) / hv_conf.unit_size).nsec,
			.bucket_d     = hv_conf.unit_size,
			.min_value    = hv_conf.min_value,
		};

		histogram_t hv;

		{
			meow::stopwatch_t sw;
			srandom(sw.now().tv_nsec);

			for (size_t i = 0; i < n_iterations; i++)
			{
				// hv.increment(hv_conf, (random() % hv_conf.bucket_count) * d_millisecond);
				hv.increment(hv_conf, ((uint32_t)i % hv_conf.bucket_count) * d_microsecond);
			}

			hash_d = timeval_to_double(sw.stamp());
			ff::fmt(stdout, "hash: added {0} values, elapsed: {1}, \t{2} inserts/sec, mem: {3}\n"
				, n_iterations, hash_d, (double)n_iterations / hash_d, hv.map_cref().bucket_count() * sizeof(hv.map_cref().begin()));
		}

		{
			meow::stopwatch_t sw;

			ff::fmt(stdout, "  p50: {0}\n", get_percentile(hv, hv_conf, 50));
			ff::fmt(stdout, "  p75: {0}\n", get_percentile(hv, hv_conf, 75));
			ff::fmt(stdout, "  p95: {0}\n", get_percentile(hv, hv_conf, 95));
			ff::fmt(stdout, "  p99: {0}\n", get_percentile(hv, hv_conf, 99));
			ff::fmt(stdout, "  p100: {0}\n", get_percentile(hv, hv_conf, 100));

			ff::fmt(stdout, "percentiles calc took: {0}\n", sw.stamp());
		}

		{
			meow::stopwatch_t sw;
			auto const flat_hv = histogram___convert_ht_to_flat(hv);
			ff::fmt(stdout, "conversion to flat_hv took: {0}\n", sw.stamp());

			sw.reset();

			ff::fmt(stdout, "[flat_hv]\n");
			ff::fmt(stdout, "  p50: {0}\n", get_percentile(flat_hv, hv_conf, 50));
			ff::fmt(stdout, "  p75: {0}\n", get_percentile(flat_hv, hv_conf, 75));
			ff::fmt(stdout, "  p95: {0}\n", get_percentile(flat_hv, hv_conf, 95));
			ff::fmt(stdout, "  p99: {0}\n", get_percentile(flat_hv, hv_conf, 99));
			ff::fmt(stdout, "  p100: {0}\n", get_percentile(flat_hv, hv_conf, 100));
			ff::fmt(stdout, "flat_hv percentiles calc took: {0}, mem: {1}\n"
				, sw.stamp(), flat_hv.values.capacity() * sizeof(*flat_hv.values.begin()));
		}
	}
#endif


#if 0 // hdr_histogram opensource impl, removed from repo
	{
		using hdr_histogram_t = struct hdr_histogram;
		hdr_histogram_t *h;

		struct hdr_histogram_bucket_config cfg;

		{
			int r = hdr_calculate_bucket_config(1, ((hv_conf.max_value - hv_conf.min_value) / hv_conf.unit_size).nsec, 3, &cfg);
			if (0 != r)
				throw std::runtime_error(ff::fmt_str("hdr_calculate_bucket_config error: {0}", r));
		}

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
			bool const ok = hdr_record_values(h, (i % (cfg.highest_trackable_value + 1)), 1);
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

		{
			meow::stopwatch_t sw;

			ff::fmt(stdout, "  p50: {0}\n", hdr_value_at_percentile(h, 50));
			ff::fmt(stdout, "  p75: {0}\n", hdr_value_at_percentile(h, 75));
			ff::fmt(stdout, "  p95: {0}\n", hdr_value_at_percentile(h, 95));
			ff::fmt(stdout, "  p99: {0}\n", hdr_value_at_percentile(h, 99));
			ff::fmt(stdout, "  p100: {0}\n", hdr_value_at_percentile(h, 100));

			ff::fmt(stdout, "hdr percentiles calc took: {0}\n", sw.stamp());
		}

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
#endif

	{
		// using hv_t = hdr_histogram_t<int64_t>;
		// using hv_t = hdr_histogram_t<uint64_t>;
		// using hv_t = hdr_histogram_t<int32_t>;
		using hv_t = hdr_histogram_t<uint32_t>;

		hdr_histogram_conf_t conf;
		auto const err = hdr_histogram_configure(&conf, hv_conf);
		if (err)
			throw std::runtime_error(err.what());

		hv_t hv(conf);

		ff::fmt(stdout, "hv.lowest_trackable_value          = {0}\n", conf.lowest_trackable_value);
		ff::fmt(stdout, "hv.highest_trackable_value         = {0}\n", conf.highest_trackable_value);
		ff::fmt(stdout, "hv.unit_magnitude                  = {0}\n", conf.unit_magnitude);
		ff::fmt(stdout, "hv.significant_bits                = {0}\n", conf.significant_bits);
		ff::fmt(stdout, "hv.sub_bucket_half_count_magnitude = {0}\n", conf.sub_bucket_half_count_magnitude);
		ff::fmt(stdout, "hv.sub_bucket_half_count           = {0}\n", conf.sub_bucket_half_count);
		ff::fmt(stdout, "hv.sub_bucket_mask                 = {0}\n", conf.sub_bucket_mask);
		ff::fmt(stdout, "hv.sub_bucket_count                = {0}\n", conf.sub_bucket_count);
		ff::fmt(stdout, "hv.bucket_count                    = {0}\n", conf.bucket_count);
		ff::fmt(stdout, "hv.counts_len                      = {0}\n", conf.counts_len);

		{
			meow::stopwatch_t sw;
			srandom(sw.now().tv_nsec);

			size_t failed_count = 0;

			for (size_t i = 0; i < n_iterations; i++)
			{
				// hdr_record_values(h, (random() % hv_conf.bucket_count), 1);
				bool const ok = hv.increment(conf, (i % (conf.highest_trackable_value + 1)), 1);
				if (!ok)
					failed_count++;
			}

			auto const hdr_hv_d = timeval_to_double(sw.stamp());
			ff::fmt(stdout, "hdr_hv: added {0} values, elapsed: {1}, \t{2} inserts/sec. speedup: {3}, mem: {4}, failed: {5}\n"
				, n_iterations, hdr_hv_d, (double)n_iterations / hdr_hv_d, hash_d / hdr_hv_d, hv.get_allocated_size(), failed_count);
		}

		{
			meow::stopwatch_t sw;

			ff::fmt(stdout, "  p50: {0}\n", get_percentile(hv, conf, 50));
			ff::fmt(stdout, "  p75: {0}\n", get_percentile(hv, conf, 75));
			ff::fmt(stdout, "  p95: {0}\n", get_percentile(hv, conf, 95));
			ff::fmt(stdout, "  p99: {0}\n", get_percentile(hv, conf, 99));
			ff::fmt(stdout, "  p100: {0}\n", get_percentile(hv, conf, 100));

			ff::fmt(stdout, "hdr_hv percentiles calc took: {0}\n", sw.stamp());
		}

		{
			meow::stopwatch_t sw;
			auto const flat_hv = histogram___convert_hdr_to_flat(hv, conf);
			ff::fmt(stdout, "hdr_hv -> to flat conversion took: {0}, mem: {1}\n"
				, sw.stamp(), flat_hv.values.capacity() * sizeof(*flat_hv.values.begin()));

			sw.reset();

			ff::fmt(stdout, "  p50: {0}\n", get_percentile(flat_hv, hv_conf, 50));
			ff::fmt(stdout, "  p75: {0}\n", get_percentile(flat_hv, hv_conf, 75));
			ff::fmt(stdout, "  p95: {0}\n", get_percentile(flat_hv, hv_conf, 95));
			ff::fmt(stdout, "  p99: {0}\n", get_percentile(flat_hv, hv_conf, 99));
			ff::fmt(stdout, "  p100: {0}\n", get_percentile(flat_hv, hv_conf, 100));

			ff::fmt(stdout, "flat percentiles calc took: {0}\n", sw.stamp());
		}

		{
			hv_t hv_copy = hv;
			size_t const n_merges = 10 * 1000;

			meow::stopwatch_t sw;

			for (size_t i = 0; i < n_merges; i++)
				hv.merge_other_with_same_conf(hv_copy, conf);

			auto const d = timeval_to_double(sw.stamp());
			ff::fmt(stdout, "hdr_hv {0} merges took: {1}, {2} per merge\n", n_merges, d, ff::as_printf("%1.10f", d / n_merges));

			ff::fmt(stderr, "{0} neg_inf: {1}, pos_inf: {2}, total_count: {3}, hv_size: {4}\n",
				__func__, hv.negative_inf(), hv.positive_inf(), hv.total_count(), hv.get_counts_range().size());

			ff::fmt(stdout, "[merged_hv percentiles, should not change]\n");
			ff::fmt(stdout, "  p50: {0}\n", get_percentile(hv, conf, 50));
			ff::fmt(stdout, "  p75: {0}\n", get_percentile(hv, conf, 75));
			ff::fmt(stdout, "  p95: {0}\n", get_percentile(hv, conf, 95));
			ff::fmt(stdout, "  p99: {0}\n", get_percentile(hv, conf, 99));
			ff::fmt(stdout, "  p100: {0}\n", get_percentile(hv, conf, 100));
		}
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
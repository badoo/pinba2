#ifndef PINBA__HDR_HISTOGRAM_H_
#define PINBA__HDR_HISTOGRAM_H_

#include <cstdint>
#include <cmath>   // ceil
#include <memory>
#include <type_traits>

#include <boost/noncopyable.hpp>

#include <meow/error.hpp>
#include <meow/str_ref.hpp>
#include <meow/format/format.hpp>

#include "misc/nmpa.h"

#include "pinba/globals.h"
#include "pinba/limits.h"

////////////////////////////////////////////////////////////////////////////////////////////////

struct hdr_histogram_conf_t
{
	// internal bits
	uint16_t sub_bucket_count;
	uint16_t sub_bucket_half_count;
	uint16_t sub_bucket_mask;
	uint8_t unit_magnitude;
	uint8_t sub_bucket_half_count_magnitude;

	// information only
	int64_t lowest_trackable_value;
	int64_t highest_trackable_value;
	int64_t significant_bits;
	int32_t bucket_count;
	int32_t counts_len;
};

static inline int32_t hdr___buckets_needed_to_cover_value(int64_t value, int32_t sub_bucket_count, int32_t unit_magnitude)
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

static inline int64_t hdr___int_power(int64_t base, int64_t exp)
{
	int64_t result = 1;
	while(exp)
	{
		result *= base;
		exp--;
	}
	return result;
};

inline meow::error_t hdr_histogram_configure(
		hdr_histogram_conf_t *cfg,
		int64_t lowest_trackable_value,
		int64_t highest_trackable_value,
		int significant_bits)
{
	if (lowest_trackable_value <= 0)
	{
		return meow::format::fmt_err("lowest_trackable_value must be > 0, {0} given", lowest_trackable_value);
	}

	if (lowest_trackable_value * 2 > highest_trackable_value)
	{
		return meow::format::fmt_err("lowest_trackable_value * 2 must be <= highest_trackable_value, {0}*2 > {1}",
			lowest_trackable_value, highest_trackable_value);
	}

	if (significant_bits < 1 || significant_bits > 14)
	{
		return meow::format::fmt_err("significant_bits must be in range [1, 14], {0} given", significant_bits);
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

inline meow::error_t hdr_histogram_configure___sig_figures(
		hdr_histogram_conf_t* cfg,
		int64_t lowest_trackable_value,
		int64_t highest_trackable_value,
		int significant_figures)
{
	// smallerst power of 2, to cover given power of 10
	int const sig_bits = 64 - __builtin_clzll(hdr___int_power(10, significant_figures));
	return hdr_histogram_configure(cfg, lowest_trackable_value, highest_trackable_value, sig_bits);
}

////////////////////////////////////////////////////////////////////////////////////////////////

template<class CounterT>
struct hdr_histogram___impl_t : private boost::noncopyable
{
	using self_t    = hdr_histogram___impl_t;
	using counter_t = CounterT;
	using config_t  = hdr_histogram_conf_t;

	static_assert(std::is_integral<counter_t>::value, "T must be an integral type");

public:

	hdr_histogram___impl_t(struct nmpa_s *nmpa, config_t const& conf)
	{
		nmpa_                           = nmpa;

		negative_inf_                   = 0;
		positive_inf_                   = 0;
		total_count_                    = 0;
		counts_nonzero_                 = 0;
		counts_maxlen_                  = conf.counts_len;

		sub_bucket_count                = conf.sub_bucket_count;
		sub_bucket_half_count           = conf.sub_bucket_half_count;
		sub_bucket_mask                 = conf.sub_bucket_mask;
		unit_magnitude                  = conf.unit_magnitude;
		sub_bucket_half_count_magnitude = conf.sub_bucket_half_count_magnitude;

		// allocate small part on init
		counts_len_                     = conf.sub_bucket_half_count;
		counts_ = (counter_t*)nmpa_calloc(nmpa_, counts_len_ * sizeof(counter_t));
		if (counts_ == nullptr)
			throw std::bad_alloc();
	}

	~hdr_histogram___impl_t()
	{
		// not supposed to free anything from nmpa
		// and it won't work anyway
	}

	// not to be copied
	hdr_histogram___impl_t(hdr_histogram___impl_t const& other) = delete;
	hdr_histogram___impl_t& operator=(hdr_histogram___impl_t const& other) = delete;

	// movable

	hdr_histogram___impl_t(hdr_histogram___impl_t&& other) noexcept
	{
		(*this) = std::move(other); // call move operator=()
	}

	hdr_histogram___impl_t& operator=(hdr_histogram___impl_t&& other) noexcept
	{
		nmpa_                           = other.nmpa;

		negative_inf_                   = other.negative_inf_;
		positive_inf_                   = other.positive_inf_;
		total_count_                    = other.total_count_;
		counts_nonzero_                 = other.counts_nonzero_;
		counts_len_                     = other.counts_len_;
		counts_maxlen_                  = other.counts_maxlen_;

		sub_bucket_count                = other.sub_bucket_count;
		sub_bucket_half_count           = other.sub_bucket_half_count;
		sub_bucket_mask                 = other.sub_bucket_mask;
		unit_magnitude                  = other.unit_magnitude;
		sub_bucket_half_count_magnitude = other.sub_bucket_half_count_magnitude;

		counts_ = other.counts_;
		other.counts_ = nullptr;

		return *this;
	}

public: // reads

	counter_t negative_inf() const noexcept { return negative_inf_; }
	counter_t positive_inf() const noexcept { return positive_inf_; }
	uint64_t  total_count() const noexcept { return total_count_; }
	uint32_t  counts_nonzero() const noexcept { return counts_nonzero_; }
	uint32_t  counts_len() const noexcept { return counts_len_; }

	using counts_range_t    = meow::string_ref<counter_t const>;
	using counts_range_nc_t = meow::string_ref<counter_t>;

	counts_range_t get_counts_range() const
	{
		return { this->counts_, this->counts_len_ };
	}

	counts_range_nc_t mutable_counts_range()
	{
		return { this->counts_, this->counts_len_ };
	}

	uint64_t get_allocated_size() const
	{
		return sizeof(counter_t) * this->counts_len_;
	}

public:

	bool increment(config_t const& conf, int64_t value, counter_t increment_by = 1) noexcept
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
			// assert((counts_index >= 0) && ((uint32_t)counts_index < this->counts_len_));

			if ((uint32_t)counts_index >= counts_len_)
			{
				counter_t *tmp = (counter_t*)nmpa_realloc(nmpa_, counts_, counts_len_ * sizeof(counter_t), counts_maxlen_ * sizeof(counter_t));
				if (tmp == nullptr)
				{
					// we're noexcept, can't throw, so silently ignore the operation, best we can do for the time being
					// TODO: fix this somehow properly (maybe have a counter for this error type? needed at all? reasonable reaction possible?)
					// throw std::bad_alloc();
					return false;
				}
				std::uninitialized_fill(tmp + counts_len_, tmp + counts_maxlen_, 0);
				counts_len_ = counts_maxlen_;
				counts_ = tmp;
			}

			counter_t& counter = this->counts_[counts_index];

			counts_nonzero_ += (counter == 0);
			counter         += increment_by;
		}

		this->total_count_ += increment_by;
		return true;
	}

	void merge_other_with_same_conf(self_t const& other, config_t const& conf)
	{
		assert(this->counts_maxlen_ == other.counts_maxlen_);

		if (this->counts_len_ < other.counts_len_)
		{
			counter_t *tmp = (counter_t*)nmpa_realloc(nmpa_, counts_, counts_len_ * sizeof(counter_t), counts_maxlen_ * sizeof(counter_t));
			if (tmp == nullptr)
				throw std::bad_alloc();

			std::uninitialized_fill(tmp + counts_len_, tmp + counts_maxlen_, 0);
			counts_len_ = counts_maxlen_;
			counts_ = tmp;
		}

		for (uint32_t i = 0; i < other.counts_len_; i++)
		{
			counter_t&       dst_counter = this->counts_[i];
			counter_t const& src_counter = other.counts_[i];

			if ((dst_counter == 0) && (src_counter != 0))
				this->counts_nonzero_ += 1;

			dst_counter += src_counter;
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

		// meow::format::fmt(stderr, "{0}({1}); neg_inf: {2}, pos_inf: {3}\n", __func__, percentile, this->negative_inf(), this->positive_inf());
		// meow::format::fmt(stderr, "{0}({1}); total: {2}, required: {3}\n", __func__, percentile, this->total_count(), required_sum);

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
				// meow::format::fmt(stderr, "[{0}] current_sum +=; {1} -> {2}\n", bucket_id, next_has_values, current_sum);
				continue;
			}

			if (next_has_values == need_values) // complete bucket, return upper time bound for this bucket
			{
				// meow::format::fmt(stderr, "[{0}] full; current_sum +=; {1} -> {2}\n", bucket_id, next_has_values, current_sum);
				int64_t const result = this->highest_equivalent_value(this->value_at_index(bucket_id));
				return (result < conf.highest_trackable_value)
						? result
						: conf.highest_trackable_value;
			}

			// incomplete bucket, interpolate, assuming flat time distribution within bucket
			{
				int64_t const d = this->size_of_equivalent_value_range(bucket_id) * need_values / next_has_values;

				// meow::format::fmt(stderr, "[{0}] last, has: {1}, taking: {2}, {3}\n", bucket_id, next_has_values, need_values, d);
				int64_t const result = this->lowest_equivalent_value(this->value_at_index(bucket_id)) + d;
				return (result < conf.highest_trackable_value)
						? result
						: conf.highest_trackable_value;
			}
		}

		// dump hv contents to stderr, as we're going to die anyway
		{
			meow::format::fmt(stderr, "{0} internal failure, dumping histogram\n", __func__);
			meow::format::fmt(stderr, "{0} neg_inf: {1}, pos_inf: {2}, total_count: {3}, hv_size: {4}\n",
				__func__, this->negative_inf(), this->positive_inf(), this->total_count(), this->counts_len_);

			for (uint32_t i = 0; i < counts_r.size(); i++)
				meow::format::fmt(stderr, "[{0}] -> {1}\n", this->value_at_index(i), this->count_at_index(i));
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
	struct nmpa_s *nmpa_;
	// 8

	// FIXME: experiment with small types, they seem to generate too many instructions to extend
	uint16_t   sub_bucket_count;
	uint16_t   sub_bucket_half_count;
	uint16_t   sub_bucket_mask;
	uint8_t    unit_magnitude;
	uint8_t    sub_bucket_half_count_magnitude;
	// 16

	uint32_t   counts_nonzero_;
	uint32_t   counts_len_;
	// 24

	uint64_t   total_count_;
	// 32

	counter_t  *counts_;
	// 40

	uint32_t    counts_maxlen_;
	counter_t   negative_inf_;
	counter_t   positive_inf_;
	uint32_t    padding____; // explicitly put it somewhere, as we know it exists
	// 56
};
static_assert(sizeof(hdr_histogram___impl_t<uint32_t>) == 56, "");

////////////////////////////////////////////////////////////////////////////////////////////////

template<class SinkT, class Histogram>
inline void hdr_histogram___debug_dump(SinkT& sink, Histogram const& hv, meow::str_ref func_name)
{
	auto const counts_r = hv.get_counts_range();

	meow::format::fmt(stderr, "{0} internal failure, dumping histogram\n", func_name);
	meow::format::fmt(stderr, "{0} neg_inf: {1}, pos_inf: {2}, total_count: {3}, hv_size: {4}\n",
		func_name, hv.negative_inf(), hv.positive_inf(), hv.total_count(), counts_r.size());

	for (uint32_t i = 0; i < counts_r.size(); i++)
		meow::format::fmt(stderr, "  [{0}] -> {1}\n", hv.value_at_index(i), hv.count_at_index(i));
}

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__HDR_HISTOGRAM_H_

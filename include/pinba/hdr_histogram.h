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

struct hdr_algorithms
{
	static inline int64_t value_at_index(hdr_histogram_conf_t const& conf, int32_t index)
	{
		int32_t bucket_index = (index >> conf.sub_bucket_half_count_magnitude) - 1;
		int32_t sub_bucket_index = (index & (conf.sub_bucket_half_count - 1)) + conf.sub_bucket_half_count;

		if (bucket_index < 0)
		{
			sub_bucket_index -= conf.sub_bucket_half_count;
			bucket_index = 0;
		}

		return value_from_index(conf, bucket_index, sub_bucket_index);
	}

	static inline int64_t value_from_index(hdr_histogram_conf_t const& conf, int32_t bucket_index, int32_t sub_bucket_index)
	{
		return ((int64_t) sub_bucket_index) << (bucket_index + conf.unit_magnitude);
	}

	static inline int32_t index_for_value(hdr_histogram_conf_t const& conf, int64_t value)
	{
		int32_t const bucket_index     = get_bucket_index(conf, value);
		int32_t const sub_bucket_index = get_sub_bucket_index(conf, value, bucket_index);

		return index_combined(conf, bucket_index, sub_bucket_index);
	}

	static inline int32_t index_combined(hdr_histogram_conf_t const& conf, int32_t bucket_index, int32_t sub_bucket_index)
	{
		// Calculate the index for the first entry in the bucket:
		// (The following is the equivalent of ((bucket_index + 1) * subBucketHalfCount) ):
		int32_t const bucket_base_index = (bucket_index + 1) << conf.sub_bucket_half_count_magnitude;
		// Calculate the offset in the bucket:
		int32_t const offset_in_bucket = sub_bucket_index - conf.sub_bucket_half_count;
		// The following is the equivalent of ((sub_bucket_index  - subBucketHalfCount) + bucketBaseIndex;
		return bucket_base_index + offset_in_bucket;
	}

// ranges

	static inline int64_t size_of_equivalent_value_range(hdr_histogram_conf_t const& conf, int64_t value)
	{
		int32_t const bucket_index     = get_bucket_index(conf, value);
		int32_t const sub_bucket_index = get_sub_bucket_index(conf, value, bucket_index);
		int32_t const adjusted_bucket  = (sub_bucket_index >= conf.sub_bucket_count) ? (bucket_index + 1) : bucket_index;
		return int64_t(1) << (conf.unit_magnitude + adjusted_bucket);
	}

	static inline int64_t next_non_equivalent_value(hdr_histogram_conf_t const& conf, int64_t value)
	{
		return lowest_equivalent_value(conf, value) + size_of_equivalent_value_range(conf, value);
	}

	static inline int64_t lowest_equivalent_value(hdr_histogram_conf_t const& conf, int64_t value)
	{
		int32_t const bucket_index     = get_bucket_index(conf, value);
		int32_t const sub_bucket_index = get_sub_bucket_index(conf, value, bucket_index);
		return value_from_index(conf, bucket_index, sub_bucket_index);
	}

	static inline int64_t highest_equivalent_value(hdr_histogram_conf_t const& conf, int64_t value)
	{
		return next_non_equivalent_value(conf, value) - 1;
	}

// utilities

	static inline int32_t get_sub_bucket_index(hdr_histogram_conf_t const& conf, int64_t value, int32_t bucket_index)
	{
		return (int32_t)(value >> (bucket_index + conf.unit_magnitude));
	}

	static inline int32_t get_bucket_index(hdr_histogram_conf_t const& conf, int64_t value)
	{
		// get smallest power of 2 containing value
		int32_t const pow2ceiling = (sizeof(unsigned long long) * 8) - __builtin_clzll(value | conf.sub_bucket_mask);
		// adjust for unit bitsize and bucket size
		return pow2ceiling - conf.unit_magnitude - (conf.sub_bucket_half_count_magnitude + 1);
	}
};

////////////////////////////////////////////////////////////////////////////////////////////////
#if 0

template<class CounterT>
struct hdr_snapshot___impl_t
{
	struct count_t
	{
		uint32_t index;
		CounterT count;
	};

	std::vector<count_t>  counts       = {};
	uint64_t              total_count  = 0;
	CounterT              negative_inf = 0;
	CounterT              positive_inf = 0;

	hdr_snapshot___impl_t() = default;

	// move only
	hdr_snapshot___impl_t(hdr_snapshot___impl_t&& other) = default;
	hdr_snapshot___impl_t& operator=(hdr_snapshot___impl_t&& other) = default;
};
static_assert(std::is_integral<uint32_t>::value, "hdr_snapshot_t; CounterT must be integral type");
static_assert(!std::is_copy_constructible<hdr_snapshot___impl_t<uint32_t>>::value, "hdr_snapshot_t must be movable");
static_assert(std::is_nothrow_move_constructible<hdr_snapshot___impl_t<uint32_t>>::value, "hdr_snapshot_t must be movable");
static_assert(std::is_nothrow_move_assignable<hdr_snapshot___impl_t<uint32_t>>::value, "hdr_snapshot_t must be movable");
#endif
////////////////////////////////////////////////////////////////////////////////////////////////

template<class CounterT>
struct hdr_histogram___impl_t : private boost::noncopyable
{
	using self_t     = hdr_histogram___impl_t;
	using counter_t  = CounterT;
	using config_t   = hdr_histogram_conf_t;
	// using snapshot_t = hdr_snapshot___impl_t<CounterT>;

	static_assert(std::is_integral<counter_t>::value, "T must be an integral type");

public:

	hdr_histogram___impl_t(struct nmpa_s *nmpa, config_t const& conf)
	{
		nmpa_                           = nmpa;

		negative_inf                   = 0;
		positive_inf                   = 0;
		total_count                    = 0;
		counts_nonzero                 = 0;

		// allocate small part on init
		counts_len                      = conf.sub_bucket_half_count;
		counts = (counter_t*)nmpa_calloc(nmpa_, counts_len * sizeof(counter_t));
		if (counts == nullptr)
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

		negative_inf                   = other.negative_inf;
		positive_inf                   = other.positive_inf;
		total_count                    = other.total_count;
		counts_nonzero                 = other.counts_nonzero;
		counts_len                     = other.counts_len;

		counts = other.counts;
		other.counts = nullptr;

		return *this;
	}

public: // reads

	counter_t get_negative_inf() const noexcept { return negative_inf; }
	counter_t get_positive_inf() const noexcept { return positive_inf; }
	uint64_t  get_total_count() const noexcept { return total_count; }
	uint32_t  get_counts_nonzero() const noexcept { return counts_nonzero; }
	uint32_t  get_counts_len() const noexcept { return counts_len; }
	uint32_t* get_counts_ptr() const noexcept { return counts; }

	using counts_range_t    = meow::string_ref<counter_t const>;
	using counts_range_nc_t = meow::string_ref<counter_t>;

	counts_range_t get_counts_range() const
	{
		return { this->counts, this->counts_len };
	}

	counts_range_nc_t mutable_counts_range()
	{
		return { this->counts, this->counts_len };
	}

	uint64_t get_allocated_size() const
	{
		return sizeof(counter_t) * this->counts_len;
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
			this->negative_inf += increment_by;
		}
		else if (__builtin_expect(value > conf.highest_trackable_value, 0))
		{
			this->positive_inf += increment_by;
		}
		else {
			int32_t const counts_index = hdr_algorithms::index_for_value(conf, value);
			// assert((counts_index >= 0) && ((uint32_t)counts_index < this->counts_len));

			if ((uint32_t)counts_index >= counts_len)
			{
				uint32_t const new_counts_len = conf.counts_len;

				counter_t *tmp = (counter_t*)nmpa_realloc(nmpa_, counts, counts_len * sizeof(counter_t), new_counts_len * sizeof(counter_t));
				if (tmp == nullptr)
				{
					// we're noexcept, can't throw, so silently ignore the operation, best we can do for the time being
					// TODO: fix this somehow properly (maybe have a counter for this error type? needed at all? reasonable reaction possible?)
					// throw std::bad_alloc();
					return false;
				}
				std::uninitialized_fill(tmp + counts_len, tmp + new_counts_len, 0);
				counts_len = new_counts_len;
				counts = tmp;
			}

			counter_t& counter = this->counts[counts_index];

			counts_nonzero += (counter == 0);
			counter         += increment_by;
		}

		this->total_count += increment_by;
		return true;
	}

	void merge_other_with_same_conf(self_t const& other, config_t const& conf)
	{
		if (this->counts_len < other.counts_len)
		{
			uint32_t const new_counts_len = conf.counts_len;

			counter_t *tmp = (counter_t*)nmpa_realloc(nmpa_, counts, counts_len * sizeof(counter_t), new_counts_len * sizeof(counter_t));
			if (tmp == nullptr)
				throw std::bad_alloc();

			std::uninitialized_fill(tmp + counts_len, tmp + new_counts_len, 0);
			counts_len = new_counts_len;
			counts = tmp;
		}

		for (uint32_t i = 0; i < other.counts_len; i++)
		{
			counter_t&       dst_counter = this->counts[i];
			counter_t const& src_counter = other.counts[i];

			if ((dst_counter == 0) && (src_counter != 0))
				this->counts_nonzero += 1;

			dst_counter += src_counter;
		}

		this->negative_inf += other.negative_inf;
		this->positive_inf += other.positive_inf;
		this->total_count += other.total_count;
	}

public:

	inline counter_t count_at_index(int32_t index) const
	{
		return this->counts[index];
	}

	inline int64_t value_at_index(config_t const& conf, int32_t index) const
	{
		return hdr_algorithms::value_at_index(conf, index);
	}

private:
	struct nmpa_s *nmpa_;
	// 8

	uint32_t   counts_nonzero;
	uint32_t   counts_len;
	// 16

	uint64_t   total_count;
	// 24

	counter_t  *counts;
	// 32

	counter_t   negative_inf;
	counter_t   positive_inf;
	// 40
};
static_assert(sizeof(hdr_histogram___impl_t<uint32_t>) == 40, "");

////////////////////////////////////////////////////////////////////////////////////////////////

template<class Histogram>
inline int64_t hdr_histogram___get_percentile(Histogram const& h, hdr_histogram_conf_t const& conf, double percentile)
{
	if (percentile == 0.)
		return conf.lowest_trackable_value;

	if (h.get_total_count() == 0) // no values in histogram, nothing to do
		return conf.lowest_trackable_value;

	uint64_t required_sum = [&]()
	{
		uint64_t const res = std::ceil(h.get_total_count() * percentile / 100.0);
		uint64_t const total = (uint64_t)h.get_total_count();
		return (res > total) ? total : res;
	}();

	// meow::format::fmt(stderr, "{0}({1}); neg_inf: {2}, pos_inf: {3}\n", __func__, percentile, h.negative_inf(), h.positive_inf());
	// meow::format::fmt(stderr, "{0}({1}); total: {2}, required: {3}\n", __func__, percentile, h.total_count(), required_sum);

	// fastpath - are we in negative_inf?, <= here !
	if (required_sum <= (uint64_t)h.get_negative_inf())
		return conf.lowest_trackable_value;

	// fastpath - are we going to hit positive_inf?
	if (required_sum > (uint64_t)(h.get_total_count() - h.get_positive_inf()))
		return conf.highest_trackable_value;

	// already past negative_inf, adjust
	required_sum -= h.get_negative_inf();


	// slowpath - shut up and calculate
	uint64_t current_sum = 0;

	auto const counts_r = h.get_counts_range();

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
			int64_t const result = hdr_algorithms::highest_equivalent_value(conf, hdr_algorithms::value_at_index(conf, bucket_id));
			return (result < conf.highest_trackable_value)
					? result
					: conf.highest_trackable_value;
		}

		// incomplete bucket, interpolate, assuming flat time distribution within bucket
		{
			int64_t const d = hdr_algorithms::size_of_equivalent_value_range(conf, bucket_id) * need_values / next_has_values;

			// meow::format::fmt(stderr, "[{0}] last, has: {1}, taking: {2}, {3}\n", bucket_id, next_has_values, need_values, d);
			int64_t const result = hdr_algorithms::lowest_equivalent_value(conf, hdr_algorithms::value_at_index(conf, bucket_id)) + d;
			return (result < conf.highest_trackable_value)
					? result
					: conf.highest_trackable_value;
		}
	}

	// dump hv contents to stderr, as we're going to die anyway
	hdr_histogram___debug_dump(stderr, h, conf, "get_percentile");

	assert(!"must not be reached");
}
#if 0
template<class Histogram>
typename Histogram::snapshot_t hdr_histogram___create_snapshot(Histogram const& h, config_t const& conf)
{
	typename Histogram::snapshot_t result;

	result.counts.resize(h.counts_nonzero());
	result.total_count  = h.total_count();
	result.negative_inf = h.negative_inf();
	result.positive_inf = h.positive_inf();

	uint32_t write_position = 0;

	for (uint32_t i = 0; i < h.counts_len(); ++i)
	{
		uint32_t const cnt = h.count_at_index(i);
		if (cnt == 0)
			continue;

		auto& snap_elt = result.counts[write_position];

		snap_elt.index = i;
		snap_elt.count = cnt;

		write_position++;
	}

	assert(write_position == result.counts.size());

	return result;
}
#endif

// nmpa allocated pod / standard-layout type,
// intended to be allocated in nmpa, not constructed with ctor
struct hdr_snapshot___nmpa_t
{
	struct count_t
	{
		uint32_t index;
		uint32_t count;
	};

	uint64_t  total_count;
	uint32_t  negative_inf;
	uint32_t  positive_inf;

	uint32_t  counts_len;
	// uint32_t  padding___;
	count_t   counts[0];
};
static_assert(sizeof(hdr_snapshot___nmpa_t) == 24, "no padding expected in hdr_snapshot___nmpa_t");

struct hdr_snapshot___merged_t
{
	uint64_t  total_count;
	uint32_t  negative_inf;
	uint32_t  positive_inf;

	uint32_t  counts_len;
	uint32_t  counts_nonzero;
	uint32_t  counts[0];
};
static_assert(sizeof(hdr_snapshot___merged_t) == 24, "no padding expected in hdr_snapshot___merged_t");


template<class Histogram>
inline hdr_snapshot___nmpa_t* hdr_histogram___save_snapshot_nmpa(Histogram const& h, hdr_histogram_conf_t const& conf, struct nmpa_s *nmpa)
{
	hdr_snapshot___nmpa_t *result = nullptr;
	size_t const object_size = sizeof(*result) + h.get_counts_nonzero() * sizeof(result->counts[0]);

	result = static_cast<hdr_snapshot___nmpa_t*>(nmpa_calloc(nmpa, object_size));
	if (nullptr == result)
		return nullptr;

	result->counts_len   = h.get_counts_nonzero();
	result->total_count  = h.get_total_count();
	result->negative_inf = h.get_negative_inf();
	result->positive_inf = h.get_positive_inf();

	// find and copy all non-zero elts of the array (of which there are h.get_counts_nonzero())
	//  the loop structure idea is to avoid scanning the long tail of zeroes in source array
	uint32_t read_position = 0;
	for (uint32_t i = 0; i < result->counts_len; ++i)
	{
		auto read_count = 0;

		while (0 == (read_count = h.count_at_index(read_position)))
			++read_position;

		auto& snap_elt = result->counts[i];

		snap_elt.index = read_position;
		snap_elt.count = read_count;

		++read_position;
	}

	return result;
}

inline hdr_snapshot___merged_t* hdr_histogram___allocate_snapshot_merger(struct nmpa_s *nmpa, hdr_histogram_conf_t const& conf)
{
	size_t const obj_size = sizeof(hdr_snapshot___merged_t) + conf.counts_len * sizeof(uint32_t);

	auto *result = static_cast<hdr_snapshot___merged_t*>(nmpa_calloc(nmpa, obj_size)); // zero init is important here
	if (nullptr == result)
		return nullptr;

	result->counts_len = conf.counts_len;

	return result;
}

inline void hdr_histogram___merge_snapshot_from_to(hdr_histogram_conf_t const& conf, hdr_snapshot___nmpa_t const& from, hdr_snapshot___merged_t& to)
{
	to.total_count  += from.total_count;
	to.negative_inf += from.negative_inf;
	to.positive_inf += from.positive_inf;

	for (uint32_t i = 0; i < from.counts_len; ++i)
	{
		auto const    & src_pair    = from.counts[i];
		uint32_t      & dst_counter = to.counts[src_pair.index];

		// ff::fmt(stderr, "merging; at: {0}, src: {1} + dst: {2} = ", src_pair.index, src_pair.count, dst_counter);

		// check if a zero counter is changing to non zero
		//  1st check is the real one
		//  2nd check is always true, since we're copying from a snapshot, which is not supposed to store zeroes at all
		if ((dst_counter == 0) && (src_pair.count != 0))
			to.counts_nonzero += 1;

		dst_counter += src_pair.count;

		// ff::fmt(stderr, "{0}\n", dst_counter);
	}
}

template<class HSnapshot>
inline int64_t hdr_snapshot___get_percentile(HSnapshot const& h, hdr_histogram_conf_t const& conf, double percentile)
{
	if (percentile == 0.)
		return conf.lowest_trackable_value;

	if (h.total_count == 0) // no values in histogram, nothing to do
		return conf.lowest_trackable_value;

	uint64_t required_sum = [&]()
	{
		uint64_t const res = std::ceil(h.total_count * percentile / 100.0);
		uint64_t const total = (uint64_t)h.total_count;
		return (res > total) ? total : res;
	}();

	// meow::format::fmt(stderr, "{0}({1}); neg_inf: {2}, pos_inf: {3}\n", __func__, percentile, h.negative_inf(), h.positive_inf());
	// meow::format::fmt(stderr, "{0}({1}); total: {2}, required: {3}\n", __func__, percentile, h.total_count(), required_sum);

	// fastpath - are we in negative_inf?, <= here !
	if (required_sum <= (uint64_t)h.negative_inf)
		return conf.lowest_trackable_value;

	// fastpath - are we going to hit positive_inf?
	if (required_sum > (uint64_t)(h.total_count - h.positive_inf))
		return conf.highest_trackable_value;

	// already past negative_inf, adjust
	required_sum -= h.negative_inf;


	// slowpath - shut up and calculate
	uint64_t current_sum = 0;

	for (uint32_t i = 0; i < h.counts_len; i++)
	{
		uint32_t const index           = i;
		uint64_t const next_has_values = h.counts[i];
		uint64_t const need_values     = required_sum - current_sum;

		if (next_has_values < need_values) // take bucket and move on
		{
			current_sum += next_has_values;
			// meow::format::fmt(stderr, "[{0}] current_sum +=; {1} -> {2}\n", index, next_has_values, current_sum);
			continue;
		}

		if (next_has_values == need_values) // complete bucket, return upper time bound for this bucket
		{
			// meow::format::fmt(stderr, "[{0}] full; current_sum +=; {1} -> {2}\n", index, next_has_values, current_sum);
			int64_t const result = hdr_algorithms::highest_equivalent_value(conf, hdr_algorithms::value_at_index(conf, index));
			return (result < conf.highest_trackable_value)
					? result
					: conf.highest_trackable_value;
		}

		// incomplete bucket, interpolate, assuming flat time distribution within bucket
		{
			int64_t const d = hdr_algorithms::size_of_equivalent_value_range(conf, index) * need_values / next_has_values;

			// meow::format::fmt(stderr, "[{0}] last, has: {1}, taking: {2}, {3}\n", index, next_has_values, need_values, d);
			int64_t const result = hdr_algorithms::lowest_equivalent_value(conf, hdr_algorithms::value_at_index(conf, index)) + d;
			return (result < conf.highest_trackable_value)
					? result
					: conf.highest_trackable_value;
		}
	}

	// dump hv contents to stderr, as we're going to die anyway
	// FIXME:
	// hdr_histogram___debug_dump(stderr, h, "get_percentile");

	assert(!"must not be reached");
}


template<class SinkT, class Histogram>
inline void hdr_histogram___debug_dump(SinkT& sink, Histogram const& hv, hdr_histogram_conf_t const& conf, meow::str_ref func_name)
{
	auto const counts_r = hv.get_counts_range();

	meow::format::fmt(stderr, "{0} internal failure, dumping histogram\n", func_name);
	meow::format::fmt(stderr, "{0} neg_inf: {1}, pos_inf: {2}, total_count: {3}, hv_size: {4}\n",
		func_name, hv.get_negative_inf(), hv.get_positive_inf(), hv.get_total_count(), counts_r.size());

	for (uint32_t i = 0; i < counts_r.size(); i++)
		meow::format::fmt(stderr, "  [{0}] -> {1}\n", hdr_algorithms::value_at_index(conf, i), hv.count_at_index(i));
}

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__HDR_HISTOGRAM_H_

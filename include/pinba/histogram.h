#ifndef PINBA__HISTOGRAM_H_
#define PINBA__HISTOGRAM_H_

#include <cstdint>
#include <cmath>   // ceil

#include "pinba/limits.h"
#include "pinba/hdr_histogram.h"

////////////////////////////////////////////////////////////////////////////////////////////////
// histograms

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
	uint32_t            value_count;
	uint32_t            negative_inf;
	uint32_t            positive_inf;
};
static_assert(sizeof(flat_histogram_t) == (sizeof(histogram_values_t)+4*sizeof(uint32_t)), "flat_histogram_t must have no padding");

inline duration_t get_percentile(flat_histogram_t const& hv, histogram_conf_t const& conf, double percentile)
{
	if (percentile == 0.)
		return conf.min_value;

	if (hv.value_count == 0) // no values in histogram, nothing to do
		return conf.min_value;

	uint32_t required_sum = [&]()
	{
		uint32_t const res = std::ceil(hv.value_count * percentile / 100.0);
		return (res > hv.value_count) ? hv.value_count : res;
	}();

	// ff::fmt(stdout, "{0}({1}); total: {2}, required: {3}\n", __func__, percentile, hv.value_count, required_sum);

	// fastpath - very low percentile, nothing to do
	if (required_sum == 0)
		return conf.min_value;

	// fastpath - are we going to hit negative infinity?
	if (required_sum <= hv.negative_inf)
		return conf.min_value;

	// fastpath - are we going to hit positive infinity?
	if (required_sum > (hv.value_count - hv.positive_inf))
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
		ff::fmt(stderr, "{0} neg_inf: {1}, pos_inf: {2}, value_count: {3}, hv_size: {4}\n",
			__func__, hv.negative_inf, hv.positive_inf, hv.value_count, hv.values.size());

		for (auto const& item : hv.values)
			ff::fmt(stderr, "[{0}] -> {1}\n", item.bucket_id, item.value);
	}

	assert(!"must not be reached");
}

////////////////////////////////////////////////////////////////////////////////////////////////
// hdr histogram - used for current timeslice histograms aggregation

struct hdr_histogram_t : public hdr_histogram___impl_t<uint32_t>
{
	using base_t = hdr_histogram___impl_t<uint32_t>;

	hdr_histogram_t(histogram_conf_t const& conf)
		: base_t(conf.hdr)
	{
	}

	void increment(histogram_conf_t const& conf, duration_t const d)
	{
		this->base_t::increment(conf.hdr, (d / conf.unit_size).nsec);
	}

	void merge_other_with_same_conf(hdr_histogram_t const& other, histogram_conf_t const& conf)
	{
		return this->base_t::merge_other_with_same_conf(other, conf.hdr);
	}
};

inline duration_t get_percentile(hdr_histogram_t const& hv, histogram_conf_t const& conf, double percentile)
{
	int64_t const pct_value = hv.get_percentile(conf.hdr, percentile);
	return pct_value * conf.unit_size;
}

inline meow::error_t hdr_histogram_configure(hdr_histogram_conf_t *conf, histogram_conf_t const& hv_conf)
{
	// FIXME: find a better way to fix this
	int64_t low  = (hv_conf.min_value / hv_conf.unit_size).nsec;
	if (low <= 0) // fix common param incompatibility (does not affect semantics really)
		low = 1;

	int64_t const high = (hv_conf.max_value / hv_conf.unit_size).nsec;
	return hdr_histogram_configure(conf, low, high, hv_conf.precision_bits);
}

////////////////////////////////////////////////////////////////////////////////////////////////
// general funcs

inline flat_histogram_t histogram___convert_hdr_to_flat(hdr_histogram_t const& hdr, histogram_conf_t const& conf)
{
	flat_histogram_t flat;

	flat.value_count  = hdr.total_count();
	flat.negative_inf = hdr.negative_inf();
	flat.positive_inf = hdr.positive_inf();

	flat.values.clear();
	flat.values.resize(hdr.counts_nonzero()); // this does useless zero init

	auto const counts_r = hdr.get_counts_range();

	uint32_t read_position = 0;
	for (uint32_t i = 0; i < hdr.counts_nonzero(); i++)
	{
		while (counts_r[read_position] == 0)
			read_position++;

		histogram_value_t& hv_value = flat.values[i];
		hv_value.bucket_id = (uint32_t) hdr.value_at_index(read_position); // gotta cast here, since flat_histogram_t uses uint32_t
		hv_value.value     = counts_r[read_position];
		read_position++;
	}

	assert(read_position <= counts_r.size());

#if 0
	size_t insert_position = 0;
	for (size_t i = 0; i < counts_r.size(); i++)
	{
		uint32_t const count = counts_r[i];
		if (count == 0) // XXX: predicts badly, no idea how to improve
			continue;

		// uint32_t const bucket_id = (uint32_t) hdr.value_at_index(i);
		// flat.values.push_back({ .bucket_id = bucket_id, .value = count });

		histogram_value_t& hv_value = flat.values[insert_position++];
		hv_value.bucket_id = (uint32_t) hdr.value_at_index(i); // gotta cast here, since flat_histogram_t uses uint32_t
		hv_value.value     = count;
	}

	assert(insert_position == hdr.counts_nonzero()); // sanity
#endif

	return flat;
}

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__HISTOGRAM_H_

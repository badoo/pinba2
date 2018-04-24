#ifndef PINBA__HISTOGRAM_H_
#define PINBA__HISTOGRAM_H_

#include <cstdint>
#include <cmath>   // ceil
#include <utility> // c++11 swap

#include <boost/noncopyable.hpp>
// #include <sparsehash/sparse_hash_map>
#include <sparsehash/dense_hash_map>

#include "t1ha/t1ha.h"
// #include "sparsepp/spp.h"

#include "pinba/limits.h"

////////////////////////////////////////////////////////////////////////////////////////////////

struct histogram_conf_t
{
	uint32_t    bucket_count;
	duration_t  bucket_d;
	duration_t  min_value;
};

struct histogram_hasher_t // TODO: try std::hash here, should be good for uint32_t
{
	inline uint64_t operator()(uint32_t const v) const
	{
		return t1ha0(&v, sizeof(v), v);
	}
};

struct histogram_t
	// FIXME: put this back, when report internal row structures, containing this one by value, stop being copyable
	// : private boost::noncopyable
{
	// map time_interval -> request_count
	//
	// sparse_hash_map is said to be more efficient memory-wise
	// and we kinda need that, since there is a histogram per row per timeslice
	//
	// TODO: a couple of ideas on tuning this
	//  1. maybe make hash map impl configureable, so that if report has few rows
	//     a faster hash map can be use (like dense_hash_map)
	//  2. experiment with hash function for integers here (maybe a simple identity will work?)
	//  3. control hash map grows as much as possible, to save memory (at least use small initial size)
	//  4. keep as little state in this object as possible to save memory
	//     all confguration information (like histogram max size and hash initial size can be passed from outside)
	//  5. maybe use memory pools to allocate objects like this one (x84_64)
	//     sizeof(unordered_map) == 56    // as of gcc 4.9.4
	//     sizeof(dense_hash_map) == 80
	//     sizeof(sparse_hash_map) == 88
	//  6. try https://github.com/greg7mdp/sparsepp (within 1% of sparse_hash_map by mem usage, but faster lookup/insert)
	//     sizeof(spp::sparse_hash_map) == 88
	//  7. sparse_hash_set should use less memory due to using uint32_t but expect 64bit alignments?
	//  8. maybe use google in-memory btree, since we need to sort to calc percentiles,
	//     or do N a zillion hash lookups to traverse hashmap in a 'sorted' way by key
	//
	// typedef google::sparse_hash_map<uint32_t, uint32_t, histogram_hasher_t> map_t;
	// typedef spp::sparse_hash_map<uint32_t, uint32_t, histogram_hasher_t> map_t;

	struct map_t
		: public google::dense_hash_map<uint32_t, uint32_t, histogram_hasher_t>
	{
		map_t()
		{
			this->set_empty_key(PINBA_INTERNAL___EMPTY_HV_BUCKET_ID);
		}
	};

private:
	map_t     map_;
	uint32_t  total_count_; // total number of measurements, includes inf
	uint32_t  negative_inf_;
	uint32_t  positive_inf_;

public:

	histogram_t()
		: map_()
		, total_count_(0)
		, negative_inf_(0)
		, positive_inf_(0)
	{
	}

	histogram_t(histogram_t const& other)
		: map_(other.map_)
		, total_count_(other.total_count_)
		, negative_inf_(other.negative_inf_)
		, positive_inf_(other.positive_inf_)
	{
	}

	histogram_t(histogram_t&& other) noexcept
		: histogram_t()
	{
		(*this) = std::move(other); // move assign
	}

	void operator=(histogram_t&& other) noexcept
	{
		map_.swap(other.map_);
		std::swap(total_count_, other.total_count_);
		std::swap(negative_inf_, other.negative_inf_);
		std::swap(positive_inf_, other.positive_inf_);
	}

	void clear()
	{
		map_.clear();
		map_.resize(0);

		total_count_  = 0;
		negative_inf_ = 0;
		positive_inf_ = 0;
	}

	map_t const& map_cref() const noexcept
	{
		return map_;
	}

	uint32_t total_count() const noexcept
	{
		return total_count_;
	}

	uint32_t negative_inf() const noexcept
	{
		return negative_inf_;
	}

	uint32_t positive_inf() const noexcept
	{
		return positive_inf_;
	}

	void merge_other(histogram_t const& other)
	{
		for (auto const& pair : other.map_)
			map_[pair.first] += pair.second;

		total_count_ += other.total_count_;
		negative_inf_ += other.negative_inf_;
		positive_inf_ += other.positive_inf_;
	}

	void increment(histogram_conf_t const& conf, duration_t d, uint32_t increment_by = 1)
	{
		// buckets are open on the left, and closed on the right side, i.e. "(from, to]"
		// [negative_inf]   -> (-inf, min_value]
		// [0]              -> (min_value, min_value+bucket_d]
		// [1]              -> (min_value+bucket_d, min_value+bucket_d*2]
		// ....
		// [bucket_count-1] -> (min_value+bucket_d*(bucket_count-1), min_value+bucket_d*bucket_count]
		// [positive_inf]   -> (max_value, +inf)

		d -= conf.min_value;

		if (d.nsec <= 0) // <=, since we fit upper-bound match to prev bucket
		{
			negative_inf_ += increment_by;
		}
		else
		{
			uint32_t bucket_id = d.nsec / conf.bucket_d.nsec;

			if (bucket_id < conf.bucket_count)
			{
				// try fit exact upper-bound match to previous bucket
				if (bucket_id > 0)
				{
					if (d == bucket_id * conf.bucket_d)
						bucket_id -= 1;
				}

				map_[bucket_id] += increment_by;
			}
			else
			{
				positive_inf_ += increment_by;
			}
		}

		total_count_ += increment_by;
	}
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
	uint32_t            total_count;
	uint32_t            negative_inf;
	uint32_t            positive_inf;
};
static_assert(sizeof(flat_histogram_t) == (sizeof(histogram_values_t)+4*sizeof(uint32_t)), "flat_histogram_t must have no padding");

inline void histogram___convert_ht_to_flat(histogram_t const& ht, flat_histogram_t *flat)
{
	flat->total_count  = ht.total_count();
	flat->negative_inf = ht.negative_inf();
	flat->positive_inf = ht.positive_inf();

	flat->values.clear();
	flat->values.reserve(ht.map_cref().size());

	for (auto const& ht_pair : ht.map_cref())
		flat->values.push_back({ .bucket_id = ht_pair.first, .value = ht_pair.second });

	std::sort(flat->values.begin(), flat->values.end());
}

inline flat_histogram_t histogram___convert_ht_to_flat(histogram_t const& ht)
{
	flat_histogram_t result;
	histogram___convert_ht_to_flat(ht, &result);
	return result;
}

////////////////////////////////////////////////////////////////////////////////////////////////

inline duration_t get_percentile(histogram_t const& hv, histogram_conf_t const& conf, double percentile)
{
	if (percentile == 0.)
		return conf.min_value;

	if (hv.total_count() == 0) // no values in histogram, nothing to do
		return conf.min_value;

	uint32_t required_sum = [&]()
	{
		uint32_t const res = std::ceil(hv.total_count() * percentile / 100.0);
		return (res > hv.total_count()) ? hv.total_count() : res;
	}();

	// ff::fmt(stdout, "{0}({1}); total: {2}, required: {3}\n", __func__, percentile, hv.total_count(), required_sum);

	// fastpath - are we in negative_inf?, <= here !
	if (required_sum <= hv.negative_inf())
		return conf.min_value;

	// fastpath - are we going to hit positive_inf?
	if (required_sum > (hv.total_count() - hv.positive_inf()))
		return conf.min_value + conf.bucket_d * conf.bucket_count;

	// already past negative_inf, adjust
	required_sum -= hv.negative_inf();


	// slowpath - shut up and calculate
	auto const& map = hv.map_cref();
	auto const map_end = map.end();

	uint32_t current_sum = 0;

	for (uint32_t bucket_id = 0; bucket_id < conf.bucket_count; bucket_id++)
	{
		auto const it = map.find(bucket_id);
		if (it == map_end)
		{
			// ff::fmt(stdout, "[{0}] empty; current_sum = {1}\n", bucket_id, current_sum);
			continue;
		}

		uint32_t const next_has_values = it->second;
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

	// dump map contents to stderr, as we're going to die anyway
	{
		ff::fmt(stderr, "{0} internal failure, dumping histogram\n", __func__);
		ff::fmt(stderr, "{0} neg_inf: {1}, pos_inf: {2}, total_count: {3}, hv_size: {4}\n",
			__func__, hv.negative_inf(), hv.positive_inf(), hv.total_count(), hv.map_cref().size());

		for (auto const& pair : map)
			ff::fmt(stderr, "  [{0}] -> {1}\n", pair.first, pair.second);
	}

	assert(!"must not be reached");
}


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
		return conf.min_value + conf.bucket_d * conf.bucket_count;

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

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__HISTOGRAM_H_

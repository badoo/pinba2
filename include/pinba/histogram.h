#ifndef PINBA__HISTOGRAM_H_
#define PINBA__HISTOGRAM_H_

#include <cstdint>
#include <cmath>   // ceil
#include <utility> // c++11 swap

#include <boost/noncopyable.hpp>
// #include <sparsehash/sparse_hash_map>

#include "t1ha/t1ha.h"
#include "sparsepp/spp.h"

////////////////////////////////////////////////////////////////////////////////////////////////

struct histogram_conf_t
{
	uint32_t    bucket_count;
	duration_t  bucket_d;
};

struct histogram_hasher_t // TODO: try std::hash here, should be good for uint32_t
{
	inline uint64_t operator()(uint32_t const v) const
	{
		return t1ha0(&v, sizeof(v), 0);
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
	typedef spp::sparse_hash_map<uint32_t, uint32_t, histogram_hasher_t> map_t;

private:
	map_t     map_;
	uint32_t  items_total_;
	uint32_t  inf_value_;

public:

	histogram_t()
		: map_()
		, items_total_(0)
		, inf_value_(0)
	{
	}

	histogram_t(histogram_t const& other)
		: map_(other.map_)
		, items_total_(other.items_total_)
		, inf_value_(other.inf_value_)
	{
	}

	histogram_t(histogram_t&& other)
	{
		(*this) = std::move(other); // move assign
	}

	void operator=(histogram_t&& other)
	{
		map_.swap(other.map_);
		std::swap(items_total_, other.items_total_);
		std::swap(inf_value_, other.inf_value_);
	}

	map_t const& map_cref() const
	{
		return map_;
	}

	uint32_t items_total() const
	{
		return items_total_;
	}

	uint32_t bucket_value(uint32_t id) const
	{
		auto const it = map_.find(id);
		return (it == map_.end())
				? 0
				: it->second;
	}

	uint32_t inf_value() const
	{
		return inf_value_;
	}

	void merge_other(histogram_t const& other)
	{
		for (auto const& pair : other.map_)
			map_[pair.first] += pair.second;

		items_total_ += other.items_total_;
		inf_value_ += other.inf_value_;
	}

	void increment(histogram_conf_t const& conf, duration_t d, uint32_t increment_by = 1)
	{
		uint32_t const bucket_id = d.nsec / conf.bucket_d.nsec;

		if (bucket_id < conf.bucket_count)
			map_[bucket_id] += increment_by;
		else
			inf_value_ += increment_by;

		items_total_ += increment_by;
	}

	void increment_bucket(uint32_t bucket_id, uint32_t increment_by)
	{
		map_[bucket_id] += increment_by;
		items_total_ += increment_by;
	}

	void increment_inf(uint32_t increment_by)
	{
		inf_value_ += increment_by;
	}
};

////////////////////////////////////////////////////////////////////////////////////////////////
// plain sorted-array histograms, should be faster to merge and calculate percentiles from

struct histogram_value_t
{
	uint32_t bucket_id;
	uint32_t value;

	// histogram_value_t()
	// 	: bucket_id(0)
	// 	, value(0)
	// {
	// }

	inline bool operator<(histogram_value_t const& other) const
	{
		return bucket_id < other.bucket_id;
	}
};
static_assert(sizeof(histogram_value_t) == sizeof(uint64_t), "histogram_value_t must have no padding");

typedef std::vector<histogram_value_t> histogram_values_t;

struct flat_histogram_t
{
	histogram_values_t  values;
	uint32_t            total_value;
	uint32_t            inf_value;

	// flat_histogram_t(flat_histogram_t&& other)
	// {
	// 	(*this) = std::move(other);
	// }

	// flat_histogram_t& operator=(flat_histogram_t&& other)
	// {
	// 	using std::swap;
	// 	swap(values, other.values);
	// 	swap(total_value, other.total_value);
	// 	swap(inf_value, other.inf_value);
	// 	return *this;
	// }
};

////////////////////////////////////////////////////////////////////////////////////////////////

inline duration_t get_percentile(histogram_t const& hv, histogram_conf_t const& conf, double percentile)
{
	if (percentile == 0.) // 0'th percentile is always 0
		return {0};

	uint32_t const required_sum = [&]()
	{
		uint32_t const res = std::ceil(hv.items_total() * percentile / 100.0);

		return (res > hv.items_total())
				? hv.items_total()
				: res;
	}();

	// ff::fmt(stdout, "{0}({1}); total: {2}, required: {3}\n", __func__, percentile, hv.items_total(), required_sum);

	// fastpath - are we going to hit infinity bucket?
	if (required_sum > (hv.items_total() - hv.inf_value()))
	{
		// ff::fmt(stdout, "[{0}] inf fastpath, need {1}, got histogram for {2} values\n",
		// 	bucket_id, required_sum, (hv.items_total() - hv.inf_value()));
		return conf.bucket_d * conf.bucket_count;
	}


	auto const& map = hv.map_cref();
	auto const map_end = map.end();

	uint32_t current_sum = 0;
	uint32_t bucket_id   = 0;

	for (; current_sum < required_sum; bucket_id++)
	{
		// infinity checked before the loop
		assert(bucket_id < conf.bucket_count);

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
			return conf.bucket_d * (bucket_id + 1);
		}

		// incomplete bucket, interpolate, assuming flat time distribution within bucket
		{
			duration_t const d = conf.bucket_d * need_values / next_has_values;

			// ff::fmt(stdout, "[{0}] last, has: {1}, taking: {2}, {3}\n", bucket_id, next_has_values, need_values, d);
			return conf.bucket_d * bucket_id + d;
		}
	}

	assert(!"must not be reached");
	// return conf.bucket_d * conf.bucket_count;
}


inline duration_t get_percentile(flat_histogram_t const& hv, histogram_conf_t const& conf, double percentile)
{
	if (percentile == 0.) // 0'th percentile is always 0
		return {0};

	uint32_t const required_sum = [&]()
	{
		uint32_t const res = std::ceil(hv.total_value * percentile / 100.0);

		return (res > hv.total_value)
				? hv.total_value
				: res;
	}();

	// ff::fmt(stdout, "{0}({1}); total: {2}, required: {3}\n", __func__, percentile, hv.total_value, required_sum);

	// fastpath - are we going to hit infinity bucket?
	if (required_sum > (hv.total_value - hv.inf_value))
	{
		// ff::fmt(stdout, "inf fastpath; need {0}, got histogram for {1} values\n",
		// 	required_sum, (hv.total_value - hv.inf_value));
		return conf.bucket_d * conf.bucket_count;
	}

	uint32_t current_sum = 0;
	uint32_t bucket_id   = 0;

	for (auto const& item : hv.values)
	{
		bucket_id = item.bucket_id;

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
			return conf.bucket_d * (bucket_id + 1);
		}

		// incomplete bucket, interpolate, assuming flat time distribution within bucket
		{
			duration_t const d = conf.bucket_d * need_values / next_has_values;

			// ff::fmt(stdout, "[{0}] last, has: {1}, taking: {2}, {3}\n", bucket_id, next_has_values, need_values, d);
			return conf.bucket_d * bucket_id + d;
		}
	}

	assert(!"must not be reached");
	// return conf.bucket_d * conf.bucket_count;
}

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__HISTOGRAM_H_

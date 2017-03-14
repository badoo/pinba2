#include <algorithm>
#include <vector>

#include "pinba/globals.h"
#include "pinba/histogram.h"

////////////////////////////////////////////////////////////////////////////////////////////////

struct histogram_item_t
{
	uint64_t kv;
	uint32_t key() const { return uint32_t(kv >> 32); }
	uint32_t value() const { return uint32_t(kv & 0xFFFFFFFF); }
};

inline bool operator<(histogram_item_t const& l, histogram_item_t const& r)
{
	return l.key() < r.key();
}

struct hv_items_t
{
	uint32_t items_total;
	uint32_t inf_value;
	std::vector<histogram_item_t> items;
};

inline duration_t get_percentile_flat(hv_items_t const& hv, histogram_conf_t const& conf, double percentile)
{
	if (percentile == 0.) // 0'th percentile is always 0
		return {0};

	uint32_t const required_sum = [&]()
	{
		uint32_t const res = std::ceil(hv.items_total * percentile / 100.0);

		return (res > hv.items_total)
				? hv.items_total
				: res;
	}();

	// ff::fmt(stdout, "{0}({1}); total: {2}, required: {3}\n", __func__, percentile, hv.items_total, required_sum);

	// fastpath - are we going to hit infinity bucket?
	if (required_sum > (hv.items_total - hv.inf_value))
	{
		// ff::fmt(stdout, "inf fastpath; need {0}, got histogram for {1} values\n",
		// 	required_sum, (hv.items_total - hv.inf_value));
		return conf.bucket_d * conf.bucket_count;
	}

	uint32_t current_sum = 0;
	uint32_t bucket_id   = 0;

	// for (; current_sum < required_sum; bucket_id++)
	for (auto const& item : hv.items)
	{
		bucket_id = item.key();

		uint32_t const next_has_values = item.value();
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
#if 0
struct merge_item_t
{
	uint32_t src; // string offset in data
	uint32_t off; // offset in string, UINT_MAX is 'nothing left'
};

std::string multi_merge(std::vector<std::string> const& data)
{
	std::string result = {};
	uint32_t result_length = 0; // init later

	merge_item_t tmp[data.size()];
	merge_item_t *tmp_end = tmp + data.size();

	auto const item_greater = [&data](merge_item_t const& l, merge_item_t const& r)
	{
		return data[l.src][l.off] > data[r.src][r.off];
	};

	auto const item_equal = [&data](merge_item_t const& l, merge_item_t const& r)
	{
		return data[l.src][l.off] == data[r.src][r.off];
	};

	for (uint32_t i = 0; i < data.size(); i++)
	{
		tmp[i] = { i, 0 };
		result_length += data[i].size();
	}

	std::make_heap(tmp, tmp_end, item_greater);

	while (result_length > 0)
	{
		// ff::fmt(stdout, "result_length = {0}\n", result_length);

		std::pop_heap(tmp, tmp_end, item_greater);
		merge_item_t *last = tmp_end - 1;

		printf("last = { %d, %d '%c' }\n", last->src, last->off, data[last->src][last->off]);

		auto const v = data[last->src][last->off];

		if (v == result.back())
		{
			result.back() = '-';
		}
		else
		{
			result.push_back(v);
		}

		--result_length;

		if ((last->off + 1) < data[last->src].size())
		{
			*last = { last->src, last->off + 1 };
			std::push_heap(tmp, tmp_end, item_greater);
		}
		else
		{
			--tmp_end;
		}
	}

	return result;
}
#endif

template<class I>
struct merge_heap_item_t
{
	I value_iter;  // current value iterator
	I end_iter;    // end iterator
};

template<class Container>
inline void maybe_reserve(Container& cont, size_t sz) { /* nothing */ }

template<class T>
inline void maybe_reserve(std::vector<T>& cont, size_t sz) { cont.reserve(sz); }

// merge a range (defined by 'begin' and 'end') of *sorted* ranges into 'result'
// 'result' shall also be sorted and should support emplace(iterator, value)
// 'compare_fn' should return <0 for less, ==0 for equal, >0 for greater
// 'merge_fn' is used to merge equal elements
template<class ResultContainer, class Iterator, class Compare, class Merge>
static inline void multi_merge(ResultContainer& result, Iterator begin, Iterator end, Compare const& compare_fn, Merge const& merge_fn)
{
	using merge_item_t = merge_heap_item_t<typename Iterator::value_type::const_iterator>;

	auto const item_greater = [&](merge_item_t const& l, merge_item_t const& r)
	{
		return compare_fn(*l.value_iter, *r.value_iter) > 0;
	};

	size_t           result_length = 0; // initialized later
	size_t const     input_size = std::distance(begin, end);

	merge_item_t tmp[input_size];
	merge_item_t *tmp_end = tmp + input_size;

	// init
	{
		size_t offset = 0;
		for (auto i = begin; i != end; i = std::next(i))
		{
			auto const curr_b = std::begin(*i);
			auto const curr_e = std::end(*i);
			result_length += std::distance(curr_b, curr_e);
			tmp[offset++] = { curr_b, curr_e };
		}
	}

	maybe_reserve(result, result_length);

	std::make_heap(tmp, tmp_end, item_greater);

	while (result_length > 0)
	{
		std::pop_heap(tmp, tmp_end, item_greater);
		merge_item_t *last = tmp_end - 1;

		// make room for next item, if it's not there
		if (compare_fn(result.back(), *last->value_iter) != 0)
		{
			result.push_back({}); // emplace :(
		}

		// merge current into existing (possibly empty item)
		merge_fn(result.back(), *last->value_iter);

		--result_length;

		// advance to next item if exists
		auto const next_it = std::next(last->value_iter);
		if (next_it != last->end_iter)
		{
			last->value_iter = next_it;
			std::push_heap(tmp, tmp_end, item_greater);
		}
		else
		{
			--tmp_end;
		}
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char const *argv[])
{
	histogram_t hv;
	histogram_conf_t const hv_conf = {
		.bucket_count = 100,
		.bucket_d     = 1 * d_millisecond,
	};

	hv.increment(hv_conf, 2 * d_millisecond, 1);
	hv.increment(hv_conf, 3 * d_millisecond, 4);
	hv.increment(hv_conf, 4 * d_millisecond, 10);
	hv.increment(hv_conf, 5 * d_millisecond, 50);
	hv.increment(hv_conf, 6 * d_millisecond, 25);
	hv.increment(hv_conf, 7 * d_millisecond, 11);
	hv.increment(hv_conf, 8 * d_millisecond, 1);
	hv.increment(hv_conf, 1000 * d_millisecond, 1);

	auto const print_percentile = [&](double percentile)
	{
		hv_items_t hvi;
		hvi.items_total = hv.items_total();
		hvi.inf_value = hv.inf_value();

		// std::copy(hv.map_cref().begin(), hv.map_cref().end(), std::back_inserter(hvi.items));
		for (auto const& pair : hv.map_cref())
			hvi.items.push_back(histogram_item_t{uint64_t(pair.first) << 32 | pair.second});
		std::sort(hvi.items.begin(), hvi.items.end());

		ff::fmt(stdout, "percentile_flat {0} = {1}, {2}\n",
			percentile, get_percentile(hv, hv_conf, percentile), get_percentile_flat(hvi, hv_conf, percentile));
	};

	print_percentile(0);
	print_percentile(50);
	print_percentile(75);
	print_percentile(99);
	print_percentile(99.9);
	print_percentile(100);

	{
		std::string result;
		std::vector<std::string> const strings = {"3467778", "112YYZZabdd"};
		multi_merge(result, std::begin(strings), std::end(strings),
			[](unsigned char const l, unsigned char const r) { return (l < r) ? -1 : (r < l) ? 1 : 0; },
			[](char& l, char const& r) { l = (l != 0) ? '-' : r; });
		ff::fmt(stdout, "multi_merge = '{0}'\n", result);
	}

	return 0;
}

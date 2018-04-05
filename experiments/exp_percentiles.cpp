#include <algorithm>
#include <vector>
#include <deque>

#include <meow/str_ref.hpp>

#include "pinba/globals.h"
#include "pinba/histogram.h"


////////////////////////////////////////////////////////////////////////////////////////////////

// struct histogram_value_t
// {
// 	uint32_t bucket_id;
// 	uint32_t value;
// };
// static_assert(sizeof(histogram_value_t) == sizeof(uint64_t), "histogram_value_t must have no padding");

// inline constexpr bool operator<(histogram_value_t const& l, histogram_value_t const& r)
// {
// 	return l.bucket_id < r.bucket_id;
// }

using hvalues_ref_t = meow::string_ref<histogram_value_t>;

struct histogram_husk_t
{
	hvalues_ref_t  values;
	uint32_t       value_count;
	uint32_t       negative_inf;
	uint32_t       positive_inf;
};
static_assert(sizeof(histogram_husk_t) == (sizeof(hvalues_ref_t)+4*sizeof(uint32_t)), "histogram_husk_t must have no padding");

struct hv_storage_t
{
	std::vector<histogram_husk_t>   hvs;
	std::vector<histogram_value_t>  values_store;
};

void hv_storage___from_histograms_range(hv_storage_t *hv_storage, meow::string_ref<histogram_t const> hv_range)
{
	if (hv_range.empty())
		return;

	hv_storage->hvs.reserve(hv_range.size());

	size_t hv_values_count = 0;
	for (auto const& hv : hv_range)
	{
		histogram_husk_t flat_hv = {
			.values       = {},
			.value_count  = hv.value_count(),
			.negative_inf = hv.negative_inf(),
			.positive_inf = hv.positive_inf(),
		};

		hv_storage->hvs.emplace_back(flat_hv);

		hv_values_count += hv.map_cref().size();
	}

	// make sure values_store is never reallocated
	hv_storage->values_store.reserve(hv_values_count);

	// copy all histogram values into single contiguous array
	size_t hv_off = 0;
	for (auto const& hv : hv_range)
	{
		auto const inserted_begin = hv_storage->values_store.end();

		for (auto const& ht_pair : hv.map_cref())
			hv_storage->values_store.push_back({ .bucket_id = ht_pair.first, .value = ht_pair.second });

		// now fixup histogram husk to use newly inserted elts and sort
		histogram_husk_t *flat_hv = &hv_storage->hvs[hv_off++];
		flat_hv->values = hvalues_ref_t { &*inserted_begin, hv.map_cref().size() };
		std::sort(flat_hv->values.begin(), flat_hv->values.end());
	}

	// now fix pointers and sort
	// histogram_value_t *hvalue_ptr = &*hv_storage->values_store.begin();
	// for (auto& flat_hv : hv_storage->hvs)
	// {
	// 	flat_hv.values = hvalues_ref_t { hvalue_ptr, flat_hv.value_count };
	// 	hvalue_ptr += flat_hv.value_count;

	// 	std::sort(flat_hv.values.begin(), flat_hv.values.end());
	// }
}

hv_storage_t hv_storage___from_histograms_range(meow::string_ref<histogram_t const> hv_range)
{
	hv_storage_t result;
	hv_storage___from_histograms_range(&result, hv_range);
	return result;
}

////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char const *argv[])
{
	histogram_conf_t const hv_conf = {
		.bucket_count = 100,
		.bucket_d     = 1 * d_millisecond,
	};

	std::vector<histogram_t> hvs;
	for (size_t i = 0; i < 100; i++)
	{
		histogram_t hv;
		hv.increment(hv_conf, 2 * d_millisecond, 1);
		hv.increment(hv_conf, 3 * d_millisecond, 3);
		hv.increment(hv_conf, 4 * d_millisecond, 4);

		hvs.emplace_back(std::move(hv));
	}

	auto const hv_storage = hv_storage___from_histograms_range({ &*hvs.begin(), hvs.size() });


	// histogram_t hv;
	// // hv.increment(hv_conf, 2 * d_millisecond, 1);
	// // hv.increment(hv_conf, 3 * d_millisecond, 4);
	// // hv.increment(hv_conf, 4 * d_millisecond, 10);
	// // hv.increment(hv_conf, 5 * d_millisecond, 50);
	// hv.increment(hv_conf, 6 * d_millisecond, 25000);
	// hv.increment(hv_conf, 7 * d_millisecond, 1000);
	// hv.increment(hv_conf, 8 * d_millisecond, 30);
	// hv.increment(hv_conf, 1000 * d_millisecond, 1);

	// auto const print_percentile = [&](double percentile)
	// {
	// 	flat_histogram_t flat_hv;
	// 	histogram___convert_ht_to_flat(hv, &flat_hv);

	// 	ff::fmt(stdout, "percentile {0} = hash: {1}, flat: {2}\n\n",
	// 		percentile, get_percentile(hv, hv_conf, percentile), get_percentile(flat_hv, hv_conf, percentile));
	// };

	// // print_percentile(0);
	// print_percentile(50);
	// print_percentile(75);
	// print_percentile(95);
	// print_percentile(99);
	// print_percentile(99.9);
	// print_percentile(99.99);
	// print_percentile(99.999);
	// print_percentile(100);

	return 0;
}

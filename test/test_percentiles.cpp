#include "pinba/globals.h"
#include "pinba/histogram.h"

////////////////////////////////////////////////////////////////////////////////////////////////
#if 0
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

	ff::fmt(stdout, "{0}({1}); total: {2}, required: {3}\n", __func__, percentile, hv.items_total(), required_sum);

	// fastpath - are we going to hit infinity bucket?
	if (required_sum > (hv.items_total() - hv.inf_value()))
	{
		ff::fmt(stdout, "[{0}] inf fastpath, need {1}, got histogram for {2} values\n",
			bucket_id, required_sum, (hv.items_total() - hv.inf_value()));
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
			ff::fmt(stdout, "[{0}] current_sum +=; {1} -> {2}\n", bucket_id, next_has_values, current_sum);
			continue;
		}

		if (next_has_values == need_values) // complete bucket, return upper time bound for this bucket
		{
			ff::fmt(stdout, "[{0}] full; current_sum +=; {1} -> {2}\n", bucket_id, next_has_values, current_sum);
			return conf.bucket_d * (bucket_id + 1);
		}

		// incomplete bucket, interpolate, assuming flat time distribution within bucket
		{
			duration_t const d = conf.bucket_d * need_values / next_has_values;

			ff::fmt(stdout, "[{0}] last, has: {1}, taking: {2}, {3}\n", bucket_id, next_has_values, need_values, d);
			return conf.bucket_d * bucket_id + d;
		}
	}

	assert(!"must not be reached");
	// return conf.bucket_d * conf.bucket_count;
}
#endif
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
		ff::fmt(stdout, "percentile {0} = {1}\n", percentile, get_percentile(hv, hv_conf, percentile));
	};

	print_percentile(0);
	print_percentile(50);
	print_percentile(75);
	print_percentile(99);
	print_percentile(99.9);
	print_percentile(100);

	return 0;
}

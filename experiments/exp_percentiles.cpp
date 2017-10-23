#include <algorithm>
#include <vector>

#include "pinba/globals.h"
#include "pinba/histogram.h"

////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char const *argv[])
{
	histogram_conf_t const hv_conf = {
		.bucket_count = 100,
		.bucket_d     = 1 * d_millisecond,
	};

	histogram_t hv;
	// hv.increment(hv_conf, 2 * d_millisecond, 1);
	// hv.increment(hv_conf, 3 * d_millisecond, 4);
	// hv.increment(hv_conf, 4 * d_millisecond, 10);
	// hv.increment(hv_conf, 5 * d_millisecond, 50);
	hv.increment(hv_conf, 6 * d_millisecond, 25000);
	hv.increment(hv_conf, 7 * d_millisecond, 1000);
	hv.increment(hv_conf, 8 * d_millisecond, 30);
	hv.increment(hv_conf, 1000 * d_millisecond, 1);

	auto const print_percentile = [&](double percentile)
	{
		flat_histogram_t flat_hv;
		histogram___convert_ht_to_flat(hv, &flat_hv);

		ff::fmt(stdout, "percentile {0} = hash: {1}, flat: {2}\n\n",
			percentile, get_percentile(hv, hv_conf, percentile), get_percentile(flat_hv, hv_conf, percentile));
	};

	// print_percentile(0);
	print_percentile(50);
	print_percentile(75);
	print_percentile(95);
	print_percentile(99);
	print_percentile(99.9);
	print_percentile(99.99);
	print_percentile(99.999);
	print_percentile(100);

	return 0;
}

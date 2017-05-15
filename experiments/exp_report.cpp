#include <memory>
#include <algorithm>
#include <vector>
#include <unordered_map>
#include <type_traits>

#include <boost/noncopyable.hpp>

#include <sparsehash/dense_hash_map>

#include <meow/stopwatch.hpp>
#include <meow/hash/hash.hpp>
#include <meow/hash/hash_impl.hpp>
#include <meow/format/format_to_string.hpp>

#include "pinba/globals.h"
#include "pinba/dictionary.h"
#include "pinba/histogram.h"
#include "pinba/packet.h"
#include "pinba/report.h"
#include "pinba/report_util.h"
#include "pinba/report_by_packet.h"
#include "pinba/report_by_request.h"
#include "pinba/report_by_timer.h"
#include "pinba/multi_merge.h"

////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char const *argv[])
try
{
	pinba_options_t options = {};
	pinba_globals_t *globals = pinba_globals_init(&options);

	auto packet_data = packet_t {
		.host_id = 1,
		.server_id = 0,
		.script_id = 7,
		.schema_id = 0,
		.status = 0,
		.doc_size = 9999,
		.memory_peak = 1,
		.tag_count = 0,
		.timer_count = 0,
		.request_time = duration_t{ 15 * msec_in_sec },
		.ru_utime = duration_t{ 3 * msec_in_sec },
		.ru_stime = duration_t{ 1 * msec_in_sec },
		.dictionary = NULL,
		.tag_name_ids = NULL,
		.tag_value_ids = NULL,
		.timers = NULL,
	};

	packet_t *packet = &packet_data;

	if (argc >= 2)
	{
		FILE *f = fopen(argv[1], "r");
		uint8_t buf[16 * 1024];
		size_t n = fread(buf, 1, sizeof(buf), f);

		auto request = pinba__request__unpack(NULL, n, buf);
		if (!request)
			throw std::runtime_error("request unpack error");

		struct nmpa_s nmpa;
		nmpa_init(&nmpa, 1024);

		packet = pinba_request_to_packet(request, globals->dictionary(), &nmpa);

		debug_dump_packet(stdout, packet);
	}


	report_conf___by_timer_t const rconf_timer = [&]()
	{
		report_conf___by_timer_t conf = {};
		conf.time_window     = 5 * d_second,
		conf.tick_count      = 5;
		conf.hv_bucket_d     = 1 * d_microsecond;
		conf.hv_bucket_count = 1 * 1000 * 1000;

		// conf.min_time = 100 * d_millisecond;
		// conf.max_time = 300 * d_millisecond;

		conf.filters = {
			// {
			// 	"request_time/>=100ms/<300ms",
			// 	[](packet_t *packet)
			// 	{
			// 		static constexpr duration_t const min_time = 100 * d_millisecond;
			// 		static constexpr duration_t const max_time = 300 * d_millisecond;

			// 		return (packet->request_time >= min_time && packet->request_time < max_time);
			// 	},
			// },
		};

		conf.keys.push_back(report_conf___by_timer_t::key_descriptor_by_request_field("script_name", &packet_t::script_id));
		conf.keys.push_back(report_conf___by_timer_t::key_descriptor_by_timer_tag("group", globals->dictionary()->get_or_add("group")));
		// conf.keys.push_back(make_rtag_kd("type"));
		// conf.keys.push_back(make_rfield_kd("script_name", &packet_t::script_id));
		// conf.keys.push_back(make_rfield_kd("server_name", &packet_t::server_id));
		// conf.keys.push_back(make_rfield_kd("host_name", &packet_t::host_id));
		conf.keys.push_back(report_conf___by_timer_t::key_descriptor_by_timer_tag("server", globals->dictionary()->get_or_add("server")));

		return conf;
	}();

	report_stats_t rstats_timer;

	auto report = create_report_by_timer(globals, rconf_timer);
	report->stats_init(&rstats_timer);
	report->ticks_init(os_unix::clock_monotonic_now());

	report->add(packet);
	report->tick_now(os_unix::clock_monotonic_now());
	// report->serialize(stdout, "first");

	report->add(packet);
	report->add(packet);
	report->add(packet);
	report->tick_now(os_unix::clock_monotonic_now());
	// report->serialize(stdout, "second");

	{
		auto const snapshot = report->get_snapshot();

		// report->serialize(stdout, "second_nochange"); // snapshot should not change

		debug_dump_report_snapshot(stdout, snapshot.get(), "snapshot_1");
	}

	report->add(packet);
	report->add(packet);
	report->tick_now(os_unix::clock_monotonic_now());
	// report->serialize(stdout, "third");
	report->tick_now(os_unix::clock_monotonic_now());
	report->tick_now(os_unix::clock_monotonic_now());
	report->tick_now(os_unix::clock_monotonic_now());
	report->tick_now(os_unix::clock_monotonic_now());
	report->tick_now(os_unix::clock_monotonic_now());
	// report->serialize(stdout, "+3");

	{
		auto snapshot = report->get_snapshot();
		debug_dump_report_snapshot(stdout, snapshot.get(), "snapshot_2");
	}

	return 0;
}
catch (std::exception const& e)
{
	ff::fmt(stderr, "error: {0}\n", e.what());
	return 1;
}

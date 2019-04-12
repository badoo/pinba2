#include <fcntl.h>
#include <sys/types.h>

#include <stdexcept>
#include <thread>

#include <meow/stopwatch.hpp>

#include <nanomsg/nn.h> // nn_term

#include "pinba/globals.h"
#include "pinba/engine.h"
#include "pinba/dictionary.h"
#include "pinba/histogram.h"
#include "pinba/report_by_request.h"
#include "pinba/report_by_timer.h"

////////////////////////////////////////////////////////////////////////////////////////////////

#if !defined(PINBA_BUILD_MYSQL_MODULE) || (PINBA_BUILD_MYSQL_MODULE == 0)

int main(int argc, char const *argv[])
{
	pinba_options_t options = {
		.net_address              = "0.0.0.0",
		.net_port                 = "30002",

		.udp_threads              = 4,
		.udp_batch_messages       = 256,
		.udp_batch_timeout        = 10 * d_millisecond,

		.repacker_threads         = 12,
		.repacker_input_buffer    = 16 * 1024,
		.repacker_batch_messages  = 1024,
		.repacker_batch_timeout   = 100 * d_millisecond,

		.coordinator_input_buffer = 128,
		.report_input_buffer      = 32,

		.logger                   = {},
	};

	auto pinba = pinba_engine_init(&options);
	pinba->startup();

	auto *logger = pinba->globals()->logger();
	logger->set_level(meow::logging::log_level::debug);

	std::atomic<bool> in_shutdown = { false };

	auto const threaded_print_report_snapshot = [&](std::string report_name)
	{
		std::thread([&]()
		{
			FILE *sink = stdout;

			while (!in_shutdown.load())
			{
				sleep(1);
				auto const snapshot = pinba->get_report_snapshot(report_name);

				report_info_t const *rinfo = snapshot->report_info();

				ff::fmt(stdout, "got snapshot for report {0}, {1}, hv: {2}, {3}\n"
					, report_name, snapshot.get(), rinfo->hv_enabled, rinfo->hv_kind);
				{
					meow::stopwatch_t sw;

					ff::fmt(sink, ">> {0} ----------------------->\n", report_name);
					snapshot->prepare();
					ff::fmt(sink, ">> merge took {0} --------->\n", sw.stamp());
				}
#if 1
				if (!rinfo->hv_enabled)
					continue;

				if (rinfo->hv_kind != HISTOGRAM_KIND__FLAT)
					continue;

				duration_t total_d = {};

				for (auto pos = snapshot->pos_first(); !snapshot->pos_equal(pos, snapshot->pos_last()); pos = snapshot->pos_next(pos))
				{
					auto const *hv_conf = snapshot->histogram_conf();
					auto const *hv      = snapshot->get_histogram(pos);

					if (hv == nullptr)
						continue;

					auto const *flat_hv = static_cast<flat_histogram_t const*>(hv);
					total_d = total_d + get_percentile(*flat_hv, *hv_conf, 99.9);
				}

				ff::fmt(sink, "total p(99.9) = {0}\n", total_d);
#else
				debug_dump_report_snapshot(sink, snapshot.get());
#endif
			}
		}).detach();
	};

	{
		static report_conf___by_request_t conf = {
			.name            = "scripts",
			.time_window     = 60 * d_second,
			.tick_count      = 60,
			.hv_bucket_count = 1 * 1000 * 1000,
			.hv_bucket_d     = 1 * d_microsecond,
			.hv_min_value    = {0},

			.filters = {
				report_conf___by_request_t::make_filter___by_max_time(1 * d_second),
			},

			.keys = {
				report_conf___by_request_t::key_descriptor_by_request_field("script_name", &packet_t::script_id),
			},
		};
		pinba->add_report(create_report_by_request(pinba->globals(), conf));

		// threaded_print_report_snapshot(conf.name);
	}

	{
		static report_conf___by_timer_t conf = {
			.name            = "group+server",
			.time_window     = 60 * d_second,
			.tick_count      = 60,
			.hv_bucket_count = 1 * 1000 * 1000,
			.hv_bucket_d     = 1 * d_microsecond,
			.hv_min_value    = {0},

			.filters = {
				report_conf___by_timer_t::make_filter___by_max_time(1 * d_second),
			},

			.timertag_filters = {
			},

			.keys = {
				// report_conf___by_timer_t::key_descriptor_by_request_field("hostname", &packet_t::host_id),
				// report_conf___by_timer_t::key_descriptor_by_request_field("script_name", &packet_t::script_id),
				report_conf___by_timer_t::key_descriptor_by_timer_tag("group", pinba->globals()->dictionary()->get_or_add("group")),
				report_conf___by_timer_t::key_descriptor_by_timer_tag("server", pinba->globals()->dictionary()->get_or_add("server")),
			},
		};
		pinba->add_report(create_report_by_timer(pinba->globals(), conf));

		// threaded_print_report_snapshot(conf.name);
	}

	getchar();

	ff::fmt(stderr, "got shutdown request\n");

	in_shutdown.store(true);
	sleep(1);

	ff::fmt(stderr, "shutdown reader threads\n");

	pinba->shutdown();
	pinba.reset();

	ff::fmt(stderr, "pinba shutdown done\n");

	return 0;
}

#endif // PINBA_BUILD_MYSQL_MODULE


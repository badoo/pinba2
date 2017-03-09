#include <fcntl.h>
#include <sys/types.h>

#include <stdexcept>
#include <thread>

#include <meow/stopwatch.hpp>

#include "pinba/globals.h"
#include "pinba/report_by_request.h"
#include "pinba/report_by_timer.h"

////////////////////////////////////////////////////////////////////////////////////////////////

#if !defined(PINBA_BUILD_MYSQL_MODULE) || (PINBA_BUILD_MYSQL_MODULE == 0)

int main(int argc, char const *argv[])
{
	pinba_options_t options = {
		.net_address              = "0.0.0.0",
		.net_port                 = "3002",

		.udp_threads              = 4,
		.udp_batch_messages       = 256,
		.udp_batch_timeout        = 10 * d_millisecond,

		.repacker_threads         = 4,
		.repacker_input_buffer    = 16 * 1024,
		.repacker_batch_messages  = 1024,
		.repacker_batch_timeout   = 100 * d_millisecond,

		.coordinator_input_buffer = 32,
	};

	static auto pinba = pinba_engine_init(&options);
	pinba->startup();

	auto const threaded_print_report_snapshot = [&](std::string report_name)
	{
		std::thread([&]()
		{
			FILE *sink = stdout;

			while (true)
			{
				sleep(1);
				auto const snapshot = pinba->get_report_snapshot(report_name);

				ff::fmt(stdout, "got snapshot for report {0}, {1}\n", report_name, snapshot.get());
				{
					meow::stopwatch_t sw;

					ff::fmt(sink, ">> {0} ----------------------->\n", report_name);
					snapshot->prepare();
					ff::fmt(sink, ">> merge took {0} --------->\n", sw.stamp());
				}

				debug_dump_report_snapshot(sink, snapshot.get());
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

			.filters = {
				report_conf___by_request_t::make_filter___by_max_time(1 * d_second),
			},

			.keys = {
				report_conf___by_request_t::key_descriptor_by_request_field("script_name", &packet_t::script_id),
			},
		};
		pinba->start_report_with_config(conf);

		threaded_print_report_snapshot(conf.name);
	}

	{
		static report_conf___by_timer_t conf = {
			.name            = "script+tag10",
			.time_window     = 60 * d_second,
			.tick_count      = 60,
			.hv_bucket_count = 1 * 1000 * 1000,
			.hv_bucket_d     = 1 * d_microsecond,

			.filters = {
				report_conf___by_timer_t::make_filter___by_max_time(1 * d_second),
			},

			.keys = {
				report_conf___by_timer_t::key_descriptor_by_request_field("hostname", &packet_t::host_id),
				report_conf___by_timer_t::key_descriptor_by_request_field("script_name", &packet_t::script_id),
				report_conf___by_timer_t::key_descriptor_by_timer_tag("tag10", pinba->globals()->dictionary()->get_or_add("tag10")),
			},
		};
		pinba->start_report_with_config(conf);

		threaded_print_report_snapshot(conf.name);
	}

	getchar();

	pinba->shutdown();
	pinba.reset();

	return 0;
}

#endif // PINBA_BUILD_MYSQL_MODULE


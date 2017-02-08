#include <fcntl.h>
#include <sys/types.h>

#include <stdexcept>
#include <thread>

#include <meow/stopwatch.hpp>

#include "pinba/globals.h"
#include "pinba/report_by_request.h"

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

	auto pinba = pinba_init(&options);
	pinba->startup();

	{
		static report_conf___by_request_t conf = {
			.name            = "test_report",
			.time_window     = 60 * d_second,
			.ts_count        = 60,
			.hv_bucket_count = 1 * 1000 * 1000,
			.hv_bucket_d     = 1 * d_microsecond,

			.filters = {
				report_conf___by_request_t::make_filter___by_max_time(1 * d_second),
			},

			.keys = {
				report_conf___by_request_t::key_descriptor_by_request_field("script_name", &packet_t::script_id),
			},
		};
		pinba->create_report_by_request(&conf);

		std::thread([&]()
		{
			FILE *sink = stdout;

			while (true)
			{
				sleep(1);
				auto const snapshot = pinba->get_report_snapshot(conf.name);
#if 1
				ff::fmt(stdout, "got snapshot for report {0}, {1}\n", conf.name, snapshot.get());

				{
					meow::stopwatch_t sw;

					ff::fmt(sink, ">> {0} ----------------------->\n", conf.name);
					snapshot->prepare();
					ff::fmt(sink, ">> merge took {0} --------->\n", sw.stamp());
				}

				debug_dump_report_snapshot(sink, snapshot.get());
#endif
			}
		}).detach();

	}

	getchar();

	return 0;
}

#endif // PINBA_BUILD_MYSQL_MODULE


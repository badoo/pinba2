#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <thread>
#include <stdexcept>

#include <boost/noncopyable.hpp>

#include <meow/format/format_to_string.hpp>
#include <meow/hash/hash.hpp>
#include <meow/hash/hash_impl.hpp>
#include <meow/unix/time.hpp>
#include <meow/std_unique_ptr.hpp>

#include <nanomsg/nn.h>
#include <nanomsg/pipeline.h>
#include <nanomsg/reqrep.h>
#include <nanomsg/pubsub.h>

#include "misc/nmpa.h"
#include "misc/nmpa_pba.h"

#include "proto/pinba.pb-c.h"

#include "pinba/globals.h"
#include "pinba/collector.h"
#include "pinba/dictionary.h"
#include "pinba/packet.h"
#include "pinba/coordinator.h"
#include "pinba/repacker.h"
#include "pinba/report_by_request.h"

#include "pinba/nmsg_socket.h"
#include "pinba/nmsg_ticker.h"
#include "pinba/nmsg_poller.h"

////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char const *argv[])
try
{
	pinba_options_t options = {};
	auto globals = pinba_globals_init(&options);

	collector_conf_t collector_conf = {
		.address       = "0.0.0.0",
		.port          = "3002",
		.nn_output     = "inproc://udp-collector",
		.nn_shutdown   = "inproc://udp-collector/shutdown",
		.n_threads     = 2,
		.batch_size    = 128,
		.batch_timeout = 10 * d_millisecond,
	};
	auto collector = create_collector(globals, &collector_conf);

	repacker_conf_t repacker_conf = {
		.nn_input        = collector_conf.nn_output,
		.nn_output       = "inproc://repacker",
		.nn_shutdown     = "inproc://repacker/shutdown",
		.nn_input_buffer = 2 * 1024,
		.n_threads       = 2, // FIXME: should be == 1, since dictionary is shared between threads
		.batch_size      = 1024,
		.batch_timeout   = 100 * d_millisecond,
	};
	auto repacker = create_repacker(globals, &repacker_conf);

	coordinator_conf_t coordinator_conf = {
		.nn_input                = repacker_conf.nn_output,
		.nn_input_buffer         = 16,
		.nn_control              = "inproc://coordinator/control",
		.nn_report_input_buffer  = 16,
	};
	auto coordinator = create_coordinator(globals, &coordinator_conf);

	coordinator->startup();
	repacker->startup();
	collector->startup();

	{
		static report_conf___by_request_t conf = {
			.name            = "test_report",
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

		auto const err = coordinator->add_report(create_report_by_request(globals, conf));
		ff::fmt(stdout, "got coordinator control response, err = {0}\n", err);
	}

	getchar();

	return 0;
}
catch (std::exception const& e)
{
	ff::fmt(stderr, "error: {0}\n", e.what());
	return 1;
}
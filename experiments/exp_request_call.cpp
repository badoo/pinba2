#include <unistd.h>

#include <thread>

#include <nanomsg/nn.h>
#include <nanomsg/reqrep.h>

#include "pinba/globals.h"
#include "pinba/coordinator.h"
#include "pinba/nmsg_poller.h"

////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char const *argv[])
try
{
	pinba_options_t options = {};
	pinba_globals_t *globals = pinba_globals_init(&options);
#if 0
	static coordinator_conf_t coordinator_conf = {
		.nn_input                = "inproc://coordinator/input",
		.nn_input_buffer         = 16,
		.nn_control              = "inproc://coordinator/control",
		.nn_report_input_buffer  = 16,
	};

	auto const coordinator = create_coordinator(globals, &coordinator_conf);
	coordinator->startup();

	timeval_t tv = {};
	std::function<void(coordinator_t*)> const get_tv = [&](coordinator_t*)
	{
		tv = os_unix::clock_monotonic_now();
	};
#endif

	std::thread([&]()
	{
		try
		{
			nmsg_socket_t sock;
			sock
				.open(AF_SP, NN_REP)
				.bind("inproc://control")
				.set_option(NN_SOL_SOCKET, NN_LINGER, 0);

			nmsg_poller_t poller;

			poller
				.read_nn_socket(sock, [&](timeval_t now)
				{
					int const v = sock.recv<int>();
					// ff::fmt(stderr, "got req: {0}, now: {1}\n", v, now);

					sock.send(double(v));
				})
				.loop();
		}
		catch (std::exception const& e)
		{
		}
	}).detach();


	while (true)
	{
		ff::fmt(stderr, "{0}; press for next iteration\n", getpid());
		if (getchar() == 'c')
			break;

		nmsg_socket_t sock;

		for (size_t i = 0; i < 1; i++)
		{
			sock.open(AF_SP, NN_REQ).connect("inproc://control");
			sock.set_option(NN_SOL_SOCKET, NN_LINGER, 0);

			sock.send(i);
			double const d = sock.recv<double>();
			ff::fmt(stderr, "d = {0}\n", d);

			sock.close();

			// ff::fmt(stderr, "r = {0}, {1}:{2}\n", r, nn_errno(), nn_strerror(nn_errno()));
#if 0
			auto req = meow::make_intrusive<coordinator_request___call_t>();
			req->func = get_tv;

			auto const result = coordinator->request(req);

			assert(COORDINATOR_RES__GENERIC == result->type);

			auto const *r = static_cast<coordinator_response___generic_t*>(result.get());
			assert(COORDINATOR_STATUS__OK == r->status);

			ff::fmt(stderr, "tv = {0}\n", tv);
#endif
		}
	}

	nn_term();

	// coordinator->shutdown();

	return 0;
}
catch (std::exception const& e)
{
	ff::fmt(stderr, "error: {0}\n", e.what());
	return 1;
}

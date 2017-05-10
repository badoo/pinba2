#include <thread>
#include <vector>

#include <meow/format/format_to_string.hpp>
#include <meow/unix/time.hpp>

#include <nanomsg/nn.h>
#include <nanomsg/pubsub.h>
#include <nanomsg/pipeline.h>

#include <unistd.h>

#include "pinba/globals.h"

////////////////////////////////////////////////////////////////////////////////////////////////

std::string const endpoint_1 = "inproc://broadcast-1";
std::string const endpoint_2 = "inproc://broadcast-2";

int main(int argc, char const *argv[])
try
{
	auto const publisher = [](std::string id, std::string endpoint)
	{
		try
		{
			int sock = nn_socket(AF_SP, NN_PUB);
			assert(sock >= 0);

			int r = nn_bind(sock, endpoint.c_str());
			if (r < 0)
				throw std::runtime_error(ff::fmt_str("nn_bind failed; {0}:{1}", errno, strerror(errno)));

			while (true)
			{
				auto const data = ff::fmt_str("time: {0}", os_unix::clock_monotonic_now());
				ff::fmt(stdout, "[{0}] sending: {1}\n", id, data);

				int n = nn_send(sock, data.c_str(), data.size(), 0);
				assert(size_t(n) == data.size());
				sleep(1);
			}
		}
		catch (std::exception const& e)
		{
			ff::fmt(stderr, "thread: {0}, error: {1}\n", id, e.what());
		}
	};

	{
		std::thread publisher_thread([&]() { publisher("publisher_1", endpoint_1); } );
		publisher_thread.detach();
	}

	{
		std::thread publisher_thread([&]() { publisher("publisher_2", endpoint_2); } );
		publisher_thread.detach();
	}

	for (size_t i = 0; i < 3; i++)
	{
		std::thread t([=]()
		{
			try
			{
				int sock = nn_socket(AF_SP, NN_SUB);
				assert(sock >= 0);

				if (nn_setsockopt(sock, NN_SUB, NN_SUB_SUBSCRIBE, "", 0) < 0)
					throw std::runtime_error(ff::fmt_str("nn_setsockopt failed; {0}:{1}", errno, strerror(errno)));

				if (nn_connect(sock, endpoint_1.c_str()) < 0)
					throw std::runtime_error(ff::fmt_str("nn_connect failed; {0}:{1}", errno, strerror(errno)));

				while (true)
				{
					char *msg = NULL;
					int n = nn_recv(sock, &msg, NN_MSG, 0);
					if (n < 0)
						throw std::runtime_error(ff::fmt_str("nn_recv failed; {0}:{1}", errno, strerror(errno)));

					ff::fmt(stdout, "thread: {0}, recvd: {1}\n", i, str_ref{msg, (size_t)n});

					nn_freemsg(msg);
				}
			}
			catch (std::exception const& e)
			{
				ff::fmt(stderr, "thread: {0}, error: {1}\n", i, e.what());
			}
		});
		t.detach();
	}

	std::thread poller_thread([=]()
	{
		std::vector<std::string> const endpoints = { endpoint_1, endpoint_2 };
		size_t const n_endpoints = endpoints.size();
		struct nn_pollfd pfd[n_endpoints];

		try
		{
			for (size_t i = 0; i < n_endpoints; i++)
			{
				int sock = nn_socket(AF_SP, NN_SUB);
				assert(sock >= 0);

				if (nn_setsockopt(sock, NN_SUB, NN_SUB_SUBSCRIBE, "", 0) < 0)
					throw std::runtime_error(ff::fmt_str("nn_setsockopt failed; {0}:{1}", errno, strerror(errno)));

				if (nn_connect(sock, endpoints[i].c_str()) < 0)
					throw std::runtime_error(ff::fmt_str("nn_connect failed; {0}:{1}", errno, strerror(errno)));

				pfd[i] = nn_pollfd {
					.fd      = sock,
					.events  = NN_POLLIN,
					.revents = 0,
				};
			}

			static duration_t const poll_timeout = { 500 * msec_in_sec };

			while (true)
			{
				int rc = nn_poll(pfd, n_endpoints, poll_timeout.nsec / msec_in_sec);
				switch (rc)
				{
					case 0: // timeout
						ff::fmt(stdout, "poller: received nothing (poll timeout)\n");
						break;

					case -1: // error
						ff::fmt(stdout, "poller: error {0}:{1}\n", errno, strerror(errno));
						break;

					default: // something
						for (size_t i = 0; i < n_endpoints; i++)
						{
							if ((pfd[i].revents & NN_POLLIN) == 0)
								continue;

							char *msg = NULL;
							int n = nn_recv(pfd[i].fd, &msg, NN_MSG, 0);
							if (n < 0)
								throw std::runtime_error(ff::fmt_str("nn_recv failed; {0}:{1}", errno, strerror(errno)));

							ff::fmt(stdout, "polled, recvd from {0}: {1}\n", endpoints[i], str_ref{msg, (size_t)n});

							nn_freemsg(msg);
						}
						break;
				}
			}
		}
		catch (std::exception const& e)
		{
			ff::fmt(stderr, "thread: {0}, error: {1}\n", "poller", e.what());
		}
	});
	poller_thread.join();

	getchar();

	return 0;
}
catch (std::exception const& e)
{
	ff::fmt(stderr, "error: {0}\n", e.what());
	return 1;
}

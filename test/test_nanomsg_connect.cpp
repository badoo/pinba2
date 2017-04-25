#include <thread>

#include <nanomsg/nn.h>
#include <nanomsg/pipeline.h>
#include <nanomsg/reqrep.h>

#include "pinba/globals.h"
#include "pinba/nmsg_socket.h"

int main(int argc, char const *argv[])
{
	std::string const endpoint = "inproc://test";

	std::thread t([&]()
	{
		nmsg_socket_t sock;
		sock.open(AF_SP, NN_REP).bind(endpoint);
		int linger = sock.get_option_int(NN_SOL_SOCKET, NN_LINGER);
		ff::fmt(stderr, "receiving, linger: {0}\n", linger);

		while (true)
		{
			int const v = sock.recv<int>();
			ff::fmt(stderr, "v = {0}\n", v);

			sock.send(v);

			if (v == 0)
				break;
		}

		sock.close();
		ff::fmt(stderr, "receiver exiting\n");
	});
	t.detach();

	ff::fmt(stderr, "sending\n");

	for (unsigned i = 1; i < 5; i++)
	{
		sleep(1);

		nmsg_socket_t sock;
		ff::fmt(stderr, "connecting\n");
		sock.open(AF_SP, NN_REQ).connect(endpoint);

		bool const success = sock.send(i);
		ff::fmt(stderr, "sent {0} {1}\n", i, success);

		int const v = sock.recv<int>();
		ff::fmt(stderr, "response = {0}\n", v);
	}

	{
		nmsg_socket_t sock;
		sock.open(AF_SP, NN_REQ).connect(endpoint);
		int linger = sock.get_option_int(NN_SOL_SOCKET, NN_LINGER);

		bool const success = sock.send(0);
		ff::fmt(stderr, "sent {0} {1} (linger: {2})\n", 0, success, linger);

		int const v = sock.recv<int>();
		ff::fmt(stderr, "response = {0}\n", v);
	}

	ff::fmt(stderr, "sender done\n");

	getchar();

	return 0;
}
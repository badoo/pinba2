#include <thread>

#include <nanomsg/nn.h>
#include <nanomsg/pipeline.h>

#include "pinba/globals.h"
#include "pinba/nmsg_socket.h"

int main(int argc, char const *argv[])
{
	std::string const endpoint = "inproc://test";

	std::thread t([&]()
	{
		nmsg_socket_t sock;
		sock.open(AF_SP, NN_PULL).bind(endpoint);
		ff::fmt(stdout, "receiving\n");

		while (true)
		{
			int const v = sock.recv<int>();
			ff::fmt(stdout, "v = {0}\n", v);
		}
	});
	t.detach();

	std::thread([&]()
	{
		nmsg_socket_t sock;
		ff::fmt(stdout, "connecting\n");
		sock.open(AF_SP, NN_PUSH).connect(endpoint);
		ff::fmt(stdout, "sending\n");

		for (unsigned i = 0; i < 10; i++)
		{
			sleep(1);
			bool const success = sock.send(i, NN_DONTWAIT);
			ff::fmt(stdout, "sent {0}\n", success);
		}

		ff::fmt(stdout, "done\n");
	}).detach();

	getchar();

	return 0;
}
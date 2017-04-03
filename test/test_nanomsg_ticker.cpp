#include <thread>
#include <string>

#include <boost/noncopyable.hpp>

#include <meow/format/format_to_string.hpp>
#include <meow/std_unique_ptr.hpp>

#include <nanomsg/nn.h>
#include <nanomsg/pipeline.h>

#include "pinba/globals.h"
#include "pinba/nmsg_ticker.h"
#include "pinba/nmsg_poller.h"

////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char const *argv[])
{
	// auto ticker = meow::make_unique<nmsg_ticker___thread_per_timer_t>();
	auto ticker = meow::make_unique<nmsg_ticker___single_thread_t>();
	auto const chan_1 = ticker->subscribe(1000 * d_millisecond, "1");
	auto chan_2 = ticker->subscribe(250 * d_millisecond, "2");

	{
		// auto chan = ticker->subscribe(250 * d_millisecond, "3");
		// sleep(1);
		ticker->unsubscribe(chan_2);
		chan_2.reset(); // reset to allow socket re-bind
		chan_2 = ticker->subscribe(250 * d_millisecond, "2");
	}
	{
		auto const str_chan = nmsg_channel_create<str_ref>("test_string");
	}

	auto const str_chan = nmsg_channel_create<str_ref>("test_string");

	std::thread([&]() {
		sleep(1);
		str_chan->send("preved!");
	}).detach();

	nmsg_poller_t()
		.read_nn_channel(*chan_1, [&](nmsg_ticker_chan_t& chan, timeval_t now) {
			ff::fmt(stderr, "{0}; received: {1}, delay: {2}\n",
				chan.endpoint(), chan.recv_dontwait(), os_unix::clock_monotonic_now() - now);
		})
		.read_nn_channel(*chan_2, [&](nmsg_ticker_chan_t& chan, timeval_t now) {
			ff::fmt(stderr, "{0}; received: {1}, delay: {2}\n",
				chan.endpoint(), chan.recv_dontwait(), os_unix::clock_monotonic_now() - now);
		})
		.read_nn_channel(*str_chan, [](nmsg_channel_t<str_ref>& chan, timeval_t now) {
			ff::fmt(stderr, "{0}; got new data! '{1}'\n", chan.endpoint(), chan.recv());
		})
		.loop();

	return 0;
}
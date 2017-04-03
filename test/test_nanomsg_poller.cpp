#include <thread>
#include <string>

#include <boost/noncopyable.hpp>

#include <meow/format/format_to_string.hpp>
#include <meow/std_unique_ptr.hpp>

#include "pinba/globals.h"
#include "pinba/nmsg_ticker.h"
#include "pinba/nmsg_poller.h"

////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char const *argv[])
{
	auto ticker = meow::make_unique<nmsg_ticker___single_thread_t>();
	auto const chan_1 = ticker->subscribe(750 * d_millisecond, "1");
	// auto const chan_2 = ticker->subscribe(750 * d_millisecond, "1");

	nmsg_poller_t poller;

	auto resetable_ticker = poller.ticker_with_reset(
		100 * d_millisecond, [](timeval_t now)
		{
			ff::fmt(stderr, "> ticker/100ms/R: {0}\n", now);
		});

	poller
		.read_nn_channel(*chan_1, [&](nmsg_ticker_chan_t& chan, timeval_t now) {
			ff::fmt(stderr, "{0}; received: {1}, delay: {2}\n",
				chan.endpoint(), chan.recv_dontwait(), os_unix::clock_monotonic_now() - now);

			poller.reset_ticker(resetable_ticker, now + 550 * d_millisecond);
		})
		// .ticker(500 * d_millisecond, [](timeval_t now)
		// {
		// 	ff::fmt(stderr, "> ticker/500ms: {0}\n", now);
		// })
		// .ticker(1000 * d_millisecond, [](timeval_t now)
		// {
		// 	ff::fmt(stderr, ">> ticker/1000s: {0}\n", now);
		// })
		.loop();

	return 0;
}
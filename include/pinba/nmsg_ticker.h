#ifndef PINBA__NMSG__TICKER_H_
#define PINBA__NMSG__TICKER_H_

#include <map>
#include <thread>
#include <string>
#include <stdexcept>

#include <boost/noncopyable.hpp>

#include <nanomsg/nn.h>
#include <nanomsg/reqrep.h>
#include <nanomsg/pipeline.h>

#include "pinba/globals.h"
#include "pinba/nmsg_channel.h"
#include "pinba/nmsg_socket.h"

////////////////////////////////////////////////////////////////////////////////////////////////

typedef nmsg_channel_t<timeval_t>   nmsg_ticker_chan_t;
typedef nmsg_channel_ptr<timeval_t> nmsg_ticker_chan_ptr;

struct nmsg_ticker_t : private boost::noncopyable
{
	typedef nmsg_ticker_chan_t   channel_t;
	typedef nmsg_ticker_chan_ptr channel_ptr;

	virtual ~nmsg_ticker_t() {}

	virtual channel_ptr subscribe(duration_t period, str_ref name = {}) = 0;
	virtual channel_ptr once_after(duration_t after, str_ref name = {}) = 0;
	virtual channel_ptr once_at(timeval_t at, channel_ptr chan) = 0;
	virtual void unsubscribe(channel_ptr) = 0;
};

////////////////////////////////////////////////////////////////////////////////////////////////

struct nmsg_ticker___thread_per_timer_t : public nmsg_ticker_t
{
	nmsg_ticker___thread_per_timer_t()
	{
	}

	virtual channel_ptr subscribe(duration_t period, str_ref name = {}) override
	{
		std::string const chan_name = [&]()
		{
			std::string const real_name = (name) ? ff::fmt_str("{0}/{1}", name, period) : ff::write_str(period);
			return ff::fmt_str("nn_ticker/{0}", real_name);
		}();

		auto chan = nmsg_channel_create<timeval_t>(chan_name);

		std::thread t([=]()
		{
			timeval_t sleep_for = timeval_from_duration(period);
			timeval_t next_tick_tv = os_unix::clock_monotonic_now() + period;

			while (true)
			{
				struct timespec sleep_tspec = {
					.tv_sec  = sleep_for.tv_sec,
					.tv_nsec = sleep_for.tv_nsec,
				};
				struct timespec remain = {0,0};

				// ff::fmt(stdout, "{0}; sleeping for {1}\n", endpoint, sleep_for);

				int const r = nanosleep(&sleep_tspec, &remain);
				if (r == 0) // slept fine
				{
					timeval_t const now = os_unix::clock_monotonic_now();

					// ff::fmt(stdout, "{0}; tick, sending {1}\n", endpoint, now);
					chan->send_dontwait(now);

					for (; next_tick_tv < now; /**/)
					{
						// ff::fmt(stdout, "{0}; incrementing {1} < {2}\n", endpoint, next_tick_tv, now);
						next_tick_tv += period;
					}

					sleep_for = next_tick_tv - os_unix::clock_monotonic_now();

					continue;
				}

				// error sleeing, should be EINTR, or we have no idea how to fix really
				assert(r == -1);
				assert(errno == EINTR);

				// restart with remainder
				sleep_for = timeval_t{ remain.tv_sec, remain.tv_nsec };
			}
		});
		t.detach();

		return chan;
	}

	virtual channel_ptr once_after(duration_t after, str_ref name = {}) override
	{
		throw std::runtime_error("nmsg_ticker___thread_per_timer_t::once_after not implemented!");
	}

	virtual channel_ptr once_at(timeval_t at, channel_ptr chan) override
	{
		throw std::runtime_error("nmsg_ticker___thread_per_timer_t::once_at not implemented!");
	}

	virtual void unsubscribe(channel_ptr) override
	{
		throw std::runtime_error("nmsg_ticker___thread_per_timer_t::unsubscribe not implemented!");
	}
};

////////////////////////////////////////////////////////////////////////////////////////////////

struct nmsg_ticker___single_thread_t : public nmsg_ticker_t
{
	struct subscription_t : public nmsg_message_t
	{
		channel_ptr  chan      = {};
		timeval_t    next_tick = {};
		duration_t   period    = {};
		bool         once      = false;
		bool         unsub     = false;
		bool         shutdown  = false;
	};
	typedef boost::intrusive_ptr<subscription_t> subscription_ptr;

private:
	typedef std::multimap<timeval_t, subscription_ptr> subs_t;
	subs_t subs_;

	std::string    in_endpoint_;
	nmsg_socket_t  in_sock_;

	std::thread    t_;

public:

	nmsg_ticker___single_thread_t()
		: in_endpoint_(ff::fmt_str("inproc://nn_ticker/incoming/{0}", os_unix::clock_monotonic_now()))
	{
		in_sock_
			.open(AF_SP, NN_REP)
			.set_option(NN_SOL_SOCKET, NN_RCVBUF, 16 * sizeof(subscription_ptr), in_endpoint_)
			.bind(in_endpoint_);

		std::thread t([this]()
		{
			struct nn_pollfd pfd[] = {
				{
					.fd      = *in_sock_,
					.events  = NN_POLLIN,
					.revents = 0,
				},
			};
			size_t const pfd_size = sizeof(pfd) / sizeof(pfd[0]);

			while (true)
			{
				int wait_for_ms = [&]()
				{
					if (subs_.empty())
						return 1000; // generic wait for 1 second, when there is nothing to do

					timeval_t const next_tick = subs_.begin()->first;
					auto const wait_for = next_tick - os_unix::clock_monotonic_now();

					int const w = wait_for.tv_sec * msec_in_sec + wait_for.tv_nsec / (nsec_in_sec / msec_in_sec);
					return (w < 0) ? 0 : w;
				}();

				// ff::fmt(stderr, "polling for {0}ms\n", wait_for_ms);
				int const r = nn_poll(pfd, pfd_size, wait_for_ms);
				// ff::fmt(stderr, "r = {0}\n", r);

				if (r < 0)
				{
					if (EINTR == nn_errno())
						continue;

					ff::fmt(stderr, "{0}; nn_poll() failed, {1}:{2}\n", in_endpoint_, nn_errno(), strerror(nn_errno()));
					break;
				}

				if (r == 0) // timeout
				{
					auto const now = os_unix::clock_monotonic_now();

					while (!subs_.empty())
					{
						auto sub = subs_.begin()->second;
						if (now < sub->next_tick)
							break;

						subs_.erase(subs_.begin());

						// ff::fmt(stdout, "{0}; tick, sending {1}\n", sub.chan->endpoint(), now);
						sub->chan->send_dontwait({now});

						if (sub->once)
							continue;

						// find next tick, maybe skipping some if it took too long for us to process everything
						// have to account for it, even if unlikely
						// <= here is essential to avoid infinite loop when (sub.next_tick_tv == now)
						while (sub->next_tick <= now)
						{
							// ff::fmt(stdout, "{0}; incrementing {1} < {2}\n", sub.chan->endpoint(), sub.next_tick, now);
							sub->next_tick += sub->period;
						}

						subs_.insert({sub->next_tick, sub});
					}

					continue;
				}

				auto const sub = in_sock_.recv<subscription_ptr>(in_endpoint_, NN_DONTWAIT);
				if (!sub) // empty sub = nothing has been received
					continue;

				// ff::fmt(stdout, "got new sub request: {0}, {1}, {2}, {3}\n", sub->chan->endpoint(), sub->period, sub->unsub, sub->shutdown);

				if (sub->unsub)
				{
					auto const sub_it = [&]() // find subscription in the map by it's channel pointer
					{
						for (auto it = subs_.begin(), it_end = subs_.end(); it != it_end; ++it)
						{
							if (it->second->chan == sub->chan)
								return it;
						}
						return subs_.end();
					}();

					if (sub_it != subs_.end()) // found
					{
						subs_.erase(sub_it);
						in_sock_.send(1);
					}
					else
					{
						in_sock_.send(0);
					}

					continue;
				}

				if (sub->shutdown)
				{
					in_sock_.send(1);
					return;
				}

				// regular message, subscribe

				{
					subs_.insert({sub->next_tick, sub});
					in_sock_.send(1);

					continue;
				}
			}
		});
		t_ = move(t);
	}

	~nmsg_ticker___single_thread_t()
	{
		// avoid std::terminate(), since our thread is joinable
		// this is required anyway, as object dtor will close nmsg sockets and thread will start getting exceptions
		this->do_shutdown();
	}

	virtual channel_ptr subscribe(duration_t period, str_ref name = {}) override
	{
		auto chan = this->make_channel(name, period);

		auto sub = meow::make_intrusive<subscription_t>();
		sub->chan      = chan;
		sub->next_tick = os_unix::clock_monotonic_now() + period;
		sub->period    = period;

		this->do_send_message_to_thread(sub);

		return chan;
	}

	virtual channel_ptr once_after(duration_t after, str_ref name = {}) override
	{
		auto chan = this->make_channel(name, after);

		auto sub = meow::make_intrusive<subscription_t>();
		sub->chan      = chan;
		sub->next_tick = os_unix::clock_monotonic_now() + after;
		sub->once      = true;

		this->do_send_message_to_thread(sub);

		return chan;
	}

	virtual channel_ptr once_at(timeval_t at, channel_ptr chan) override
	{
		auto sub = meow::make_intrusive<subscription_t>();
		sub->chan      = chan;
		sub->next_tick = at;
		sub->once      = true;

		this->do_send_message_to_thread(sub);

		return chan;
	}

	virtual void unsubscribe(channel_ptr chan) override
	{
		auto sub = meow::make_intrusive<subscription_t>();
		sub->chan  = chan;
		sub->unsub = true;

		this->do_send_message_to_thread(sub);
	}

private:

	void do_shutdown()
	{
		auto sub = meow::make_intrusive<subscription_t>();
		sub->shutdown = true;

		this->do_send_message_to_thread(sub);
		t_.join();
	}

private:

	channel_ptr make_channel(str_ref name, duration_t period)
	{
		std::string const chan_name = [&]()
		{
			std::string const real_name = (name) ? name.str() : ff::write_str(os_unix::clock_monotonic_now());
			return ff::fmt_str("nn_ticker/{0}/{1}", real_name, period);
		}();

		return nmsg_channel_create<timeval_t>(chan_name);
	}

	int do_send_message_to_thread(subscription_ptr& sub)
	{
		nmsg_socket_t sock;
		sock.open(AF_SP, NN_REQ).connect(in_endpoint_);

		sock.send_message(sub);
		return sock.recv<int>();
	}
};

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__NMSG__TICKER_H_

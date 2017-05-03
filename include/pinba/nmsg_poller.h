#ifndef PINBA__NMSG__POLLER_H_
#define PINBA__NMSG__POLLER_H_

#ifndef _GNU_SOURCE
#define _GNU_SOURCE 1
#endif

#include <poll.h>

#include <cassert>
#include <vector>
#include <map>
#include <memory>      // unique_ptr
#include <stdexcept>   // runtime_error
#include <functional>  // function

#include <boost/noncopyable.hpp>

#include <meow/defer.hpp>
#include <meow/format/format.hpp>
#include <meow/format/format_to_string.hpp>

#include "pinba/nmsg_channel.h"
#include "pinba/nmsg_socket.h"

////////////////////////////////////////////////////////////////////////////////////////////////

struct nmsg_poller_t : private boost::noncopyable
{
private: // io pollers

	struct poller_t : private boost::noncopyable
	{
		virtual ~poller_t() {}
		virtual int   fd() const = 0;
		virtual short ev() const = 0;
		virtual void  callback(timeval_t) = 0;
	};

	template<class Function>
	struct poller___fd_t : public poller_t
	{
		int       sys_fd;
		short     events;
		Function  func;

		poller___fd_t(int f, short e, Function const& fn)
			: sys_fd(f)
			, events(e)
			, func(fn)
		{
		}

		virtual int   fd() const override { return sys_fd; }
		virtual short ev() const override { return events; }
		virtual void  callback(timeval_t now) override { func(now); }
	};

	template<class T, class Function>
	struct poller___nn_chan_t : public poller_t
	{
		nmsg_channel_t<T>&  chan;
		int                 sys_fd;
		short               events;
		Function            func;

		poller___nn_chan_t(nmsg_channel_t<T>& c, short e, Function const& fn)
			: chan(c)
			, sys_fd(-1)
			, events(e)
			, func(fn)
		{
			nmsg_socket_t tmp_sock { chan.read_sock() };
			MEOW_DEFER(
				tmp_sock.release();
			);

			if (events == NN_POLLIN)
				this->sys_fd = tmp_sock.get_option_int(NN_SOL_SOCKET, NN_RCVFD);
			if (events == NN_POLLOUT)
				this->sys_fd = tmp_sock.get_option_int(NN_SOL_SOCKET, NN_SNDFD);
		}

		virtual int   fd() const override { return sys_fd; }
		virtual short ev() const override { return events; }
		virtual void  callback(timeval_t now) override { func(chan, now); }
	};

	template<class Function>
	struct poller___nn_sock_t : public poller_t
	{
		int       sys_fd;
		short     events;
		Function  func;

		poller___nn_sock_t(nmsg_socket_t& sock, short e, Function const& f)
			: sys_fd(-1)
			, events(e)
			, func(f)
		{
			if (events == NN_POLLIN)
				this->sys_fd = sock.get_option_int(NN_SOL_SOCKET, NN_RCVFD);
			if (events == NN_POLLOUT)
				this->sys_fd = sock.get_option_int(NN_SOL_SOCKET, NN_SNDFD);
		}

		virtual int   fd() const override { return sys_fd; }
		virtual short ev() const override { return events; }
		virtual void  callback(timeval_t now) override { func(now); }
	};

	using poller_ptr = std::unique_ptr<poller_t>;

private: // periodic events

	struct ticker_t : private boost::noncopyable
	{
		virtual ~ticker_t() {}
		virtual timeval_t  when() const = 0;
		virtual duration_t interval() const = 0;
		virtual void       set_when(timeval_t) = 0;
		virtual void       callback(timeval_t) = 0;
	};
	using ticker_ptr = std::unique_ptr<ticker_t>;

	template<class Function>
	struct ticker___impl_t : public ticker_t
	{
		timeval_t   next_tv;
		duration_t  interval_d;
		Function    func;

		ticker___impl_t(timeval_t next, duration_t iv, Function const& fn)
			: next_tv(next)
			, interval_d(iv)
			, func(fn)
		{
		}

		virtual timeval_t  when() const override { return next_tv; }
		virtual duration_t interval() const override { return interval_d; }
		virtual void       set_when(timeval_t tv) override { next_tv = tv; }
		virtual void       callback(timeval_t now) override { func(now); }
	};

private:

	std::vector<poller_ptr>               pollers_;
	std::multimap<timeval_t, ticker_ptr>  tickers_; // FIXME: need a simpler impl imo

	std::function<void(timeval_t, duration_t)> before_poll_;
	bool shutting_down;

private:

	nmsg_poller_t& add_poller(poller_ptr p)
	{
		pollers_.push_back(move(p));
		return *this;
	}

	nmsg_poller_t& add_ticker(ticker_ptr t)
	{
		timeval_t const when = t->when();
		tickers_.emplace(when, move(t));
		return *this;
	}

public:

	nmsg_poller_t()
		: shutting_down(false)
	{
	}

public: // readers

	template<class T, class Function>
	nmsg_poller_t& read_nn_channel(nmsg_channel_t<T>& chan, Function const& func)
	{
		return this->add_poller(meow::make_unique<poller___nn_chan_t<T, Function>>(chan, NN_POLLIN, func));
	}

	template<class Function>
	nmsg_poller_t& read_nn_socket(nmsg_socket_t& sock, Function const& func)
	{
		return this->add_poller(meow::make_unique<poller___nn_sock_t<Function>>(sock, NN_POLLIN, func));
	}

	template<class Function>
	nmsg_poller_t& read_plain_fd(int fd, Function const& func)
	{
		return this->add_poller(meow::make_unique<poller___fd_t<Function>>(fd, NN_POLLIN, func));
	}

public: // writers

	template<class T, class Function>
	nmsg_poller_t& write(nmsg_channel_t<T>& chan, Function const& func)
	{
		return this->add_poller(meow::make_unique<poller___nn_chan_t<T, Function>>(chan, NN_POLLOUT, func));
	}

public: // tickers

	template<class Function>
	nmsg_poller_t& ticker(duration_t interval, Function const& func)
	{
		timeval_t const next_tv = os_unix::clock_monotonic_now() + interval;
		return this->add_ticker(meow::make_unique<ticker___impl_t<Function>>(next_tv, interval, func));
	}

	template<class Function>
	ticker_t const* ticker_with_reset(duration_t interval, Function const& func)
	{
		timeval_t const next_tv = os_unix::clock_monotonic_now() + interval;
		auto ticker    = meow::make_unique<ticker___impl_t<Function>>(next_tv, interval, func);
		auto *ticker_p = ticker.get(); // save ptr for return

		this->add_ticker(move(ticker));
		return ticker_p;
	}

	void reset_ticker(ticker_t const *t, timeval_t now)
	{
		// we're most likely called from inside one of the callbacks
		// so be careful here! and update the ticker like a baws!

		auto const it = [this, t]()
		{
			timeval_t const when = t->when();
			auto it = tickers_.find(when);

			// we've found just the lower bound, as this is the multimap
			// look for the real element, comparing pointers
			while ((it != tickers_.end()) && (it->first == when))
			{
				if (it->second.get() == t)
					break;
				++it;
			}
			assert(it != tickers_.end());

			return it;
		}();

		ticker_ptr ticker = move(it->second);
		tickers_.erase(it);

		timeval_t const next_tv = now + t->interval();
		ticker->set_when(next_tv);

		tickers_.emplace(next_tv, move(ticker));
	}

public: // utility

	void set_shutdown_flag()
	{
		shutting_down = true;
	}

	nmsg_poller_t& before_poll(std::function<void(timeval_t, duration_t)> const& func)
	{
		before_poll_ = func;
		return *this;
	}

	int loop()
	{
		size_t const pfd_size = pollers_.size();
		struct pollfd pfd[pfd_size];

		for (size_t i = 0; i < pfd_size; i++)
		{
			auto const& poller = pollers_[i];

			pfd[i] = pollfd {
				.fd      = poller->fd(),
				.events  = poller->ev(),
				.revents = 0,
			};
		}

		// TODO: add support for changing number of pollers/tickers when inside this function

		while (true)
		{
			// do not start next iteration if shutting down
			// no idea how this flag can be set from outside the callback really
			// but have it here for completeness
			// (might be useful later from cross-thread shutdowns? - scary!)
			if (shutting_down)
				return -ECANCELED;

			// process tickers that need to fire now
			timeval_t const now = os_unix::clock_monotonic_now();

			while (!tickers_.empty())
			{
				ticker_t *top = tickers_.begin()->second.get();
				if (now < top->when())
					break;

				ticker_ptr curr = move(tickers_.begin()->second);
				tickers_.erase(tickers_.begin());

				// ff::fmt(stdout, "now: {0}, next_tv: {1}\n", now, top->when());
				curr->callback(now);

				timeval_t const next_tv = curr->when() + curr->interval();
				curr->set_when(next_tv);
				tickers_.emplace(next_tv, move(curr));
			}

			// calculate the wait for next ticker to fire
			duration_t const wait_for = [&]()
			{
				ticker_t *top_ticker = (tickers_.empty())
							? nullptr
							: tickers_.begin()->second.get();

				if (!top_ticker)
					return 10 * d_second; // generic wait timeout, no tickers

				return duration_from_timeval(top_ticker->when() - now);
			}();

			int const wait_for_ms = [](duration_t wait_for)
			{
				int const wait_for_ms = (wait_for.nsec / (nsec_in_sec / msec_in_sec));

				// advance forward at least 1 millisecond every time
				// since poll granularity might be insufficient with high percision clocks
				// and we'd like to avoid _GNU_SOURCE dependency for ppoll()
				return (wait_for_ms <= 1)
						? 1
						: wait_for_ms;
			}(wait_for);

			// ff::fmt(stdout, "{0}; will poll for {1}ms with {2} fds\n", now, wait_for_ms, pfd_size);

			if (before_poll_)
				before_poll_(now, wait_for);

			// and perform a single poll iteration
			int const r = this->poll_and_callback(pfd, pfd_size, wait_for_ms);
			if (r < 0)
				return r;
		}

		return 0;
	}

private:

	int poll_and_callback(struct pollfd *pfd, size_t pfd_size, int wait_for_ms)
	{
		int const r = poll(pfd, pfd_size, wait_for_ms);
		// meow::format::fmt(stderr, "r = {0}\n", r);

		if (r < 0)
		{
			int e = errno;

			if (EINTR == e)
				return 0;

			return -e;
		}

		if (r == 0) // timeout, not an error
			return 1;

		// call dem callbacks, starting at random position
		timeval_t const now = os_unix::clock_monotonic_now();
		size_t const offset = now.tv_nsec % pfd_size;

		for (size_t i = 0; i < pfd_size; i++)
		{
			size_t real_offset = (i + offset) % pfd_size;

			if ((pfd[real_offset].revents & pfd[real_offset].events) == 0)
				continue;

			pollers_[real_offset]->callback(now);

			if (shutting_down) // this flag is set from inside the callback often
				return -ECANCELED;
		}

		return 0;
	}
};


////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__NMSG__POLLER_H_

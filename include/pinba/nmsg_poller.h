#ifndef PINBA__NMSG__POLLER_H_
#define PINBA__NMSG__POLLER_H_

#include <poll.h>

#include <cassert>
#include <vector>
#include <memory>      // unique_ptr
#include <stdexcept>   // runtime_error
#include <functional>  // function

#include <boost/noncopyable.hpp>

#include <meow/format/format.hpp>
#include <meow/format/format_to_string.hpp>

#include "pinba/nmsg_channel.h"

////////////////////////////////////////////////////////////////////////////////////////////////

struct nmsg_poller_t : private boost::noncopyable
{
private:
	struct poll_wrapper_t
	{
		virtual ~poll_wrapper_t() {}
		virtual int fd() const = 0;
		virtual short ev() const = 0;
		virtual void callback(timeval_t) = 0;
	};

	template<class T, class Function>
	struct poll_wrapper___chan_t : public poll_wrapper_t
	{
		nmsg_channel_t<T>& chan;
		short events;
		Function func;

		poll_wrapper___chan_t(nmsg_channel_t<T>& c, short e, Function const& f)
			: chan(c), events(e), func(f)
		{
		}

		virtual int fd() const override { return chan.read_sock(); }
		virtual short ev() const override { return events; }
		virtual void callback(timeval_t now) override { func(chan, now); }
	};

	template<class Function>
	struct poll_wrapper___sock_t : public poll_wrapper_t
	{
		int sock;
		short events;
		Function func;

		poll_wrapper___sock_t(int s, short e, Function const& f)
			: sock(s), events(e), func(f)
		{
		}

		virtual int fd() const override { return sock; }
		virtual short ev() const override { return events; }
		virtual void callback(timeval_t now) override { func(now); }
	};

	typedef std::unique_ptr<poll_wrapper_t> chan_wrapper_ptr;
	std::vector<chan_wrapper_ptr> channels_;

	bool shutting_down;

public:

	nmsg_poller_t()
		: shutting_down(false)
	{
	}

	template<class T, class Function>
	nmsg_poller_t& read(nmsg_channel_t<T>& chan, Function const& func)
	{
		chan_wrapper_ptr ptr {new poll_wrapper___chan_t<T, Function>(chan, NN_POLLIN, func)};
		channels_.push_back(move(ptr));
		return *this;
	}

	template<class Function>
	nmsg_poller_t& read_sock(int fd, Function const& func)
	{
		chan_wrapper_ptr ptr {new poll_wrapper___sock_t<Function>(fd, NN_POLLIN, func)};
		channels_.push_back(move(ptr));
		return *this;
	}

	template<class T, class Function>
	nmsg_poller_t& write(nmsg_channel_t<T>& chan, std::function<void(nmsg_channel_t<T>&, timeval_t)> const& func)
	{
		chan_wrapper_ptr ptr {new poll_wrapper___chan_t<T, Function>(chan, NN_POLLOUT, func)};
		channels_.push_back(move(ptr));
		return *this;
	}

	void set_shutdown_flag()
	{
		shutting_down = true;
	}

	int once(duration_t timeout)
	{
		// TODO: rewrite this with plain poll() to save some allocations and locking

		size_t const pfd_size = channels_.size();
		struct nn_pollfd pfd[pfd_size];

		for (size_t i = 0; i < pfd_size; i++)
		{
			auto& chan = channels_[i];
			pfd[i] = nn_pollfd { .fd = chan->fd(), .events = chan->ev() };
		}

		int const wait_for_ms = timeout.nsec / (nsec_in_sec / msec_in_sec);

		return this->poll_and_callback(pfd, pfd_size, wait_for_ms);
	}

	int loop()
	{
		// this function uses regular system poll() to avoid allocation/locking/etc. overhead in nn_poll()

		size_t const pfd_size = channels_.size();
		struct pollfd pfd[pfd_size];

		for (size_t i = 0; i < pfd_size; i++)
		{
			auto& chan = channels_[i];

			int sys_fd = -1;
			size_t sz = sizeof(sys_fd);
			int const r = nn_getsockopt(chan->fd(), NN_SOL_SOCKET, NN_RCVFD, &sys_fd, &sz);
			if (r < 0)
				throw std::runtime_error(meow::format::fmt_str("nn_getsockopt(RCVFD) failed, {0}:{1}\n", nn_errno(), nn_strerror(nn_errno())));

			assert(sz == sizeof(sys_fd));
			assert(sys_fd >= 0);

			pfd[i] = pollfd { .fd = sys_fd, .events = chan->ev() };
		}

		int const wait_for_ms = 1000 * 1000; // we have to give them SOME timeout, eh

		while (true)
		{
			// do not start next iteration if shutting down
			// no idea how this flag can be set from outside the callback really
			// but have it here for completeness
			// (might be useful later from cross-thread shutdowns? - scary!)
			if (shutting_down)
				return -1;

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
			// FIXME: should calculate remaining wait time here
			if (EINTR == errno)
				return 0;

			throw std::runtime_error(meow::format::fmt_str("poll() failed, {0}:{1}\n", errno, strerror(errno)));
		}

		if (r == 0) // timeout
			return 1;

		timeval_t const now = os_unix::clock_monotonic_now();

		for (size_t i = 0; i < pfd_size; i++)
		{
			if ((pfd[i].revents & pfd[i].events) == 0)
				continue;

			channels_[i]->callback(now);

			if (shutting_down) // this flag is set from inside the callback often
				return -1;
		}

		return 0;
	}

	int poll_and_callback(struct nn_pollfd *pfd, size_t pfd_size, int wait_for_ms)
	{
		int const r = nn_poll(pfd, pfd_size, wait_for_ms);
		// meow::format::fmt(stderr, "r = {0}\n", r);

		if (r < 0)
		{
			// FIXME: should calculate remaining wait time here
			if (EINTR == nn_errno())
				return 0;

			throw std::runtime_error(meow::format::fmt_str("nn_poll() failed, {0}:{1}\n", nn_errno(), nn_strerror(nn_errno())));
		}

		if (r == 0) // timeout
			return 1;

		timeval_t const now = os_unix::clock_monotonic_now();

		for (size_t i = 0; i < pfd_size; i++)
		{
			if ((pfd[i].revents & pfd[i].events) == 0)
				continue;

			channels_[i]->callback(now);

			if (shutting_down) // this flag is set from inside the callback often
				return -1;
		}

		return 0;
	}
};


////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__NMSG__POLLER_H_

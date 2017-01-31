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
#if 0
struct nmsg_socket_t : private boost::noncopyable
{
	int fd_;

	nmsg_socket_t()
		: fd_(-1)
	{
	}

	explicit nmsg_socket_t(int fd)
		: fd_(fd)
	{
	}

	nmsg_socket_t(nmsg_socket_t&& other)
		: nmsg_socket_t()
	{
		using std::swap;
		swap(fd_, other.fd_);
	}

	~nmsg_socket_t()
	{
		close();
	}

	int operator*() const
	{
		assert(fd_ > 0);
		return fd_;
	}

	void close()
	{
		if (fd_ >= 0)
		{
			nn_close(fd_);
			fd_ = -1;
		}
	}

	int release()
	{
		fd_ = -1;
	}

	nmsg_socket_t& open(int af, int type)
	{
		int const sock = nn_socket(af, type);
		if (sock < 0)
			throw std::runtime_error(ff::fmt_str("nn_socket({0}, {1}) failed: {2}:{3}\n", af, type, nn_errno(), nn_strerror(errno)));

		this->close();

		fd_ = sock;
	}

	nmsg_socket_t& connect(char const *endpoint)
	{
		int const r = nn_connect(fd_, endpoint);
		if (r < 0)
			throw std::runtime_error(ff::fmt_str("nn_connect({0}) failed: {1}:{2}", endpoint, nn_errno(), nn_strerror(errno)));

		return *this;
	}

	template<class T>
	nmsg_socket_t& set_option(int level, int option, T const& value)
	{
		int const r = nn_setsockopt(sock, level, option, &value, sizeof(value));
		if (r < 0)
		{
			throw std::runtime_error(ff::fmt_str(
				"nn_setsockopt({0}, {1}, {2}) failed: {3}:{4}",
				level, option, value,
				nn_errno(), nn_strerror(errno)));
		}

		return *this;
	}
};

inline nmsg_socket_t nmsg_socket(int af, int type)
{
	return std::move(nmsg_socket_t().open(af, type));
}

////////////////////////////////////////////////////////////////////////////////////////////////

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

template<class T>
struct nn_channel_t
	: public  boost::intrusive_ref_counter<nn_channel_t<T>>
	, private boost::noncopyable
{
private:
	std::string  endpoint_;
	int          read_sock_;
	int          write_sock_;

	// FIXME: add proper cleanup for channel sockets

public:

	~nn_channel_t()
	{
		// ff::fmt(stdout, "~nn_channel_t {0}, {1}\n", this, endpoint_);
	}

	nn_channel_t(str_ref name = {})
		: nn_channel_t(-1, name)
	{
	}

	nn_channel_t(int buffer_size, str_ref name = {})
	{
		endpoint_ = [&]()
		{
			std::string const chan_name = (name)
				? name.str()
				: ff::write_str(os_unix::clock_monotonic_now());

			return ff::fmt_str("inproc://nn_channel/{0}", chan_name);
		}();

		// ff::fmt(stdout, "nn_channel_t {0}, {1}\n", this, endpoint_);

		// read_sock_ = nmsg_socket()
		// 				.open(AF_SP, NN_PULL)
		// 				.set_option(NN_SOL_SOCKET, NN_RCVBUF, sizeof(T)*buffer_size)
		// 				.connect(endpoint.c_str());

		write_sock_ = [=]()
		{
			int const sock = nn_socket(AF_SP, NN_PUSH);
			if (sock < 0)
				throw std::runtime_error(ff::fmt_str("nn_socket(out) failed: {0}:{1}\n", nn_errno(), nn_strerror(errno)));

			int const r = nn_bind(sock, endpoint_.c_str());
			if (r < 0)
				throw std::runtime_error(ff::fmt_str("nn_bind({0}) failed: {1}:{2}", endpoint_, nn_errno(), nn_strerror(errno)));

			return sock;
		}();

		read_sock_ = [=]()
		{
			int const sock = nn_socket(AF_SP, NN_PULL);
			if (sock < 0)
				throw std::runtime_error(ff::fmt_str("nn_socket(in) failed: {0}:{1}\n", nn_errno(), nn_strerror(errno)));

			if (buffer_size > 0)
			{
				int optval = sizeof(T) * buffer_size;
				if (nn_setsockopt(sock, NN_SOL_SOCKET, NN_RCVBUF, &optval, sizeof(optval)) < 0)
					throw std::runtime_error(ff::fmt_str("nn_setsockopt(NN_RCVBUF) failed: {0}:{1}", nn_errno(), nn_strerror(errno)));
			}

			int const r = nn_connect(sock, endpoint_.c_str());
			if (r < 0)
				throw std::runtime_error(ff::fmt_str("nn_connect({0}) failed: {1}:{2}", endpoint_, nn_errno(), nn_strerror(errno)));

			return sock;
		}();
	}

	void send(T const& value)
	{
		int const n = this->send_ex(value, 0);
		if (n != sizeof(T))
			throw std::runtime_error(ff::fmt_str("nn_send({0}) failed; {1}:{2}", endpoint_, nn_errno(), nn_strerror(nn_errno())));
	}

	bool send_dontwait(T const& value)
	{
		int const n = this->send_ex(value, NN_DONTWAIT);
		if (n != sizeof(value))
		{
			int const e = nn_errno();
			if (EAGAIN == e)
				return false;

			throw std::runtime_error(ff::fmt_str("nn_send({0}) failed; {1}:{2}", endpoint_, e, nn_strerror(e)));
		}
		return true;
	}

	int send_ex(T const& value, int flags)
	{
		// static_assert(std::is_trivially_copyable<T>::value, "values sent through channels must be trivially_copyable");

		while (true)
		{
			int const n = nn_send(write_sock_, &value, sizeof(value), flags);
			if ((n != sizeof(T)) && (EINTR == nn_errno()))
				continue;

			return n;
		}
	}

	T recv()
	{
		return this->recv_ex(0);
	}

	T recv_dontwait()
	{
		return this->recv_ex(NN_DONTWAIT);
	}

	T recv_ex(int flags)
	{
		T value {};
		int const n = nn_recv(read_sock_, &value, sizeof(value), flags);
		if ((n != sizeof(value)) && (errno != EAGAIN))
			throw std::runtime_error(ff::fmt_str("nn_recv({0}) failed; {1}:{2}", endpoint_, nn_errno(), nn_strerror(errno)));

		return std::move(value);
	}

	std::string const& endpoint() const { return endpoint_; }
	int read_sock() const { return read_sock_; }
	int write_sock() const { return write_sock_; }
};

template<class T>
using nn_channel_ptr = boost::intrusive_ptr<nn_channel_t<T>>;

template<class T>
inline nn_channel_ptr<T> nn_channel_create(int buffer_size, str_ref name = {})
{
	return nn_channel_ptr<T>(new nn_channel_t<T>(buffer_size, name));
}

template<class T>
inline nn_channel_ptr<T> nn_channel_create(str_ref name = {})
{
	return nn_channel_ptr<T>(new nn_channel_t<T>(name));
}

////////////////////////////////////////////////////////////////////////////////////////////////

typedef nn_channel_t<timeval_t>   nn_ticker_chan_t;
typedef nn_channel_ptr<timeval_t> nn_ticker_chan_ptr;

struct nn_ticker_t : private boost::noncopyable
{
	typedef nn_ticker_chan_t   channel_t;
	typedef nn_ticker_chan_ptr channel_ptr;

	virtual ~nn_ticker_t() {}
	virtual channel_ptr subscribe(duration_t period, str_ref name = {}) = 0;
	// virtual void unsubscribe(int) = 0;
};

////////////////////////////////////////////////////////////////////////////////////////////////

#include <map>

struct nn_ticker___thread_per_timer_t : public nn_ticker_t
{
	nn_ticker___thread_per_timer_t()
	{
	}

	virtual channel_ptr subscribe(duration_t period, str_ref name = {}) override
	{
		std::string const chan_name = [&]()
		{
			std::string const real_name = (name) ? ff::fmt_str("{0}/{1}", name, period) : ff::write_str(period);
			return ff::fmt_str("nn_ticker/{0}", real_name);
		}();

		auto chan = nn_channel_create<timeval_t>(chan_name);

		// // need to add_ref, since we're copying object bytes through nanomsg
		// // and not copying it in C++ sense (i.e. ref count will not be incremented automatically)
		// intrusive_ptr_add_ref(chan.get());

		// in_chan_.send(subscription_t {
		// 	.chan = chan,
		// 	.next_tick = os_unix::clock_monotonic_now() + period,
		// 	.period = period,
		// });

		// return chan->read_sock();

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
};


struct nn_ticker___single_thread_t : public nn_ticker_t
{
	struct subscription_t
	{
		channel_ptr  chan;
		timeval_t    next_tick;
		duration_t   period;
	};

private:
	typedef std::multimap<timeval_t, subscription_t> subs_t;
	subs_t subs_;

	nn_channel_t<subscription_t> in_chan_;

	std::thread t_;

public:

	nn_ticker___single_thread_t()
		: in_chan_{1, ff::fmt_str("nn_ticker/incoming/{0}", os_unix::clock_monotonic_now())}
	{
		std::thread t([&]()
		{
			struct nn_pollfd pfd[] = {
				{
					.fd      = in_chan_.read_sock(),
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

					ff::fmt(stderr, "nn_poll failed, {0}:{1}\n", errno, strerror(errno));
					break;
				}

				if (r == 0) // timeout
				{
					auto const now = os_unix::clock_monotonic_now();

					while (!subs_.empty())
					{
						subscription_t sub = subs_.begin()->second;
						if (now < sub.next_tick)
							break;

						subs_.erase(subs_.begin());

						// ff::fmt(stdout, "{0}; tick, sending {1}\n", sub.chan->endpoint(), now);
						sub.chan->send_dontwait({now});

						while (sub.next_tick < now)
						{
							// ff::fmt(stdout, "{0}; incrementing {1} < {2}\n", endpoint, next_tick, now);
							sub.next_tick += sub.period;
						}

						subs_.insert({sub.next_tick, sub});
					}

					continue;
				}

				subscription_t const sub = in_chan_.recv_dontwait(); // use dontwait to defend from spurious wakeups
				// ff::fmt(stdout, "got new sub request: {0}, {1}\n", sub.chan->endpoint(), sub.period);

				subs_.insert({sub.next_tick, sub});
			}
		});
		t.detach();
		t_ = move(t);
	}

	virtual channel_ptr subscribe(duration_t period, str_ref name = {}) override
	{
		std::string chan_name = [&]()
		{
			std::string const real_name = (name) ? ff::fmt_str("{0}/{1}", name, period) : ff::write_str(period);
			return ff::fmt_str("nn_ticker/{0}", real_name);
		}();

		auto chan = nn_channel_create<timeval_t>(chan_name);

		// need to add_ref, since we're copying object bytes through nanomsg
		// and not copying it in C++ sense (i.e. ref count will not be incremented automatically)
		intrusive_ptr_add_ref(chan.get());

		in_chan_.send(subscription_t {
			.chan = chan,
			.next_tick = os_unix::clock_monotonic_now() + period,
			.period = period,
		});

		return chan;
	}
};

#include <vector>

struct nmsg_poller_t
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

public:

	template<class T, class Function>
	nmsg_poller_t& read(nmsg_channel_t<T>& chan, Function const& func)
	{
		chan_wrapper_ptr ptr {new poll_wrapper___chan_t<T, Function>(chan, NN_POLLIN, func)};
		channels_.push_back(move(ptr));
		return *this;
	}

	template<class T, class Function>
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

	int once(duration_t timeout)
	{
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
		size_t const pfd_size = channels_.size();
		struct nn_pollfd pfd[pfd_size];

		for (size_t i = 0; i < pfd_size; i++)
		{
			auto& chan = channels_[i];
			pfd[i] = nn_pollfd { .fd = chan->fd(), .events = chan->ev() };
		}

		int const wait_for_ms = 1000; // we have to give them SOME timeout, eh

		while (true)
		{
			int const r = this->poll_and_callback(pfd, pfd_size, wait_for_ms);
			if (r < 0)
				return r;
		}

		return 0;
	}

private:

	int poll_and_callback(struct nn_pollfd *pfd, size_t pfd_size, int wait_for_ms)
	{
		int const r = nn_poll(pfd, pfd_size, wait_for_ms);
		// ff::fmt(stderr, "r = {0}\n", r);

		if (r < 0)
		{
			ff::fmt(stderr, "nn_poll failed, {0}:{1}\n", nn_errno(), strerror(nn_errno()));
			return -1;
		}

		if (r == 0) // timeout
			return 1;

		timeval_t const now = os_unix::clock_monotonic_now();

		for (int i = 0; i < pfd_size; i++)
		{
			if ((pfd[i].revents & pfd[i].events) == 0)
				continue;

			channels_[i]->callback(now);
		}

		return 0;
	}
};
#endif
////////////////////////////////////////////////////////////////////////////////////////////////

int main(int argc, char const *argv[])
{
	// auto ticker = meow::make_unique<nmsg_ticker___thread_per_timer_t>();
	auto ticker = meow::make_unique<nmsg_ticker___single_thread_t>();
	auto const chan_1 = ticker->subscribe(1000 * d_millisecond, "1");
	auto const chan_2 = ticker->subscribe(250 * d_millisecond, "2");

	auto const str_chan = nmsg_channel_create<str_ref>("test_string");

	std::thread([&]() {
		sleep(1);
		str_chan->send("preved!");
	}).detach();

	nmsg_poller_t()
		.read(*chan_1, [&](nmsg_ticker_chan_t& chan, timeval_t now) {
			ff::fmt(stderr, "{0}; received: {1}, delay: {2}\n",
				chan.endpoint(), chan.recv_dontwait(), os_unix::clock_monotonic_now() - now);
		})
		.read(*chan_2, [&](nmsg_ticker_chan_t& chan, timeval_t now) {
			ff::fmt(stderr, "{0}; received: {1}, delay: {2}\n",
				chan.endpoint(), chan.recv_dontwait(), os_unix::clock_monotonic_now() - now);
		})
		.read(*str_chan, [](nmsg_channel_t<str_ref>& chan, timeval_t now) {
			ff::fmt(stderr, "{0}; got new data! '{1}'\n", chan.endpoint(), chan.recv());
		})
		.loop();

	return 0;
}
#ifndef PINBA__NMSG__CHANNEL_H_
#define PINBA__NMSG__CHANNEL_H_

#include <string>
#include <stdexcept>

#include <boost/noncopyable.hpp>

#include <meow/str_ref.hpp>
#include <meow/intrusive_ptr.hpp>
#include <meow/format/format.hpp>
#include <meow/format/format_to_string.hpp>
#include <meow/unix/time.hpp>

#include <nanomsg/nn.h>
#include <nanomsg/pipeline.h>

////////////////////////////////////////////////////////////////////////////////////////////////

template<class T>
struct nmsg_channel_t
	: public  boost::intrusive_ref_counter<nmsg_channel_t<T>>
	, private boost::noncopyable
{
private:
	std::string  endpoint_;
	int          read_sock_;
	int          write_sock_;

	// FIXME: add proper cleanup for channel sockets

public:

	~nmsg_channel_t()
	{
		nn_close(read_sock_);
		nn_close(write_sock_);
	}

	nmsg_channel_t(meow::str_ref name = {})
		: nmsg_channel_t(-1, name)
	{
	}

	nmsg_channel_t(int buffer_size, meow::str_ref name = {})
	{
		endpoint_ = [&]()
		{
			std::string const chan_name = (name)
				? name.str()
				: meow::format::write_str(os_unix::clock_monotonic_now());

			return meow::format::fmt_str("inproc://nn_channel/{0}", chan_name);
		}();

		// meow::format::fmt(stdout, "nmsg_channel_t {0}, {1}\n", this, endpoint_);

		// read_sock_ = nmsg_socket()
		// 				.open(AF_SP, NN_PULL)
		// 				.set_option(NN_SOL_SOCKET, NN_RCVBUF, sizeof(T)*buffer_size)
		// 				.connect(endpoint.c_str());

		write_sock_ = [=]()
		{
			int const sock = nn_socket(AF_SP, NN_PUSH);
			if (sock < 0)
				throw std::runtime_error(meow::format::fmt_str("nn_socket(out) failed: {0}:{1}\n", nn_errno(), nn_strerror(errno)));

			int const r = nn_bind(sock, endpoint_.c_str());
			if (r < 0)
				throw std::runtime_error(meow::format::fmt_str("nn_bind({0}) failed: {1}:{2}", endpoint_, nn_errno(), nn_strerror(errno)));

			return sock;
		}();

		read_sock_ = [=]()
		{
			int const sock = nn_socket(AF_SP, NN_PULL);
			if (sock < 0)
				throw std::runtime_error(meow::format::fmt_str("nn_socket(in) failed: {0}:{1}\n", nn_errno(), nn_strerror(errno)));

			if (buffer_size > 0)
			{
				int optval = sizeof(T) * buffer_size;
				if (nn_setsockopt(sock, NN_SOL_SOCKET, NN_RCVBUF, &optval, sizeof(optval)) < 0)
					throw std::runtime_error(meow::format::fmt_str("nn_setsockopt(NN_RCVBUF) failed: {0}:{1}", nn_errno(), nn_strerror(errno)));
			}

			int const r = nn_connect(sock, endpoint_.c_str());
			if (r < 0)
				throw std::runtime_error(meow::format::fmt_str("nn_connect({0}) failed: {1}:{2}", endpoint_, nn_errno(), nn_strerror(errno)));

			return sock;
		}();
	}

	void send(T const& value)
	{
		int const n = this->send_ex(value, 0);
		if (n != sizeof(T))
			throw std::runtime_error(meow::format::fmt_str("nn_send({0}) failed; {1}:{2}", endpoint_, nn_errno(), nn_strerror(nn_errno())));
	}

	bool send_dontwait(T const& value)
	{
		int const n = this->send_ex(value, NN_DONTWAIT);
		if (n != sizeof(value))
		{
			int const e = nn_errno();
			if (EAGAIN == e)
				return false;

			throw std::runtime_error(meow::format::fmt_str("nn_send({0}) failed; {1}:{2}", endpoint_, e, nn_strerror(e)));
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
			throw std::runtime_error(meow::format::fmt_str("nn_recv({0}) failed; {1}:{2}", endpoint_, nn_errno(), nn_strerror(errno)));

		return std::move(value);
	}

	std::string const& endpoint() const { return endpoint_; }
	int read_sock() const { return read_sock_; }
	int write_sock() const { return write_sock_; }
};

template<class T>
using nmsg_channel_ptr = boost::intrusive_ptr<nmsg_channel_t<T>>;

template<class T>
inline nmsg_channel_ptr<T> nmsg_channel_create(int buffer_size, meow::str_ref name = {})
{
	return nmsg_channel_ptr<T>(new nmsg_channel_t<T>(buffer_size, name));
}

template<class T>
inline nmsg_channel_ptr<T> nmsg_channel_create(meow::str_ref name = {})
{
	return nmsg_channel_ptr<T>(new nmsg_channel_t<T>(name));
}


////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__NMSG__CHANNEL_H_

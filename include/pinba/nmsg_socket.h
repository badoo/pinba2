#ifndef PINBA__NMSG__SOCKET_H_
#define PINBA__NMSG__SOCKET_H_

#include <stdexcept>
#include <utility>   // move, swap
#include <type_traits>

#include <boost/noncopyable.hpp>

#include <meow/str_ref.hpp>
#include <meow/intrusive_ptr.hpp>
#include <meow/format/format.hpp>
#include <meow/format/format_to_string.hpp>

#include <nanomsg/nn.h>

////////////////////////////////////////////////////////////////////////////////////////////////

template<class Derived>
struct nmsg_message_ex_t
	: public boost::intrusive_ref_counter<Derived>
	, private boost::noncopyable
{
};

struct nmsg_message_t : public nmsg_message_ex_t<nmsg_message_t>
{
	virtual ~nmsg_message_t() {} // an absolute must have, to properly delete children
};

template<int ID>
struct nmsg_message__with_id_t : public nmsg_message_t
{
	nmsg_message__with_id_t()
	{
		this->type = ID;
	}
};

////////////////////////////////////////////////////////////////////////////////////////////////

struct nmsg_socket_t : private boost::noncopyable
{
private:
	int fd_;

public:

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
		std::swap(fd_, other.fd_);
	}

	~nmsg_socket_t()
	{
		close();
	}

	void operator=(nmsg_socket_t&& other)
	{
		this->close();
		std::swap(fd_, other.fd_);
	}

	int operator*() const
	{
		assert(fd_ >= 0);
		return fd_;
	}

	void close()
	{
		if (fd_ < 0)
			return;

		while (true)
		{
			int r = nn_close(fd_);
			if (r < 0)
			{
				assert(nn_errno() == EINTR);
				continue; // should retry
			}
			break;
		}

		fd_ = -1;
	}

	int release()
	{
		int const result = fd_;
		fd_ = -1;
		return result;
	}

	nmsg_socket_t& open(int af, int type)
	{
		int const sock = nn_socket(af, type);
		if (sock < 0)
			throw std::runtime_error(meow::format::fmt_str("nn_socket({0}, {1}) failed: {2}:{3}", af, type, nn_errno(), nn_strerror(errno)));

		this->close();

		fd_ = sock;

		return *this;
	}

	nmsg_socket_t& connect(std::string const& endpoint)
	{
		return this->connect(endpoint.c_str());
	}

	nmsg_socket_t& connect(char const *endpoint)
	{
		int const r = nn_connect(fd_, endpoint);
		if (r < 0)
			throw std::runtime_error(meow::format::fmt_str("nn_connect({0}) failed: {1}:{2}", endpoint, nn_errno(), nn_strerror(nn_errno())));

		return *this;
	}

	nmsg_socket_t& bind(std::string const& endpoint)
	{
		return this->bind(endpoint.c_str());
	}

	nmsg_socket_t& bind(char const *endpoint)
	{
		int const r = nn_bind(fd_, endpoint);
		if (r < 0)
			throw std::runtime_error(meow::format::fmt_str("nn_bind({0}) failed: {1}:{2}", endpoint, nn_errno(), nn_strerror(nn_errno())));

		return *this;
	}

	// template<class T>
	// nmsg_socket_t& set_option(int level, int option, T const& value)
	// nn_setsockopt() just gives EINVAL if typeof(value) is size_t (i guess it's a size based check)
	nmsg_socket_t& set_option(int level, int option, int const value, meow::str_ref sockname = {})
	{
		int const r = nn_setsockopt(fd_, level, option, &value, sizeof(value));
		if (r < 0)
		{
			throw std::runtime_error(meow::format::fmt_str(
				"nn_setsockopt({0}, {1}, {2}, {3}) failed: {4}:{5}",
				sockname, level, option, value, nn_errno(), nn_strerror(errno)));
		}

		return *this;
	}

	nmsg_socket_t& set_option(int level, int option, meow::str_ref value, meow::str_ref sockname = {})
	{
		int const r = nn_setsockopt(fd_, level, option, value.data(), value.length());
		if (r < 0)
		{
			throw std::runtime_error(meow::format::fmt_str(
				"nn_setsockopt({0}, {1}, '{2}') failed: {3}:{4}",
				sockname, level, value, nn_errno(), nn_strerror(errno)));
		}

		return *this;
	}

	int get_option_int(int level, int option, meow::str_ref sockname = {})
	{
		int value = -1;
		size_t sz = sizeof(value);

		int const r = nn_getsockopt(fd_, level, option, &value, &sz);
		if (r < 0)
		{
			throw std::runtime_error(meow::format::fmt_str("nn_getsockopt({0}, {1}, {2}) failed, {3}:{4}\n",
				sockname, level, option,
				nn_errno(), nn_strerror(nn_errno())));
		}

		assert(sz == sizeof(value));
		return value;
	}

	template<class T>
	bool recv(T *value, meow::str_ref sockname = {}, int flags = 0)
	{
		int const n = nn_recv(fd_, (void*)value, sizeof(T), flags);
		if (n < 0) {
			int const err = nn_errno();
			if ((flags & NN_DONTWAIT) && (err == EAGAIN))
				return false;

			throw std::runtime_error(meow::format::fmt_str("nn_recv({0}) failed: {1}:{2}", sockname, nn_errno(), nn_strerror(nn_errno())));
		}

		return true;
	}

	template<class T>
	T recv(meow::str_ref sockname = {}, int flags = 0)
	{
		T value;

		int const n = nn_recv(fd_, (void*)&value, sizeof(T), flags);
		// ff::fmt(stderr, "nn_recv({0}, {1}, {2}, {3}) -> {4}\n", fd_, &value, sizeof(T), flags, n);
		if (n < 0) {
			int const err = nn_errno();

			// the caller must be ready to distinguish between value and EAGAIN
			if ((flags & NN_DONTWAIT) && (err == EAGAIN))
				return value;

			throw std::runtime_error(meow::format::fmt_str("nn_recv({0}) failed: {1}:{2}", sockname, err, nn_strerror(err)));
		}

		return value;
	}

	// send a value through the socket with given flags
	// NOTE: use this one only if you know what you're doing
	template<class T>
	bool send_ex(T const& value, int flags)
	{
		int const n = nn_send(fd_, (void*)&value, sizeof(T), flags);
		// ff::fmt(stderr, "nn_send({0}, {1}, {2}, {3}) -> {4}\n", fd_, &value, sizeof(T), flags, n);
		if (n < 0) {
			int const err = nn_errno();
			if ((flags & NN_DONTWAIT) && (err == EAGAIN))
				return false;

			throw std::runtime_error(meow::format::fmt_str("nn_send() failed: {0}:{1}", err, nn_strerror(err)));
		}
		return true;
	}

	template<class T>
	bool send(T const& value, int flags = 0)
	{
		// see other send functions for non-trivial objects
#if defined(__GNUC__) && (__GNUC__ > 4)
		// is_trivially_copyable is not implemented in gcc 4.9.4 and below :(
		static_assert(std::is_trivially_copyable<T>::value, "nmsg_socket_t::send() can only be used with trivially copyable types");
#endif
		return this->send_ex(value, flags);
	}

	template<class T>
	bool send(boost::intrusive_ptr<T> const& value, int flags = 0)
	{
		intrusive_ptr_add_ref(value.get());

		try
		{
			bool const success = this->send_ex(value, flags);
			if (!success)
				intrusive_ptr_release(value.get());

			return success;
		}
		catch (std::exception const& e)
		{
			intrusive_ptr_release(value.get());
			throw;
		}
	}

	template<class T>
	bool send_message(boost::intrusive_ptr<T> const& value, int flags = 0)
	{
		static_assert(
			(std::is_base_of<nmsg_message_ex_t<T>, T>::value || std::is_base_of<nmsg_message_t, T>::value),
			"send_message expects an intrusive_ptr to something derived from nmsg_message_t");

		return this->send(value, flags);
	}
};

inline nmsg_socket_t nmsg_socket(int af, int type)
{
	return std::move(nmsg_socket_t().open(af, type));
}

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__NMSG__SOCKET_H_
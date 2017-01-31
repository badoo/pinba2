#include <fcntl.h>
#include <sys/types.h>

#include <stdexcept>
#include <thread>

#include <meow/unix/fd_handle.hpp>
#include <meow/unix/socket.hpp>
#include <meow/unix/netdb.hpp>

#include "pinba/globals.h"
#include "pinba/collector.h"

////////////////////////////////////////////////////////////////////////////////////////////////

struct reader_socket_t
{
	int                   sock;
	os_addrinfo_list_ptr  ai_list;
};

reader_socket_t reader_socket_init(std::string const& addr, std::string const& port)
{
	os_addrinfo_list_ptr ai_list = os_unix::getaddrinfo_ex(addr.c_str(), port.c_str(), AF_INET, SOCK_DGRAM, 0);
	os_addrinfo_t *ai = ai_list.get(); // take 1st item for now

	os_unix::fd_handle_t fd { os_unix::socket_ex(ai->ai_family, ai->ai_socktype, ai->ai_protocol) };
	os_unix::setsockopt_ex(fd.get(), SOL_SOCKET, SO_REUSEADDR, 1);
	os_unix::bind_ex(fd.get(), ai->ai_addr, ai->ai_addrlen);

	return reader_socket_t {
		.sock    = fd.release(),
		.ai_list = move(ai_list),
	};
}

////////////////////////////////////////////////////////////////////////////////////////////////

// struct nn_tracker_t
// {
// 	virtual ~nn_tracker_t() {}

// 	virtual void                    reg_collector(str_ref endpoint, collector_t*) = 0;
// 	virtual string_ref<collector_t> collectors() = 0;
// };

// auto nn_reg_collector(str_ref endpoint, collector_t*) -> void;
// auto nn_all_collectors()                              -> string_ref<collector_t>;

// auto nn_reg_repacker(str_ref endpoint) -> void;

////////////////////////////////////////////////////////////////////////////////////////////////

int pinba_main(pinba_options_t const& options)
try
{
	// auto collector = create_collector(collector_conf);
	// auto repacker = create_repacker(repacker_conf);

	// repacker->startup();
	// collector->startup();

	// auto const reader_socket = reader_socket_init(options.net_address, options.net_port);

	// for (size_t i = 0; i < options.reader_threads; i++)
	// {
	// 	udp_reader_conf_t const conf = {
	// 		.socket         = &reader_socket,
	// 		.batch_messages = options.reader_batch_messages,
	// 		.batch_timeout  = options.reader_batch_timeout,
	// 	};

	// 	auto reader = udp_reader_create(conf);
	// 	reader->startup(PINBA_UDP_READER_ENDPOINT);

	// 	udp_readers_.push_back(move(reader));
	// }

	return 0;
}
catch (std::exception const& e)
{
	ff::fmt(stderr, "{0}; error: {1}\n", __func__, e.what());
	return -1;
}

#ifndef PINBA_BUILD_MYSQL_MODULE

int main(int argc, char const *argv[])
{
	pinba_options_t options = {
		.net_address           = "0.0.0.0",
		.net_port              = "3002",
		.reader_threads        = 4,
		.reader_batch_messages = 256,
		.reader_batch_timeout  = { 100 * msec_in_sec },
	};

	if (pinba_main(options) < 0)
		return 1;

	return 0;
}

#endif // PINBA_BUILD_MYSQL_MODULE
#include <fcntl.h>
#include <sys/types.h>

#include <stdexcept>
#include <thread>

#include <meow/unix/fd_handle.hpp>
#include <meow/unix/socket.hpp>
#include <meow/unix/netdb.hpp>

#include "pinba/globals.h"

////////////////////////////////////////////////////////////////////////////////////////////////

#if !defined(PINBA_BUILD_MYSQL_MODULE) || (PINBA_BUILD_MYSQL_MODULE == 0)

int main(int argc, char const *argv[])
{
	pinba_options_t options = {
		.net_address              = "0.0.0.0",
		.net_port                 = "3002",

		.udp_threads              = 4,
		.udp_batch_messages       = 256,
		.udp_batch_timeout        = 10 * d_millisecond,

		.repacker_threads         = 4,
		.repacker_input_buffer    = 16 * 1024,
		.repacker_batch_messages  = 1024,
		.repacker_batch_timeout   = 100 * d_millisecond,

		.coordinator_input_buffer = 32,
	};

	auto pinba = pinba_init(&options);
	pinba->startup();

	getchar();

	return 0;
}

#endif // PINBA_BUILD_MYSQL_MODULE


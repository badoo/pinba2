#ifndef PINBA__COLLECTOR_H_
#define PINBA__COLLECTOR_H_

#include <string>
#include <meow/std_unique_ptr.hpp>
#include <meow/unix/time.hpp>

#include "pinba/globals.h"
#include "pinba/nmsg_socket.h" // nmsg_message_ex_t

#include "misc/nmpa.h"

////////////////////////////////////////////////////////////////////////////////////////////////

#define PINBA_NET_DATAGRAM_FLAG___COMPRESSED_LZ4 (1 << 0)

struct net_datagram_t // network datagram
{
	uint8_t   version;
	uint32_t  flags;
	str_ref   data; // without header, if any
};

////////////////////////////////////////////////////////////////////////////////////////////////

// these are sent over PUSH/PULL channel
struct raw_request_t
	: public nmsg_message_ex_t<raw_request_t>
{
	struct nmpa_s   nmpa;
	uint32_t        request_count;
	Pinba__Request **requests;

	raw_request_t(uint32_t max_requests, size_t nmpa_block_sz)
	{
		PINBA_STATS_(objects).n_raw_batches++;

		nmpa_init(&nmpa, nmpa_block_sz);
		request_count = 0;
		requests = (Pinba__Request**)nmpa_alloc(&nmpa, sizeof(requests[0]) * max_requests);
	}

	~raw_request_t()
	{
		nmpa_free(&nmpa);

		PINBA_STATS_(objects).n_raw_batches--;
	}
};
using raw_request_ptr = boost::intrusive_ptr<raw_request_t>;

struct collector_conf_t
{
	std::string  address;
	std::string  port;

	std::string  nn_output;      // parsed udp packets are avilable here (as raw_request_t)
	std::string  nn_shutdown;    // used for graceful shutdown

	uint32_t     n_threads;      // reader threads to start

	uint32_t     batch_size;     // max number of messages to return in batch
	duration_t   batch_timeout;  // max time to wait to assemble a batch
};

struct collector_t
{
	virtual ~collector_t() {}

	virtual void startup() = 0;
	virtual void shutdown() = 0;
};

typedef std::unique_ptr<collector_t> collector_ptr;

collector_ptr create_collector(pinba_globals_t*, collector_conf_t*);

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__COLLECTOR_H_
#ifndef PINBA__COLLECTOR_H_
#define PINBA__COLLECTOR_H_

#include <string>
#include <meow/std_unique_ptr.hpp>
#include <meow/unix/time.hpp>

#include "pinba/globals.h"
#include "pinba/nmsg_socket.h" // nmsg_message_ex_t

#include "misc/nmpa.h"

////////////////////////////////////////////////////////////////////////////////////////////////

struct raw_request_t
	: public nmsg_message_ex_t<raw_request_t>
{
	struct nmpa_s   nmpa;
	uint32_t        request_count;
	Pinba__Request **requests;

	raw_request_t(uint32_t max_requests, size_t nmpa_block_sz)
	{
		nmpa_init(&nmpa, nmpa_block_sz);
		request_count = 0;
		requests = (Pinba__Request**)nmpa_alloc(&nmpa, sizeof(*requests) * max_requests);
	}

	~raw_request_t()
	{
		nmpa_free(&nmpa);
	}
};

// these are sent over PUSH/PULL channel
typedef boost::intrusive_ptr<raw_request_t> raw_request_ptr;

// struct raw_request_t // these are sent over PUSH/PULL channel
// {
// 	struct nmpa_s   nmpa;
// 	Pinba__Request *request;
// };

struct collector_conf_t
{
	std::string  address;
	std::string  port;

	std::string  nn_output;      // parsed udp packets are avilable here (as raw_request_t)

	uint32_t     n_threads;      // reader threads to start

	uint32_t     batch_size;     // max number of messages to return in batch
	duration_t   batch_timeout;  // max time to wait to assemble a batch
};

struct collector_t
{
	virtual ~collector_t() {}

	virtual void    startup() = 0;
	virtual str_ref nn_output() = 0;
};

typedef std::unique_ptr<collector_t> collector_ptr;

collector_ptr create_collector(pinba_globals_t*, collector_conf_t*);

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__COLLECTOR_H_
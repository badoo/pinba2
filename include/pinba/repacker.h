#ifndef PINBA__REPACKER_H_
#define PINBA__REPACKER_H_

#include <string>

#include "pinba/globals.h"
#include "pinba/nmsg_socket.h" // nmsg_message_ex_t

#include "misc/nmpa.h"

////////////////////////////////////////////////////////////////////////////////////////////////

struct packet_t;

struct packet_batch_t : public nmsg_message_ex_t<packet_batch_t>
{
	struct nmpa_s       nmpa;
	uint32_t            packet_count;
	packet_t            **packets;

	repacker_state_ptr  repacker_state; // can be empty


	packet_batch_t(size_t max_packets, size_t nmpa_block_sz)
		: packet_count{0}
	{
		PINBA_STATS_(objects).n_packet_batches++;

		nmpa_init(&nmpa, nmpa_block_sz);
		packets = (packet_t**)nmpa_alloc(&nmpa, sizeof(packets[0]) * max_packets);
	}

	~packet_batch_t()
	{
		nmpa_free(&nmpa);

		PINBA_STATS_(objects).n_packet_batches--;
	}
};
typedef boost::intrusive_ptr<packet_batch_t> packet_batch_ptr;

////////////////////////////////////////////////////////////////////////////////////////////////

struct repacker_conf_t
{
	std::string  nn_input;         // read raw_request_t from this nanomsg pipe
	std::string  nn_output;        // send batched repacked packets to this nanomsg pipe
	std::string  nn_shutdown;      // bind on this socket, receive shutdown signal here (user should call shutdown())

	size_t       nn_input_buffer;  // NN_RCVBUF for nn_input connection

	uint32_t     n_threads;        // threads to start

	uint32_t     batch_size;       // max packets in batch
	duration_t   batch_timeout;    // max delay between batches
};

struct repacker_t : private boost::noncopyable
{
	virtual ~repacker_t() {}
	virtual void startup() = 0;
	virtual void shutdown() = 0;
};
using repacker_ptr = std::unique_ptr<repacker_t>;

repacker_ptr create_repacker(pinba_globals_t*, repacker_conf_t*);

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__REPACKER_H_

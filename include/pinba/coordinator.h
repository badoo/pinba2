#ifndef PINBA__COORDINATOR_H_
#define PINBA__COORDINATOR_H_

#include <string>

#include <meow/error.hpp>

#include "pinba/globals.h"
#include "pinba/packet.h"
#include "pinba/report.h"
#include "pinba/nmsg_socket.h" // nmsg_message_ex_t

////////////////////////////////////////////////////////////////////////////////////////////////
// requests

#define COORDINATOR_REQ__ADD_REPORT          0
#define COORDINATOR_REQ__GET_REPORT_SNAPSHOT 1

struct coordinator_request_t : public nmsg_message_t
{
	int type;
};
typedef boost::intrusive_ptr<coordinator_request_t> coordinator_request_ptr;

template<int ID>
struct coordinator_request__with_id_t : public coordinator_request_t
{
	coordinator_request__with_id_t()
	{
		this->type = ID;
	}
};

// real requests
struct coordinator_request___add_report_t
	: public coordinator_request__with_id_t<COORDINATOR_REQ__ADD_REPORT>
{
	report_ptr report; // created somewhere else, carries information about itself
};

struct coordinator_request___get_report_snapshot_t
	: public coordinator_request__with_id_t<COORDINATOR_REQ__GET_REPORT_SNAPSHOT>
{
	std::string report_name;
};

////////////////////////////////////////////////////////////////////////////////////////////////
// responses

#define COORDINATOR_RES__GENERIC         0
#define COORDINATOR_RES__REPORT_SNAPSHOT 1

#define COORDINATOR_STATUS__OK     0
#define COORDINATOR_STATUS__ERROR -1

struct coordinator_response_t : public nmsg_message_t
{
	int type;
};
typedef boost::intrusive_ptr<coordinator_response_t> coordinator_response_ptr;

template<int ID>
struct coordinator_response__with_id_t : public coordinator_response_t
{
	coordinator_response__with_id_t()
	{
		this->type = ID;
	}
};

struct coordinator_response___generic_t
	: public coordinator_response__with_id_t<COORDINATOR_RES__GENERIC>
{
	int            status;
	meow::error_t  err;

	coordinator_response___generic_t(int s)
		: status(s)
		, err()
	{
	}

	coordinator_response___generic_t(int s, std::string const& err_string)
		: status(s)
		, err(err_string)
	{
	}
};

// real responses
struct coordinator_response___report_snapshot_t
	: public coordinator_response__with_id_t<COORDINATOR_RES__REPORT_SNAPSHOT>
{
	report_snapshot_ptr snapshot;
};

////////////////////////////////////////////////////////////////////////////////////////////////

struct coordinator_conf_t
{
	std::string  nn_input;                // connect to this socket and receive packet_batch_ptr-s (PULL)
	size_t       nn_input_buffer;         // NN_RCVBUF for nn_input (can leav this small, due to low-ish traffic)

	std::string  nn_control;              // control messages received here (binds, REP)
	std::string  nn_report_output;        // broadcasts packet_batch_ptr-s for reports here (binds, PUB)
	size_t       nn_report_output_buffer; // report_handler uses this as NN_RCVBUF
};

struct coordinator_t : private boost::noncopyable
{
	virtual ~coordinator_t() {}
	virtual void startup() = 0;

	virtual coordinator_response_ptr request(coordinator_request_ptr) = 0;
};
typedef std::unique_ptr<coordinator_t> coordinator_ptr;

coordinator_ptr create_coordinator(pinba_globals_t*, coordinator_conf_t*);

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__COORDINATOR_H_

#ifndef PINBA__COORDINATOR_H_
#define PINBA__COORDINATOR_H_

#include "pinba/globals.h"
#include "pinba/report.h"

////////////////////////////////////////////////////////////////////////////////////////////////

struct coordinator_conf_t
{
	std::string  nn_input;                // connect to this socket and receive packet_batch_ptr-s (PULL)
	size_t       nn_input_buffer;         // NN_RCVBUF for nn_input (can leav this small, due to low-ish traffic)

	std::string  nn_control;              // control messages received here (binds, REP)
	size_t       nn_report_input_buffer;  // report_handler uses this as NN_RCVBUF
};

struct coordinator_t : private boost::noncopyable
{
	virtual ~coordinator_t() {}

	virtual void startup() = 0;
	virtual void shutdown() = 0;

	virtual pinba_error_t       add_report(report_ptr report) = 0;
	virtual pinba_error_t       delete_report(std::string const& name) = 0;
	virtual report_snapshot_ptr get_report_snapshot(std::string const& name) = 0;
	virtual report_state_ptr    get_report_state(std::string const& name) = 0;
};
typedef std::unique_ptr<coordinator_t> coordinator_ptr;

coordinator_ptr create_coordinator(pinba_globals_t*, coordinator_conf_t*);

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__COORDINATOR_H_

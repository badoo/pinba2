#ifndef PINBA__GLOBALS_H_
#define PINBA__GLOBALS_H_

#include <cstdint>
#include <cassert>

#include <atomic>
#include <memory>      // unique_ptr

#include <meow/str_ref.hpp>
#include <meow/std_unique_ptr.hpp>
#include <meow/format/format.hpp>
#include <meow/unix/time.hpp>

#include "pinba/report.h"

////////////////////////////////////////////////////////////////////////////////////////////////

namespace ff = meow::format;
using meow::str_ref;

struct _Pinba__Request;
typedef struct _Pinba__Request Pinba__Request;

////////////////////////////////////////////////////////////////////////////////////////////////

struct pinba_options_t
{
	std::string net_address;
	std::string net_port;

	uint32_t    udp_threads;
	uint32_t    udp_batch_messages;
	duration_t  udp_batch_timeout;

	uint32_t    repacker_threads;
	uint32_t    repacker_input_buffer;
	uint32_t    repacker_batch_messages;
	duration_t  repacker_batch_timeout;

	uint32_t    coordinator_input_buffer;
};

struct nmsg_ticker_t;
struct dictionary_t;

struct report_conf___by_request_t;
struct report_conf___by_timer_t;

// TODO: maybe have this as global registry for all threaded objects here
//       and not just explicit stats, but everything (and ticker for example!)
struct pinba_globals_t
{
	struct {
		std::atomic<uint64_t> recv_total;
		std::atomic<uint64_t> recv_nonblocking;
		std::atomic<uint64_t> recv_eagain;
		std::atomic<uint64_t> packets_received;
	} udp;

	struct {
		std::atomic<uint64_t> poll_total;
		std::atomic<uint64_t> recv_total;
		std::atomic<uint64_t> recv_eagain;
		std::atomic<uint64_t> packets_processed;
	} repacker;

	virtual ~pinba_globals_t() {}

	virtual void startup() = 0;

	// virtual logger_t* logger() const = 0;

	virtual nmsg_ticker_t* ticker() const = 0;
	virtual dictionary_t*  dictionary() const = 0;

	virtual bool create_report_by_request(report_conf___by_request_t*) = 0;
	virtual bool create_report_by_timer(report_conf___by_timer_t*) = 0;

	virtual report_snapshot_ptr get_report_snapshot(str_ref name) = 0;
};
typedef std::unique_ptr<pinba_globals_t> pinba_globals_ptr;

pinba_globals_ptr pinba_init(pinba_options_t*);

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__GLOBALS_H_
#ifndef PINBA__GLOBALS_H_
#define PINBA__GLOBALS_H_

#include <cstdint>
#include <cassert>

#include <atomic>
#include <memory>      // unique_ptr
#include <mutex>
#include <vector>

#include <boost/noncopyable.hpp>

#include <meow/error.hpp>
#include <meow/str_ref.hpp>
#include <meow/std_unique_ptr.hpp>
#include <meow/format/format.hpp>
#include <meow/logging/logger.hpp>
#include <meow/logging/log_write.hpp>
#include <meow/unix/time.hpp>

////////////////////////////////////////////////////////////////////////////////////////////////

namespace ff = meow::format;
using meow::str_ref;

struct _Pinba__Request;
typedef struct _Pinba__Request Pinba__Request;

typedef meow::error_t pinba_error_t;

////////////////////////////////////////////////////////////////////////////////////////////////

struct nmsg_ticker_t;
struct dictionary_t;

struct repacker_stats_t
{
	timeval_t ru_utime = {0,0};
	timeval_t ru_stime = {0,0};
};

// this one is updated from multiple threads
// use atomic primitives to set/fetch values
// if using atomic is impossible - use pinba_stats_wrap_t below and lock
// when this structure is returned by value from pinba_globals_t::stats_copy() - the lock is held while copying
struct pinba_stats_t
{
	mutable std::mutex mtx;

	timeval_t start_tv            = {0,0};  // to calculate uptime
	timeval_t start_realtime_tv   = {0,0};  // can show to user, etc.

	struct {
		std::atomic<uint64_t> recv_total        = {0};      // total recv* calls
		std::atomic<uint64_t> recv_nonblocking  = {0};      // total recv* calls with MSG_DONTWAIT
		std::atomic<uint64_t> recv_eagain       = {0};      // EAGAIN errors from recv* calls
		std::atomic<uint64_t> recv_bytes        = {0};      // bytes received
		std::atomic<uint64_t> packets_received  = {0};      // total udp packets received
		std::atomic<uint64_t> packet_decode_err = {0};      // number of times we've failed to decode incoming message
		std::atomic<uint64_t> batch_send_total  = {0};      // batch send attempts (to repacker)
		std::atomic<uint64_t> batch_send_err    = {0};      // batch sends that failed
	} udp;

	struct {
		std::atomic<uint64_t> poll_total        = {0};
		std::atomic<uint64_t> recv_total        = {0};
		std::atomic<uint64_t> recv_eagain       = {0};
		std::atomic<uint64_t> packets_processed = {0};
	} repacker;

	std::vector<repacker_stats_t> repacker_threads;

	struct {
		std::atomic<uint64_t> batches_received = {0};    // packet batches received
		std::atomic<uint64_t> batch_send_total = {0};    // total batch send attempts (to all reports)
		std::atomic<uint64_t> batch_send_err   = {0};    // total batch send errors (to all reports)
		std::atomic<uint64_t> control_requests = {0};    // control requests processed

		timeval_t ru_utime                     = {0,0};
		timeval_t ru_stime                     = {0,0};
	} coordinator;
};

////////////////////////////////////////////////////////////////////////////////////////////////

using pinba_logger_t   = meow::logging::logger_t;
using pinba_logger_ptr = std::shared_ptr<pinba_logger_t>;


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

	pinba_logger_ptr logger;
};

struct pinba_globals_t : private boost::noncopyable
{
	virtual ~pinba_globals_t() {}

	virtual pinba_stats_t* stats() = 0;             // get shared stats and obey the rules
	// virtual pinba_stats_t  stats_copy() const = 0;  // get your own private copy and use it without locking

	virtual pinba_logger_t*  logger() const = 0;
	virtual pinba_options_t const* options() const = 0;
	virtual nmsg_ticker_t* ticker() const = 0;
	virtual dictionary_t*  dictionary() const = 0;
};
typedef std::unique_ptr<pinba_globals_t> pinba_globals_ptr;

// init just the globals, simplifies testing for example
pinba_globals_ptr pinba_globals_init(pinba_options_t*);

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__GLOBALS_H_
#ifndef PINBA__GLOBALS_H_
#define PINBA__GLOBALS_H_

#include <cstdint>
#include <cassert>

#include <atomic>
#include <memory>      // unique_ptr, shared_ptr
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

#include "pinba/limits.h"

////////////////////////////////////////////////////////////////////////////////////////////////

namespace ff = meow::format;
using meow::str_ref;

struct _Pinba__Request;
typedef struct _Pinba__Request Pinba__Request;

typedef meow::error_t pinba_error_t;

////////////////////////////////////////////////////////////////////////////////////////////////

struct pinba_os_symbols_t;
struct dictionary_t;

struct repacker_state_t;
using repacker_state_ptr = std::shared_ptr<repacker_state_t>;

struct repacker_state_t : private boost::noncopyable
{
	virtual ~repacker_state_t() {}
	virtual repacker_state_ptr clone() = 0;
	virtual void               merge_other(repacker_state_t&) = 0;
};
using repacker_state_ptr = std::shared_ptr<repacker_state_t>;

inline void repacker_state___merge_to_from(repacker_state_ptr& to, repacker_state_ptr const& from)
{
	if (!from)
		return;

	if (!to)
		to = from->clone();
	else
		to->merge_other(*from);
}

////////////////////////////////////////////////////////////////////////////////////////////////

struct collector_stats_t
{
	timeval_t ru_utime = {0,0};
	timeval_t ru_stime = {0,0};
};

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
		std::atomic<uint64_t> n_raw_batches         = {0};
		std::atomic<uint64_t> n_packet_batches      = {0};
		std::atomic<uint64_t> n_repacker_dict_words = {0};
		std::atomic<uint64_t> n_repacker_dict_ws    = {0};
		std::atomic<uint64_t> n_report_snapshots    = {0};
		std::atomic<uint64_t> n_report_ticks        = {0};
		std::atomic<uint64_t> n_coord_requests      = {0};
	// 	std::atomic<uint64_t> n_ = {0};
	// 	std::atomic<uint64_t> n_ = {0};
	} objects;

	struct {
		std::atomic<uint64_t> poll_total        = {0};      // total poll calls
		std::atomic<uint64_t> recv_total        = {0};      // total recv* calls
		std::atomic<uint64_t> recv_eagain       = {0};      // EAGAIN errors from recv* calls
		std::atomic<uint64_t> recv_bytes        = {0};      // bytes received
		std::atomic<uint64_t> recv_packets      = {0};      // total udp packets received
		std::atomic<uint64_t> packet_decode_err = {0};      // number of times we've failed to decode incoming message
		std::atomic<uint64_t> batch_send_total  = {0};      // batch send attempts (to repacker)
		std::atomic<uint64_t> batch_send_err    = {0};      // batch sends that failed
		std::atomic<uint64_t> packet_send_total = {0};      // n packets in batches we attempted to send (to repacker)
		std::atomic<uint64_t> packet_send_err   = {0};      // n packets that were lost to batch send fails
	} udp;

	std::vector<collector_stats_t> collector_threads;

	struct {
		std::atomic<uint64_t> poll_total          = {0};
		std::atomic<uint64_t> recv_total          = {0};
		std::atomic<uint64_t> recv_eagain         = {0};
		std::atomic<uint64_t> recv_packets        = {0};
		std::atomic<uint64_t> packet_validate_err = {0};
		std::atomic<uint64_t> batch_send_total    = {0};
		std::atomic<uint64_t> batch_send_by_timer = {0};
		std::atomic<uint64_t> batch_send_by_size  = {0};
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
	uint32_t    report_input_buffer;

	pinba_logger_ptr logger;

	bool        packet_debug;           // dump arriving packets to log (at info level)
	double      packet_debug_fraction;  // probability of dumping a single packet (aka, 0.01 = dump roughly every 100th)
};

struct pinba_globals_t : private boost::noncopyable
{
	virtual ~pinba_globals_t() {}

	virtual pinba_stats_t* stats() = 0;             // get shared stats and obey the rules
	// virtual pinba_stats_t  stats_copy() const = 0;  // get your own private copy and use it without locking

	virtual pinba_logger_t*        logger() const = 0;
	virtual pinba_options_t const* options() const = 0;
	virtual pinba_options_t*       options_mutable() = 0;
	virtual dictionary_t*          dictionary() const = 0;
	virtual pinba_os_symbols_t*    os_symbols() const = 0;
};
typedef std::unique_ptr<pinba_globals_t> pinba_globals_ptr;

// init just the globals, simplifies testing for example
pinba_globals_t*  pinba_globals();
pinba_globals_t*  pinba_globals_init(pinba_options_t*);

#define PINBA_STATS_(x)    (pinba_globals()->stats()->x)
#define PINBA_LOGGER_      (pinba_globals()->logger())
#define PINBA_OPTIONS_(x)  (pinba_globals()->options()->x)

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__GLOBALS_H_
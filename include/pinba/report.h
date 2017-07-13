#ifndef PINBA__REPORT_H_
#define PINBA__REPORT_H_

#include <atomic>
#include <mutex>

#include "pinba/globals.h"
#include "pinba/report_key.h"

////////////////////////////////////////////////////////////////////////////////////////////////

#define REPORT_KIND__BY_REQUEST_DATA  0
#define REPORT_KIND__BY_TIMER_DATA    1
#define REPORT_KIND__BY_PACKET_DATA   2

#define HISTOGRAM_KIND__HASHTABLE  0
#define HISTOGRAM_KIND__FLAT       1

struct report_info_t
{
	std::string name;

	int         kind;         // REPORT_KIND__*

	duration_t  time_window;
	uint32_t    tick_count;

	uint32_t    n_key_parts;

	bool        hv_enabled;
	int         hv_kind;      // HISTOGRAM_KIND__*
	uint32_t    hv_bucket_count;
	duration_t  hv_bucket_d;
};

struct report_stats_t
{
	mutable std::mutex lock;

	timeval_t created_tv;
	timeval_t created_realtime_tv;

	std::atomic<uint64_t> batches_send_total          = {0};
	std::atomic<uint64_t> batches_send_err            = {0};
	std::atomic<uint64_t> batches_recv_total          = {0};

	std::atomic<uint64_t> packets_send_total          = {0};
	std::atomic<uint64_t> packets_send_err            = {0};
	std::atomic<uint64_t> packets_recv_total          = {0};

	std::atomic<uint64_t> packets_aggregated          = {0}; // number of packets that we took useful information from
	std::atomic<uint64_t> packets_dropped_by_bloom    = {0}; // number of packets dropped by bloom filter
	std::atomic<uint64_t> packets_dropped_by_filters  = {0}; // number of packets dropped by packet-level filters
	std::atomic<uint64_t> packets_dropped_by_rfield   = {0}; // number of packets dropped by request_field aggregation
	std::atomic<uint64_t> packets_dropped_by_rtag     = {0}; // number of packets dropped by request_tag aggregation
	std::atomic<uint64_t> packets_dropped_by_timertag = {0}; // number of packets dropped by timer_tag aggregation (i.e. no useful timers)

	std::atomic<uint64_t> timers_scanned              = {0}; // number of timers scanned
	std::atomic<uint64_t> timers_aggregated           = {0}; // number of timers that we took useful information from
	std::atomic<uint64_t> timers_skipped_by_filters   = {0}; // number of timers skipped by timertag filters
	std::atomic<uint64_t> timers_skipped_by_tags      = {0}; // number of timers skipped by not having required tags present

	timeval_t  last_tick_tv          = {0,0};       // last tick happened at this time
	duration_t last_tick_prepare_d   = {0};         // how long did last tick processing take
	duration_t last_snapshot_merge_d = {0};         // how long did last snapshot merge take

	timeval_t ru_utime = {0,0};
	timeval_t ru_stime = {0,0};
};

struct report_estimates_t
{
	uint32_t  row_count = 0;
	// uint32_t  padding__;
	uint64_t  mem_used  = 0;
};

// FIXME: pointers in this struct must either be ref counted, or copies
//        since report might be destroyed, while this struct is alive still (in other thread as well)
struct report_state_t
{
	uint32_t             id;
	report_info_t const  *info;
	report_stats_t       *stats;
	report_estimates_t   estimates;
};
using report_state_ptr = std::unique_ptr<report_state_t>;

////////////////////////////////////////////////////////////////////////////////////////////////

struct packet_t;
struct dictionary_t;
struct snapshot_dictionary_t;

struct report_snapshot_t
{
	MEOW_DEFINE_SMART_ENUM_STRUCT(prepare_type,	((full,          "full"))
												((no_histograms, "no_histograms")));

	// this struct is just a placeholder, same size as real hashtable iterator
	// FIXME: what about alignment here?
	// TODO: maybe make iteration completely internal
	//       (i.e. never give position away, just "current data" and expose next/prev/reset/valid)
	struct position_t
	{
		uintptr_t dummy___[3];
	};

	virtual ~report_snapshot_t() {}

	// get global snapshot data
	// this function is here (and not JUST in report_t), to avoid race conditions,
	// i.e. report_info() and get_snapshot() returning slightly different data,
	// due to those being 2 separate function calls (and some packets might get processed in the middle)
	virtual report_info_t const* report_info() const = 0;

	// get dictionary used to translate ids to names, read only
	virtual dictionary_t const* dictionary() const = 0;

	// get thread-local this snapshot only dictionary
	// virtual snapshot_dictionary_t const* snapshot_dictionary() const = 0;

	// prepare snapshot for use
	// MUST be called before any of the functions below
	// this exists primarily to allow preparation to take place in a thread
	// different from the one handling report data (more parallelism, yey)
	virtual void prepare(prepare_type_t = prepare_type::full) = 0;
	virtual bool is_prepared() const = 0;

	// will return 0 if !is_prepared()
	virtual size_t row_count() const = 0;

	// iteration, this should be very cheap
	virtual position_t pos_first() = 0;
	virtual position_t pos_last() = 0;
	virtual position_t pos_next(position_t const&) = 0;
	// commented out, since current impls do not support backward iteration
	// as google::dense_hash_map has forward iterators only
	// virtual position_t pos_prev(position_t const&) = 0;
	virtual bool       pos_equal(position_t const&, position_t const&) const = 0;

	// key handling
	virtual report_key_t     get_key(position_t const&) const = 0;
	virtual report_key_str_t get_key_str(position_t const&) const = 0;

	// data handling
	virtual int   data_kind() const = 0;
	virtual void* get_data(position_t const&) = 0;

	// histograms
	virtual int   histogram_kind() const = 0;
	virtual void* get_histogram(position_t const&) = 0;
};
typedef std::unique_ptr<report_snapshot_t> report_snapshot_ptr;

void debug_dump_report_snapshot(FILE*, report_snapshot_t*, str_ref name = {});

////////////////////////////////////////////////////////////////////////////////////////////////
#if 0
struct report_agg_t : private boost::noncopyable
{
	virtual ~report_agg_t() {}

	virtual void stats_init(report_stats_t *stats) = 0;

	virtual void ticks_init(timeval_t curr_tv) = 0;
	virtual void tick_now(timeval_t curr_tv) = 0;

	virtual void add(packet_t*) = 0;
	virtual void add_multi(packet_t**, uint32_t) = 0;

	virtual report_estimates_t  get_estimates() = 0;
};
#endif
struct report_t : private boost::noncopyable
{
	virtual ~report_t() {}

	virtual str_ref name() const = 0;
	virtual report_info_t const* info() const = 0;

	virtual void stats_init(report_stats_t *stats) = 0;

	virtual void ticks_init(timeval_t curr_tv) = 0;
	virtual void tick_now(timeval_t curr_tv) = 0;

	virtual void add(packet_t*) = 0;
	virtual void add_multi(packet_t**, uint32_t) = 0;

	virtual report_estimates_t  get_estimates() = 0;
	virtual report_snapshot_ptr get_snapshot() = 0;
};
typedef std::shared_ptr<report_t> report_ptr;

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__REPORT_H_
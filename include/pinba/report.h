#ifndef PINBA__REPORT_H_
#define PINBA__REPORT_H_

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

	// uint32_t    row_count; // as max of all timeslice's row count
};

////////////////////////////////////////////////////////////////////////////////////////////////

struct packet_t;
struct dictionary_t;
struct histogram_t;

struct report_snapshot_t
{
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

	// prepare snapshot for use
	// MUST be called before any of the functions below
	// this exists primarily to allow preparation to take place in a thread
	// different from the one handling report data (more parallelism, yey)
	virtual void prepare() = 0;
	virtual bool is_prepared() const = 0;

	// will return 0 if !is_prepared()
	virtual size_t row_count() const = 0;

	// iteration, this should be very cheap
	virtual position_t pos_first() = 0;
	virtual position_t pos_last() = 0;
	virtual position_t pos_next(position_t const&) = 0;
	virtual position_t pos_prev(position_t const&) = 0;
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

void debug_dump_report_snapshot(FILE*, report_snapshot_t*);

////////////////////////////////////////////////////////////////////////////////////////////////

struct report_t : private boost::noncopyable
{
	virtual ~report_t() {}

	virtual str_ref name() const = 0;

	// generic report info, mostly processed from report config
	virtual report_info_t const* info() const = 0;

	// TODO: report kinds need some love, maybe just have separate classes for them
	//       or have this one too (since we'd like to store them all in one place)
	//       or just remove this one, and put kind into report_info_t
	virtual int kind() const = 0;

	virtual void ticks_init(timeval_t curr_tv) = 0;
	virtual void tick_now(timeval_t curr_tv) = 0;

	virtual void add(packet_t*) = 0;
	virtual void add_multi(packet_t**, uint32_t packet_count) = 0;

	virtual report_snapshot_ptr get_snapshot() = 0;
};
typedef std::unique_ptr<report_t> report_ptr;

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__REPORT_H_
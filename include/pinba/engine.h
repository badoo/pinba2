#ifndef PINBA__ENGINE_H_
#define PINBA__ENGINE_H_

#include "pinba/globals.h"
#include "pinba/report.h"

////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: maybe have this as global registry for all threaded objects here
//       and not just explicit stats, but everything (and ticker for example!)
struct pinba_engine_t : private boost::noncopyable
{
	virtual ~pinba_engine_t() {}

	virtual void startup() = 0;
	virtual void shutdown() = 0;

	virtual pinba_globals_t*       globals() const = 0;
	virtual pinba_options_t const* options() const = 0;

	virtual pinba_error_t add_report(report_ptr) = 0;
	virtual pinba_error_t delete_report(str_ref name) = 0;
	// virtual maybe_t<report_ptr> create_report(report_conf___by_request_t*) = 0;
	// virtual maybe_t<report_ptr> create_report(report_conf___by_timer_t*) = 0;

	virtual pinba_error_t start_report_with_config(report_conf___by_request_t const&) = 0;
	virtual pinba_error_t start_report_with_config(report_conf___by_timer_t const&) = 0;

	virtual report_snapshot_ptr get_report_snapshot(str_ref name) = 0;
};
typedef std::unique_ptr<pinba_engine_t> pinba_engine_ptr;

// init the whoile engine (automatically inits globals for itself)
pinba_engine_ptr  pinba_engine_init(pinba_options_t*);

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__ENGINE_H_

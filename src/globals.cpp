#include <string>

#include <nanomsg/pipeline.h>
#include <nanomsg/pubsub.h>
#include <nanomsg/reqrep.h>

#include <meow/intrusive_ptr.hpp>

#include "pinba/globals.h"
#include "pinba/dictionary.h"
#include "pinba/coordinator.h"
#include "pinba/collector.h"
#include "pinba/repacker.h"
#include "pinba/report_by_request.h"
#include "pinba/report_by_timer.h"

#include "pinba/nmsg_ticker.h"

////////////////////////////////////////////////////////////////////////////////////////////////

struct pinba_globals_impl_t : public pinba_globals_t
{
	pinba_globals_impl_t(pinba_options_t *options)
		: options_(options)
	{
		ticker_ = meow::make_unique<nmsg_ticker___single_thread_t>();
		dictionary_ = meow::make_unique<dictionary_t>();
	}

	virtual void startup()
	{
		static collector_conf_t collector_conf = {
			.address       = options_->net_address,
			.port          = options_->net_port,
			.nn_output     = "inproc://udp-collector",
			.n_threads     = options_->udp_threads,
			.batch_size    = options_->udp_batch_messages,
			.batch_timeout = options_->udp_batch_timeout,
		};
		collector_ = create_collector(this, &collector_conf);

		static repacker_conf_t repacker_conf = {
			.nn_input        = collector_conf.nn_output,
			.nn_output       = "inproc://repacker",
			.nn_input_buffer = options_->repacker_input_buffer,
			.n_threads       = options_->repacker_threads,
			.batch_size      = options_->repacker_batch_messages,
			.batch_timeout   = options_->repacker_batch_timeout,
		};
		repacker_ = create_repacker(this, &repacker_conf);

		static coordinator_conf_t coordinator_conf = {
			.nn_input                = repacker_conf.nn_output,
			.nn_input_buffer         = options_->coordinator_input_buffer,
			.nn_control              = "inproc://coordinator/control",
			.nn_report_output        = "inproc://coordinator/report-data",
			.nn_report_output_buffer = 16,
		};
		coordinator_ = create_coordinator(this, &coordinator_conf);

		coordinator_->startup();
		repacker_->startup();
		collector_->startup();
	}

	virtual nmsg_ticker_t* ticker() const override { return ticker_.get(); }
	virtual dictionary_t*  dictionary() const override { return dictionary_.get(); }

	virtual bool create_report_by_request(report_conf___by_request_t *conf) override
	{
		auto req = meow::make_intrusive<coordinator_request___add_report_t>();
		req->report = meow::make_unique<report___by_request_t>(this, conf);

		auto const result = coordinator_->request(req);

		assert(COORDINATOR_RES__GENERIC == result->type);
		auto const *r = static_cast<coordinator_response___generic_t*>(result.get());

		if (COORDINATOR_STATUS__OK == r->status)
			return true;

		throw std::runtime_error(ff::fmt_str("{0}; error: {1}", __func__, r->err.what()));
	}

	virtual bool create_report_by_timer(report_conf___by_timer_t *conf) override
	{
		auto req = meow::make_intrusive<coordinator_request___add_report_t>();
		req->report = meow::make_unique<report___by_timer_t>(this, conf);

		auto const result = coordinator_->request(req);

		assert(COORDINATOR_RES__GENERIC == result->type);
		auto const *r = static_cast<coordinator_response___generic_t*>(result.get());

		if (COORDINATOR_STATUS__OK == r->status)
			return true;

		throw std::runtime_error(ff::fmt_str("{0}; error: {1}", __func__, r->err.what()));
	}

	virtual report_snapshot_ptr get_report_snapshot(str_ref name) override
	{
		auto req = meow::make_intrusive<coordinator_request___get_report_snapshot_t>();
		req->report_name = name.str();

		auto const result = coordinator_->request(req);

		if (COORDINATOR_RES__REPORT_SNAPSHOT == result->type)
		{
			auto *r = static_cast<coordinator_response___report_snapshot_t*>(result.get());
			return move(r->snapshot);
		}
		else
		{
			assert(COORDINATOR_RES__GENERIC == result->type);
			auto const *r = static_cast<coordinator_response___generic_t*>(result.get());
			throw std::runtime_error(ff::fmt_str("{0}; error: {1}", __func__, r->err.what()));
		}

		assert(!"unreachable");
	}

private:
	pinba_options_t *options_;

	std::unique_ptr<collector_t>   collector_;
	std::unique_ptr<repacker_t>    repacker_;
	std::unique_ptr<coordinator_t> coordinator_;

	std::unique_ptr<nmsg_ticker_t> ticker_;
	std::unique_ptr<dictionary_t>  dictionary_;
};

////////////////////////////////////////////////////////////////////////////////////////////////

pinba_globals_ptr pinba_init(pinba_options_t *options)
{
	return meow::make_unique<pinba_globals_impl_t>(options);
}
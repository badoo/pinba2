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
namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

	struct pinba_globals_impl_t : public pinba_globals_t
	{
		pinba_globals_impl_t(pinba_options_t *options)
			: options_(options)
		{
			ticker_     = meow::make_unique<nmsg_ticker___single_thread_t>();
			dictionary_ = meow::make_unique<dictionary_t>();

			stats_.start_tv          = os_unix::clock_monotonic_now();
			stats_.start_realtime_tv = os_unix::clock_gettime_ex(CLOCK_REALTIME);
		}

	private:

		virtual pinba_stats_t* stats() override
		{
			return &stats_;
		}

		// virtual pinba_stats_t stats_copy() const override
		// {
		// 	std::lock_guard<std::mutex> lk_(stats_.mtx);
		// 	return *static_cast<pinba_stats_t const*>(&stats_);
		// }

		virtual pinba_options_t const* options() const override
		{
			return options_;
		}

		virtual nmsg_ticker_t* ticker() const override
		{
			return ticker_.get();
		}

		virtual dictionary_t*  dictionary() const override
		{
			return dictionary_.get();
		}

	private:
		pinba_options_t                *options_;

		pinba_stats_t                  stats_;
		std::unique_ptr<nmsg_ticker_t> ticker_;
		std::unique_ptr<dictionary_t>  dictionary_;
	};

////////////////////////////////////////////////////////////////////////////////////////////////

	struct pinba_engine_impl_t : public pinba_engine_t
	{
		pinba_engine_impl_t(pinba_options_t *options)
		{
			globals_ = pinba_globals_init(options);
		}

	private:

		virtual void startup()
		{
			auto const *options = this->options();

			static collector_conf_t collector_conf = {
				.address       = options->net_address,
				.port          = options->net_port,
				.nn_output     = "inproc://udp-collector",
				.n_threads     = options->udp_threads,
				.batch_size    = options->udp_batch_messages,
				.batch_timeout = options->udp_batch_timeout,
			};
			collector_ = create_collector(this->globals(), &collector_conf);

			static repacker_conf_t repacker_conf = {
				.nn_input        = collector_conf.nn_output,
				.nn_output       = "inproc://repacker",
				.nn_input_buffer = options->repacker_input_buffer,
				.n_threads       = options->repacker_threads,
				.batch_size      = options->repacker_batch_messages,
				.batch_timeout   = options->repacker_batch_timeout,
			};
			repacker_ = create_repacker(this->globals(), &repacker_conf);

			static coordinator_conf_t coordinator_conf = {
				.nn_input                = repacker_conf.nn_output,
				.nn_input_buffer         = options->coordinator_input_buffer,
				.nn_control              = "inproc://coordinator/control",
				.nn_report_output        = "inproc://coordinator/report-data",
				.nn_report_output_buffer = 16,
			};
			coordinator_ = create_coordinator(this->globals(), &coordinator_conf);

			coordinator_->startup();
			repacker_->startup();
			collector_->startup();
		}

		virtual pinba_globals_t* globals() const override
		{
			return globals_.get();
		}

		virtual pinba_options_t const* options() const override
		{
			return globals_->options();
		}

		virtual pinba_error_t add_report(report_ptr report)
		{
			auto req = meow::make_intrusive<coordinator_request___add_report_t>();
			req->report = move(report);

			auto const result = coordinator_->request(req);

			assert(COORDINATOR_RES__GENERIC == result->type);
			auto const *r = static_cast<coordinator_response___generic_t*>(result.get());

			if (COORDINATOR_STATUS__OK == r->status)
				return {};

			return std::move(r->err);
		}

		virtual pinba_error_t start_report_with_config(report_conf___by_request_t const& conf) override
		{
			return this->add_report(meow::make_unique<report___by_request_t>(this->globals(), conf));
		}

		virtual pinba_error_t start_report_with_config(report_conf___by_timer_t const& conf) override
		{
			return this->add_report(meow::make_unique<report___by_timer_t>(this->globals(), conf));
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
		std::unique_ptr<pinba_globals_t>  globals_;
		std::unique_ptr<collector_t>      collector_;
		std::unique_ptr<repacker_t>       repacker_;
		std::unique_ptr<coordinator_t>    coordinator_;
	};

////////////////////////////////////////////////////////////////////////////////////////////////
}} // namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

pinba_globals_ptr pinba_globals_init(pinba_options_t *options)
{
	return meow::make_unique<aux::pinba_globals_impl_t>(options);
}

pinba_engine_ptr pinba_engine_init(pinba_options_t *options)
{
	return meow::make_unique<aux::pinba_engine_impl_t>(options);
}

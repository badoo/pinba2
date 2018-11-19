#include "pinba_config.h"

#include <string>

#include <nanomsg/pipeline.h>
#include <nanomsg/pubsub.h>
#include <nanomsg/reqrep.h>

#define MEOW_FORMAT_FD_SINK_NO_WRITEV 1
#include <meow/logging/fd_logger.hpp>

#include "pinba/globals.h"
#include "pinba/os_symbols.h"
#include "pinba/engine.h"
#include "pinba/dictionary.h"
#include "pinba/coordinator.h"
#include "pinba/collector.h"
#include "pinba/repacker.h"

////////////////////////////////////////////////////////////////////////////////////////////////
namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

	struct pinba_globals_impl_t : public pinba_globals_t
	{
		pinba_globals_impl_t(pinba_options_t *options)
			: options_(options)
		{
			logger_ = (options->logger)
					? options->logger
					: std::make_shared<meow::logging::fd_logger_t<meow::logging::empty_prefix_t>>(STDERR_FILENO);

			// ticker_     = meow::make_unique<nmsg_ticker___single_thread_t>();
			dictionary_ = meow::make_unique<dictionary_t>();

			// NOTE: passing not fully constructed this ptr is fine here
			//       but it's a fine line to walk, mon
			os_symbols_ = pinba_os_symbols___init(this);

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

		virtual pinba_logger_t* logger() const override
		{
			return logger_.get();
		}

		virtual pinba_options_t const* options() const override
		{
			return options_;
		}

		virtual pinba_options_t* options_mutable() override
		{
			return options_;
		}

		virtual dictionary_t*  dictionary() const override
		{
			return dictionary_.get();
		}

		virtual pinba_os_symbols_t*    os_symbols() const override
		{
			return os_symbols_.get();
		}

	private:
		pinba_options_t                *options_;

		pinba_logger_ptr               logger_;
		pinba_stats_t                  stats_;
		std::unique_ptr<dictionary_t>  dictionary_;
		pinba_os_symbols_ptr           os_symbols_;
	};



	static std::unique_ptr<pinba_globals_impl_t> pinba_globals_;

	pinba_globals_t* pinba_globals_init___impl(pinba_options_t *options)
	{
		pinba_globals_ = meow::make_unique<pinba_globals_impl_t>(options);
		return pinba_globals_.get();
	}

////////////////////////////////////////////////////////////////////////////////////////////////

	struct pinba_engine_impl_t : public pinba_engine_t
	{
		pinba_engine_impl_t(pinba_options_t *options)
		{
			globals_ = pinba_globals_init(options);
		}

	private:

		virtual void startup() override
		{
			auto const *options = this->options();

			static collector_conf_t collector_conf = {
				.address       = options->net_address,
				.port          = options->net_port,
				.nn_output     = "inproc://udp-collector",
				.nn_shutdown   = "inproc://udp-collector/shutdown",
				.n_threads     = options->udp_threads,
				.batch_size    = options->udp_batch_messages,
				.batch_timeout = options->udp_batch_timeout,
			};
			collector_ = create_collector(this->globals(), &collector_conf);

			static repacker_conf_t repacker_conf = {
				.nn_input        = collector_conf.nn_output,
				.nn_output       = "inproc://repacker",
				.nn_shutdown     = "inproc://repacker/shutdown",
				.nn_input_buffer = options->repacker_input_buffer,
				.n_threads       = options->repacker_threads,
				.batch_size      = options->repacker_batch_messages,
				.batch_timeout   = options->repacker_batch_timeout,
			};
			repacker_ = create_repacker(this->globals(), &repacker_conf);

			static coordinator_conf_t coordinator_conf = {
				.nn_input               = repacker_conf.nn_output,
				.nn_input_buffer        = options->coordinator_input_buffer,
				.nn_control             = "inproc://coordinator/control",
				.nn_report_input_buffer = options->report_input_buffer,
			};
			coordinator_ = create_coordinator(this->globals(), &coordinator_conf);

			coordinator_->startup();
			repacker_->startup();
			collector_->startup();
		}

		virtual void shutdown() override
		{
			collector_.reset();
			repacker_.reset();
			coordinator_.reset();
		}

		virtual pinba_globals_t* globals() const override
		{
			return globals_;
		}

		virtual pinba_options_t const* options() const override
		{
			return globals_->options();
		}

		virtual pinba_options_t* options_mutable() override
		{
			return globals_->options_mutable();
		}

		virtual pinba_error_t add_report(report_ptr report) override
		{
			return coordinator_->add_report(report);
		}

		virtual pinba_error_t delete_report(str_ref name) override
		{
			return coordinator_->delete_report(name.str());
		}

		virtual report_state_ptr get_report_state(str_ref name) override
		{
			return coordinator_->get_report_state(name.str());
		}

		virtual report_snapshot_ptr get_report_snapshot(str_ref name) override
		{
			return coordinator_->get_report_snapshot(name.str());
		}

	private:
		// std::unique_ptr<pinba_globals_t>  globals_;
		pinba_globals_t                   *globals_;
		std::unique_ptr<collector_t>      collector_;
		std::unique_ptr<repacker_t>       repacker_;
		std::unique_ptr<coordinator_t>    coordinator_;
	};

////////////////////////////////////////////////////////////////////////////////////////////////
}} // namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

// export version and build info symbols explicitly
extern "C" {
	volatile char const pinba_version_info[] = "pinba_version_info " PINBA_VERSION " git: " PINBA_VCS_FULL_HASH " modified: " PINBA_VCS_WC_MODIFIED;
	volatile char const pinba_build_string[] = "pinba_build_string " PINBA_BUILD_STRING;
}

////////////////////////////////////////////////////////////////////////////////////////////////

pinba_globals_t* pinba_globals()
{
	return aux::pinba_globals_.get();
}

pinba_globals_t* pinba_globals_init(pinba_options_t *options)
{
	return aux::pinba_globals_init___impl(options);
}

pinba_engine_ptr pinba_engine_init(pinba_options_t *options)
{
	return meow::make_unique<aux::pinba_engine_impl_t>(options);
}

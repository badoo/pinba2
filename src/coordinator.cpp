#include "pinba_config.h"

#include <string>
#include <thread>
#include <unordered_map>

#include <nanomsg/pipeline.h>
#include <nanomsg/pubsub.h>
#include <nanomsg/reqrep.h>

#include <meow/unix/resource.hpp> // getrusage_ex

#include "pinba/globals.h"
#include "pinba/os_symbols.h"
#include "pinba/repacker.h"
#include "pinba/coordinator.h"
#include "pinba/report.h"

#include "pinba/nmsg_socket.h"
#include "pinba/nmsg_poller.h"

////////////////////////////////////////////////////////////////////////////////////////////////
namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

	struct report_host_conf_t
	{
		uint32_t    id;

		std::string name;
		std::string thread_name;

		std::string nn_control;         // control messages and stuff
		std::string nn_shutdown;        // shutdown message

		std::string nn_packets;         // get packet_batch_ptr from this endpoint as fast as possible (SUB, pair to coodinator PUB)
		size_t      nn_packets_buffer;  // NN_RCVBUF on nn_packets
	};

	struct report_host_t;
	using  report_host_call_func_t = std::function<void(report_host_t*)>;

	struct report_host_t
	{
		virtual ~report_host_t() {}
		virtual void startup(report_ptr) = 0;
		virtual void shutdown() = 0;

		virtual uint32_t           id() const = 0;
		virtual report_t*          report() const = 0;
		virtual report_agg_t*      report_agg() const = 0;
		virtual report_history_t*  report_history() const = 0;
		virtual report_stats_t*    stats() = 0;

		virtual bool process_batch(packet_batch_ptr) = 0;
		virtual void execute_in_thread(report_host_call_func_t const&) = 0;
	};
	typedef std::unique_ptr<report_host_t> report_host_ptr;

	struct report_host_req_t : public nmsg_message_t
	{
		report_host_call_func_t func;

		template<class Function>
		report_host_req_t(Function const& f)
			: func(f)
		{
		}
	};
	typedef boost::intrusive_ptr<report_host_req_t> report_host_req_ptr;

	struct report_host_result_t : public nmsg_message_t
	{};
	typedef boost::intrusive_ptr<report_host_result_t> report_host_result_ptr;

	struct report_host___new_thread_t : public report_host_t
	{
		pinba_globals_t        *globals_;
		report_host_conf_t     conf_;

		std::thread            t_;

		nmsg_socket_t          packets_send_sock_;
		nmsg_socket_t          packets_recv_sock_;

		// *_cli_sock_ + *_mtx_ are required for
		// dirty workaround for https://github.com/nanomsg/nanomsg/issues/575

		nmsg_socket_t          control_sock_;
		nmsg_socket_t          control_cli_sock_;
		std::mutex             control_mtx_;

		nmsg_socket_t          shutdown_sock_;
		nmsg_socket_t          shutdown_cli_sock_;
		std::mutex             shutdown_mtx_;

		report_ptr             report_;
		report_agg_ptr         report_agg_;
		report_history_ptr     report_history_;
		report_stats_t         stats_;

		repacker_state_ptr     repacker_state_;

	public:

		report_host___new_thread_t(pinba_globals_t *globals, report_host_conf_t const& conf)
			: globals_(globals)
			, conf_(conf)
		{
			packets_send_sock_
				.open(AF_SP, NN_PUSH)
				.bind(conf_.nn_packets);

			packets_recv_sock_
				.open(AF_SP, NN_PULL)
				.set_option(NN_SOL_SOCKET, NN_RCVBUF, sizeof(packet_batch_ptr) * conf_.nn_packets_buffer, ff::fmt_str("{0}/in_sock", conf_.name))
				.connect(conf_.nn_packets);

			control_sock_
				.open(AF_SP, NN_REP)
				.bind(conf_.nn_control);

			control_cli_sock_
				.open(AF_SP, NN_REQ)
				.connect(conf_.nn_control);

			shutdown_sock_
				.open(AF_SP, NN_REP)
				.bind(conf_.nn_shutdown);

			shutdown_cli_sock_
				.open(AF_SP, NN_REQ)
				.connect(conf_.nn_shutdown);

			stats_.created_tv          = os_unix::clock_gettime_ex(CLOCK_MONOTONIC);
			stats_.created_realtime_tv = os_unix::clock_gettime_ex(CLOCK_REALTIME);
		}

	public:

		virtual void startup(report_ptr incoming_report) override
		{
			if (report_)
				throw std::logic_error(ff::fmt_str("report handler {0} is already started", conf_.name));

			report_ = incoming_report;

			//

			auto const *rinfo        = report_->info();
			auto const tick_interval = rinfo->time_window / rinfo->tick_count;

			// FIXME(antoxa): temporary solution to aid migration from hash to hdr histograms
			// create objects early, to avoid exceptions inside worker thread
			report_agg_ = report_->create_aggregator();
			report_agg_->stats_init(&stats_);

			report_history_ = report_->create_history();
			report_history_->stats_init(&stats_);

			std::atomic_thread_fence(std::memory_order_seq_cst);

			std::thread t([this, tick_interval]()
			{
				PINBA___OS_CALL(globals_, set_thread_name, conf_.thread_name);

				MEOW_DEFER(
					LOG_DEBUG(globals_->logger(), "{0}; exiting", conf_.thread_name);
				);

				//

				nmsg_poller_t poller;
				poller
					.ticker(tick_interval, [this](timeval_t now)
					{
						report_tick_ptr tick = report_agg_->tick_now(now);
						tick->repacker_state = std::move(repacker_state_);

						report_history_->merge_tick(tick);

						timeval_t const curr_tv    = os_unix::clock_monotonic_now();
						timeval_t const curr_rt_tv = os_unix::clock_gettime_ex(CLOCK_REALTIME);

						std::unique_lock<std::mutex> lk_(stats_.lock);
						stats_.last_tick_tv        = curr_rt_tv;
						stats_.last_tick_prepare_d = duration_from_timeval(curr_tv - now);
					})
					.ticker(1 * d_second, [this](timeval_t now)
					{
						os_rusage_t ru = os_unix::getrusage_ex(RUSAGE_THREAD);

						std::unique_lock<std::mutex> lk_(stats_.lock);
						stats_.ru_utime = timeval_from_os_timeval(ru.ru_utime);
						stats_.ru_stime = timeval_from_os_timeval(ru.ru_stime);
					})
					.read_nn_socket(packets_recv_sock_, [this](timeval_t now)
					{
						auto const batch = packets_recv_sock_.recv<packet_batch_ptr>();

						stats_.batches_recv_total += 1;
						stats_.packets_recv_total += batch->packet_count;

						repacker_state___merge_to_from(repacker_state_, batch->repacker_state);

						report_agg_->add_multi(batch->packets, batch->packet_count);
					})
					.read_nn_socket(control_sock_, [this](timeval_t now)
					{
						auto const req = control_sock_.recv<report_host_req_ptr>();
						req->func(this);
						control_sock_.send(meow::make_intrusive<report_host_result_t>());
					})
					.read_nn_socket(shutdown_sock_, [this, &poller](timeval_t)
					{
						shutdown_sock_.recv<int>();
						poller.set_shutdown_flag(); // exit loop() after this iteration
						shutdown_sock_.send(1);
					})
					.loop();
			});

			t_ = move(t);
		}

		virtual bool process_batch(packet_batch_ptr batch) override
		{
			stats_.batches_send_total += 1;
			stats_.packets_send_total += batch->packet_count;

			bool const success = packets_send_sock_.send_message(batch, NN_DONTWAIT);
			if (!success)
			{
				stats_.batches_send_err += 1;
				stats_.packets_send_err += batch->packet_count;
			}
			return success;
		}

		virtual uint32_t id() const override
		{
			return conf_.id;
		}

		virtual report_t* report() const override
		{
			return report_.get();
		}

		virtual report_agg_t* report_agg() const override
		{
			return report_agg_.get();
		}

		virtual report_history_t* report_history() const override
		{
			return report_history_.get();
		}

		virtual report_stats_t* stats() override
		{
			return &stats_;
		}

		virtual void execute_in_thread(report_host_call_func_t const& func) override
		{
			// lock, so that multiple clients do not step on each other's toes
			// we have is just one client currently, but keep same pattern as in other places
			// where multiple clients are present
			std::unique_lock<std::mutex> lk_(control_mtx_);

			control_cli_sock_.send_message(meow::make_intrusive<report_host_req_t>(func));
			control_cli_sock_.recv<report_host_result_ptr>();
		}

		virtual void shutdown() override
		{

			{
				std::unique_lock<std::mutex> lk_(shutdown_mtx_);

				shutdown_cli_sock_.send(1);
				shutdown_cli_sock_.recv<int>();
			}

			t_.join();
		}
	};

////////////////////////////////////////////////////////////////////////////////////////////////

	struct relay_worker_t : private boost::noncopyable
	{
		relay_worker_t(pinba_globals_t *globals, coordinator_conf_t *conf)
			: globals_(globals)
			, stats_(globals->stats())
			, conf_(conf)
		{
			in_sock_ = nmsg_socket(AF_SP, NN_PULL);
			if (conf_->nn_input_buffer > 0)
				in_sock_.set_option(NN_SOL_SOCKET, NN_RCVBUF, conf_->nn_input_buffer * sizeof(packet_batch_ptr), conf_->nn_input);

			control_sock_
				.open(AF_SP, NN_REP)
				.bind(conf_->nn_control);

			control_cli_sock_
				.open(AF_SP, NN_REQ)
				.connect(conf_->nn_control);
		}

		~relay_worker_t()
		{
			this->shutdown();
		}

		void startup()
		{
			in_sock_.connect(conf_->nn_input);

			std::thread t([this]()
			{
				this->worker_thread();
			});
			thread_ = move(t);
		}

		void shutdown()
		{
			if (!thread_.joinable()) // has actually started and not shut down yet
				return;

			// tell relay to stop operation
			auto const err = this->execute_in_thread([this]()
			{
				poller_.set_shutdown_flag();
			});

			if (err)
			{
				LOG_ALERT(globals_->logger(), "couldn't stop relay thread: {0}, aborting", err);
			}

			thread_.join();
		}

	public:

		using request_func_t = std::function<void()>;

		struct request_t : public nmsg_message_t
		{
			request_func_t func;
		};
		using request_ptr = boost::intrusive_ptr<request_t>;

		struct response_t : public nmsg_message_t
		{
			pinba_error_t err;
		};
		using response_ptr = boost::intrusive_ptr<response_t>;


		pinba_error_t execute_in_thread(request_func_t const& func)
		{
			auto req = meow::make_intrusive<request_t>();
			req->func = func;

			response_ptr response;
			{
				std::unique_lock<std::mutex> lk_(control_mtx_);

				control_cli_sock_.send_message(req);
				response = control_cli_sock_.recv<response_ptr>();
			}

			return std::move(response->err);
		}

	private:

		void worker_thread()
		{
			std::string const thr_name = ff::fmt_str("packet-relay");

			PINBA___OS_CALL(globals_, set_thread_name, thr_name);

			MEOW_DEFER(
				LOG_DEBUG(globals_->logger(), "{0}; exiting", thr_name);
			);

			poller_
				.ticker(1000 * d_millisecond, [this](timeval_t now)
				{
					// FIXME: rename stats

					// update accumulated rusage
					os_rusage_t const ru = os_unix::getrusage_ex(RUSAGE_THREAD);

					std::lock_guard<std::mutex> lk_(stats_->mtx);
					stats_->coordinator.ru_utime = timeval_from_os_timeval(ru.ru_utime);
					stats_->coordinator.ru_stime = timeval_from_os_timeval(ru.ru_stime);
				})
				.read_nn_socket(in_sock_, [this](timeval_t now)
				{
					auto batch = in_sock_.recv<packet_batch_ptr>();

					++stats_->coordinator.batches_received;

					// FIXME
					// special counter for batches that were dropped, because no recepients were active
					// if (rhosts_.empty())
					// 	++stats_->coordinator.batches_send_dropped;

					// relay the batch to all reports,
					// TODO(antoxa): maybe move this to a separate thread?
					// NOTE:
					// this is fundamentally broken and can't be implemented with nanomsg PUB/SUB sadly
					// since we have no idea if the report handler is slow, and in that case messages will be dropped
					// and ref counts will not be decremented and memory will leak and we won't have any stats about that either
					// (we also have no need for the pub/sub routing part at the moment and probably won't need it ever)
					for (auto& report_host : rhosts_)
					{
						++stats_->coordinator.batch_send_total;
						bool const success = report_host.second->process_batch(batch);
						if (!success)
						{
							++stats_->coordinator.batch_send_err;
							// TODO: add packet counter here
						}
					}
				})
				.read_nn_socket(control_sock_, [this](timeval_t now)
				{
					auto const req = this->control_sock_.recv<request_ptr>();

					auto const result = meow::make_intrusive<response_t>();

					try
					{
						auto *r = static_cast<request_t*>(req.get());
						r->func();
					}
					catch (std::exception const& e)
					{
						result->err = ff::fmt_err("packet_relay::call: {0}", e.what());
					}

					control_sock_.send_message(result);
				})
				.loop();
		}

	public:
		pinba_globals_t     *globals_;
		pinba_stats_t       *stats_;
		coordinator_conf_t  *conf_;

		// report_name -> report_host*
		using rhost_map_t = std::unordered_map<std::string, report_host_t*>;
		rhost_map_t         rhosts_;

		nmsg_poller_t       poller_;

		nmsg_socket_t       in_sock_;
		nmsg_socket_t       control_sock_;
		nmsg_socket_t       control_cli_sock_;
		std::mutex          control_mtx_;

		std::thread         thread_;
	};

////////////////////////////////////////////////////////////////////////////////////////////////

	struct coordinator_impl_t : public coordinator_t
	{
		coordinator_impl_t(pinba_globals_t *globals, coordinator_conf_t *conf)
			: globals_(globals)
			, stats_(globals->stats())
			, conf_(conf)
			, next_report_id_(0)
			, relay_(globals, conf)
		{
		}

		~coordinator_impl_t()
		{
			this->shutdown();
		}

	private:

		virtual void startup() override
		{
			relay_.startup();
		}

		virtual void shutdown() override
		{
			// tell relay to stop operation
			relay_.shutdown();

			// shutdown all reports
			for (auto& report_host : report_hosts_)
				report_host.second->shutdown();

			report_hosts_.clear();
		}

		virtual pinba_error_t add_report(report_ptr report) override
		{
			std::unique_lock<std::mutex> lk_(mtx_);


			std::string const report_name = report->name().str();

			auto const it = report_hosts_.find(report_name);
			if (it != report_hosts_.end())
				return ff::fmt_err("report already exists: {0}", report_name);


			LOG_DEBUG(globals_->logger(), "creating report {0}", report_name);

			auto const thread_id   = next_report_id_++;
			auto const thr_name    = ff::fmt_str("rh/{0}", thread_id);
			auto const rh_name     = ff::fmt_str("rh/{0}/{1}", thread_id, report_name);

			report_host_conf_t const rh_conf = {
				.id                = thread_id,
				.name              = rh_name,
				.thread_name       = thr_name,
				.nn_control        = ff::fmt_str("inproc://{0}/control", rh_name),
				.nn_shutdown       = ff::fmt_str("inproc://{0}/shutdown", rh_name),
				.nn_packets        = ff::fmt_str("inproc://{0}/packets", rh_name),
				.nn_packets_buffer = conf_->nn_report_input_buffer,
			};

			auto  rh = meow::make_unique<report_host___new_thread_t>(globals_, rh_conf);
			auto *rh_ptr = rh.get(); // save pointer to pass to relay_call()

			rh->startup(report);

			// add report to relay thread
			{
				auto const err = relay_.execute_in_thread([this, report_name, rh_ptr]()
				{
					relay_.rhosts_.emplace(report_name, rh_ptr);
				});

				if (err)
					return err;
			}

			// add report to our hash as well
			report_hosts_.emplace(report_name, move(rh));
			return {};
		}

		virtual pinba_error_t delete_report(std::string const& report_name) override
		{
			std::unique_lock<std::mutex> lk_(mtx_);


			auto const it = report_hosts_.find(report_name);
			if (it == report_hosts_.end())
				return ff::fmt_err("unknown report: {0}", report_name);

			LOG_DEBUG(globals_->logger(), "removing report {0}", report_name);

			// remove report from relay thread
			{
				auto const err = relay_.execute_in_thread([this, &report_name]()
				{
					auto const n_erased = relay_.rhosts_.erase(report_name);
					assert ((n_erased == 1) && "BUG: report found by coordinator, but not found by relay thread");
				});

				if (err)
					return err;
			}

			// can shutdown and remove the report now
			report_host_t *host = it->second.get();
			host->shutdown(); // waits for host to completely shut itself down

			auto const n_erased = report_hosts_.erase(report_name);
			assert((n_erased == 1) && "BUG: report found initially, but nonexistent on erase");

			return {};
		}

		virtual report_snapshot_ptr get_report_snapshot(std::string const& report_name) override
		{
			std::unique_lock<std::mutex> lk_(mtx_);


			auto const it = report_hosts_.find(report_name);
			if (it == report_hosts_.end())
				throw std::runtime_error(ff::fmt_str("unknown report: {0}", report_name));

			report_host_t *host = it->second.get();

			report_snapshot_ptr snapshot;
			host->execute_in_thread([&](report_host_t *rhost)
			{
				snapshot = rhost->report_history()->get_snapshot();
			});

			return snapshot;
		}

		virtual report_state_ptr get_report_state(std::string const& report_name) override
		{
			std::unique_lock<std::mutex> lk_(mtx_);


			auto const it = report_hosts_.find(report_name);
			if (it == report_hosts_.end())
				throw std::runtime_error(ff::fmt_str("unknown report: {0}", report_name));

			report_host_t *host = it->second.get();

			auto state = meow::make_unique<report_state_t>();
			host->execute_in_thread([&](report_host_t *rhost)
			{
				state->id        = rhost->id();
				state->stats     = rhost->stats();
				state->info      = *rhost->report()->info();

				auto const a_est = rhost->report_agg()->get_estimates();
				auto const h_est = rhost->report_history()->get_estimates();

				state->estimates.row_count = h_est.row_count ? h_est.row_count : a_est.row_count;
				state->estimates.mem_used = h_est.mem_used + a_est.mem_used;
			});

			return state;
		}

	private:
		pinba_globals_t     *globals_;
		pinba_stats_t       *stats_;
		coordinator_conf_t  *conf_;

		std::mutex          mtx_;

		// report_name -> report_host
		using rhost_map_t = std::unordered_map<std::string, report_host_ptr>;
		rhost_map_t         report_hosts_;
		uint32_t            next_report_id_;

		relay_worker_t      relay_;
	};

////////////////////////////////////////////////////////////////////////////////////////////////
}} // namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

coordinator_ptr create_coordinator(pinba_globals_t *globals, coordinator_conf_t *conf)
{
	return meow::make_unique<aux::coordinator_impl_t>(globals, conf);
}

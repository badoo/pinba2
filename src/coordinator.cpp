#include <string>
#include <thread>
#include <unordered_map>

#include <nanomsg/pipeline.h>
#include <nanomsg/pubsub.h>
#include <nanomsg/reqrep.h>

#include <meow/intrusive_ptr.hpp>
#include <meow/unix/resource.hpp> // getrusage_ex

#include "pinba/globals.h"
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
		std::string name;
		std::string thread_name;

		std::string nn_reqrep;          // control messages and stuff
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

		virtual report_t* report() const = 0;
		virtual report_stats_t* stats() = 0;

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

		nmsg_socket_t          reqrep_sock_;
		nmsg_socket_t          shutdown_sock_;

		report_ptr             report_;
		report_stats_t         stats_;

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

			reqrep_sock_
				.open(AF_SP, NN_REP)
				.bind(conf_.nn_reqrep);

			shutdown_sock_
				.open(AF_SP, NN_REP)
				.bind(conf_.nn_shutdown);

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

			std::thread t([this, tick_interval]()
			{
				pthread_setname_np(pthread_self(), conf_.thread_name.c_str());

				MEOW_DEFER(
					LOG_DEBUG(globals_->logger(), "{0}; exiting", conf_.thread_name);
				);


				report_->ticks_init(os_unix::clock_monotonic_now());

				nmsg_poller_t poller;
				poller
					.ticker(tick_interval, [this](timeval_t now)
					{
						report_->tick_now(now);

						timeval_t const curr_tv         = os_unix::clock_monotonic_now();
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

						report_->add_multi(batch->packets, batch->packet_count);
					})
					.read_nn_socket(reqrep_sock_, [this](timeval_t now)
					{
						auto const req = reqrep_sock_.recv<report_host_req_ptr>();
						req->func(this);
						reqrep_sock_.send(meow::make_intrusive<report_host_result_t>());
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

		virtual report_t* report() const override
		{
			return report_.get();
		}

		virtual report_stats_t* stats()
		{
			return &stats_;
		}

		virtual void execute_in_thread(report_host_call_func_t const& func) override
		{
			nmsg_socket_t sock;
			sock.open(AF_SP, NN_REQ).connect(conf_.nn_reqrep);

			sock.send_message(meow::make_intrusive<report_host_req_t>(func));
			sock.recv<report_host_result_ptr>();
		}

		virtual void shutdown() override
		{
			nmsg_socket_t sock;
			sock.open(AF_SP, NN_REQ).connect(conf_.nn_shutdown);

			sock.send(1);
			sock.recv<int>();

			t_.join();
		}
	};

////////////////////////////////////////////////////////////////////////////////////////////////

	struct coordinator_impl_t : public coordinator_t
	{
		coordinator_impl_t(pinba_globals_t *globals, coordinator_conf_t *conf)
			: globals_(globals)
			, stats_(globals->stats())
			, conf_(conf)
		{
			in_sock_ = nmsg_socket(AF_SP, NN_PULL);
			if (conf_->nn_input_buffer > 0)
				in_sock_.set_option(NN_SOL_SOCKET, NN_RCVBUF, conf_->nn_input_buffer * sizeof(packet_batch_ptr), conf_->nn_input);

			control_sock_ = nmsg_socket(AF_SP, NN_REP);
			control_sock_.bind(conf_->nn_control);
		}

		virtual void startup() override
		{
			in_sock_.connect(conf_->nn_input.c_str());

			std::thread t([this]()
			{
				this->worker_thread();
			});
			t_ = move(t);
		}

		virtual void shutdown() override
		{
			this->request(meow::make_intrusive<coordinator_request___shutdown_t>());
			t_.join();
		}

		virtual coordinator_response_ptr request(coordinator_request_ptr req) override
		{
			nmsg_socket_t sock;
			sock.open(AF_SP, NN_REQ).connect(conf_->nn_control);

			sock.send_message(req);
			return sock.recv<coordinator_response_ptr>();
		}

	private:

		coordinator_request_ptr control_recv()
		{
			return control_sock_.recv<coordinator_request_ptr>();
		}

		void control_send(coordinator_response_ptr response)
		{
			control_sock_.send_message(response);
		}

		template<class T, class... A>
		void control_send(A&&... args)
		{
			this->control_send(meow::make_intrusive<T>(std::forward<A>(args)...));
		}

		template<class... A>
		void control_send_generic(A&&... args)
		{
			this->control_send<coordinator_response___generic_t>(std::forward<A>(args)...);
		}

		void worker_thread()
		{
			std::string const thr_name = ff::fmt_str("coordinator");
			pthread_setname_np(pthread_self(), thr_name.c_str());

			MEOW_DEFER(
				LOG_DEBUG(globals_->logger(), "{0}; exiting", thr_name);
			);

			nmsg_poller_t poller;
			poller
				.ticker(1000 * d_millisecond, [this](timeval_t now)
				{
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

					// special counter for batches that were dropped, because no recepients were active
					if (report_hosts_.empty())
						++stats_->coordinator.batches_send_dropped;

					// relay the batch to all reports,
					// TODO(antoxa): maybe move this to a separate thread?
					// NOTE:
					// this is fundamentally broken and can't be implemented with nanomsg PUB/SUB sadly
					// since we have no idea if the report handler is slow, and in that case messages will be dropped
					// and ref counts will not be decremented and memory will leak and we won't have any stats about that either
					// (we also have no need for the pub/sub routing part at the moment and probably won't need it ever)
					for (auto& report_host : report_hosts_)
					{
						++stats_->coordinator.batch_send_total;
						bool const success = report_host.second->process_batch(batch);
						if (!success)
							++stats_->coordinator.batch_send_err;
					}
				})
				.read_nn_socket(control_sock_, [this, &poller](timeval_t now)
				{
					auto const req = this->control_recv();

					++stats_->coordinator.control_requests;

					try
					{
						switch (req->type)
						{
							case COORDINATOR_REQ__CALL:
							{
								auto *r = static_cast<coordinator_request___call_t*>(req.get());
								r->func(this);
								this->control_send_generic(COORDINATOR_STATUS__OK);
							}
							break;

							case COORDINATOR_REQ__SHUTDOWN:
							{
								auto *r = static_cast<coordinator_request___shutdown_t*>(req.get());

								// shutdown all reports
								for (auto& report_host : report_hosts_)
									report_host.second->shutdown();

								// stop poll loop and reply
								poller.set_shutdown_flag();
								this->control_send_generic(COORDINATOR_STATUS__OK);
							}
							break;

							case COORDINATOR_REQ__ADD_REPORT:
							{
								auto *r = static_cast<coordinator_request___add_report_t*>(req.get());

								LOG_DEBUG(globals_->logger(), "creating report {0}", r->report->name());

								auto const report_name = r->report->name().str();
								auto const thread_id   = report_hosts_.size();
								auto const thr_name    = ff::fmt_str("rh/{0}", thread_id);
								auto const rh_name     = ff::fmt_str("rh/{0}/{1}", thread_id, report_name);

								report_host_conf_t const rh_conf = {
									.name              = rh_name,
									.thread_name       = thr_name,
									.nn_reqrep         = ff::fmt_str("inproc://{0}/control", rh_name),
									.nn_shutdown       = ff::fmt_str("inproc://{0}/shutdown", rh_name),
									.nn_packets        = ff::fmt_str("inproc://{0}/packets", rh_name),
									.nn_packets_buffer = conf_->nn_report_input_buffer,
								};

								auto rh = meow::make_unique<report_host___new_thread_t>(globals_, rh_conf);
								rh->startup(r->report);

								report_hosts_.emplace(report_name, move(rh));

								this->control_send_generic(COORDINATOR_STATUS__OK);
							}
							break;

							case COORDINATOR_REQ__DELETE_REPORT:
							{
								auto *r = static_cast<coordinator_request___delete_report_t*>(req.get());

								auto const it = report_hosts_.find(r->report_name);
								if (it == report_hosts_.end())
								{
									this->control_send_generic(COORDINATOR_STATUS__ERROR, ff::fmt_str("unknown report: {0}", r->report_name));
									return;
								}

								report_host_t *host = it->second.get();
								host->shutdown(); // waits for host to completely shut itself down

								auto const n_erased = report_hosts_.erase(r->report_name);
								assert(n_erased == 1);

								this->control_send_generic(COORDINATOR_STATUS__OK);
							}
							break;

							case COORDINATOR_REQ__GET_REPORT_SNAPSHOT:
							{
								auto *r = static_cast<coordinator_request___get_report_snapshot_t*>(req.get());

								auto const it = report_hosts_.find(r->report_name);
								if (it == report_hosts_.end())
									throw std::runtime_error(ff::fmt_str("unknown report: {0}", r->report_name));

								report_host_t *host = it->second.get();

								report_snapshot_ptr snapshot;
								host->execute_in_thread([&](report_host_t *rhost)
								{
									snapshot = rhost->report()->get_snapshot();
								});

								auto response = meow::make_intrusive<coordinator_response___report_snapshot_t>();
								response->snapshot = move(snapshot);

								this->control_send(response);
							}
							break;

							case COORDINATOR_REQ__GET_REPORT_STATE:
							{
								auto *r = static_cast<coordinator_request___get_report_state_t*>(req.get());

								auto const it = report_hosts_.find(r->report_name);
								if (it == report_hosts_.end())
									throw std::runtime_error(ff::fmt_str("unknown report: {0}", r->report_name));

								report_host_t *host = it->second.get();

								auto state = meow::make_unique<report_state_t>();
								host->execute_in_thread([&](report_host_t *rhost)
								{
									state->stats     = rhost->stats();
									state->info      = rhost->report()->info();
									state->estimates = rhost->report()->get_estimates();
								});

								auto response = meow::make_intrusive<coordinator_response___report_state_t>();
								response->state = move(state);

								this->control_send(response);
							}
							break;

							default:
								throw std::runtime_error(ff::fmt_str("unknown coordinator_control_request type: {0}", req->type));
								break;
						}
					}
					catch (std::exception const& e)
					{
						this->control_send_generic(COORDINATOR_STATUS__ERROR, e.what());
					}
				})
				.loop();
		}

	private:
		pinba_globals_t     *globals_;
		pinba_stats_t       *stats_;
		coordinator_conf_t  *conf_;

		// report_name -> report_host
		using report_host_map_t = std::unordered_map<std::string, report_host_ptr>;
		report_host_map_t report_hosts_;

		nmsg_socket_t    in_sock_;
		nmsg_socket_t    control_sock_;

		std::thread      t_;
	};

////////////////////////////////////////////////////////////////////////////////////////////////
}} // namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

coordinator_ptr create_coordinator(pinba_globals_t *globals, coordinator_conf_t *conf)
{
	return meow::make_unique<aux::coordinator_impl_t>(globals, conf);
}

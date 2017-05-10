#include "pinba_config.h"

#include <thread>
#include <vector>

#include <meow/defer.hpp>
#include <meow/unix/resource.hpp> // getrusage_ex

#include "pinba/globals.h"
#include "pinba/dictionary.h"
#include "pinba/collector.h"
#include "pinba/repacker.h"
#include "pinba/packet.h"

#include "pinba/nmsg_socket.h"
#include "pinba/nmsg_poller.h"
#include "pinba/nmsg_ticker.h"

////////////////////////////////////////////////////////////////////////////////////////////////
namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////
#if 0
	struct repack_worker_t : private boost::noncopyable
	{
		pinba_globals_t   *globals_;
		repacker_conf_t   *conf_;
		pinba_stats_t     *stats_;
		repacker_stats_t  *stats_thread_;

		nmsg_socket_t     in_sock_;        // connect to this endpoint and recv raw_request_ptr-s
		nmsg_socket_t     shutdown_sock_;  // connect to this endpoint to recv shutdown notification

	public:

		repack_worker_t(pinba_globals_t *globals, repacker_conf_t *conf)
			: globals_(globals)
			, conf_(conf)
			, stats_(globals->stats())
			, stats_thread_(nullptr) // initialized below
		{
			in_sock_
				.open(AF_SP, NN_PULL)
				.connect(conf_->nn_input);

			shutdown_sock_
				.open(AF_SP, NN_REP)
				.connect(conf_->nn_shutdown);

			stats_->repacker_threads.emplace_back();
			stats_thread_ = &stats_->repacker_threads.back();
		}

		void run(nmsg_socket_t& out_sock)
		{
			std::thread t([this, &out_sock]()
			{
				std::string const thr_name = ff::fmt_str("repacker/{0}", thread_id);
				pthread_setname_np(pthread_self(), thr_name.c_str());

				MEOW_DEFER(
					LOG_DEBUG(globals_->logger(), "{0}; exiting", thr_name);
				);

				this->thread_main(thr_name, out_sock);
			});
		}

		void shutdown()
		{

		}

	public:

		void thread_main(std::string const& thr_name, nmsg_socket_t& out_sock)
		{

			// batch state
			auto const create_batch = [this]()
			{
				constexpr size_t nmpa_block_size = 64 * 1024;
				return meow::make_intrusive<packet_batch_t>(conf_->batch_size, nmpa_block_size);
			};

			auto const try_send_batch = [this](packet_batch_ptr& b)
			{
				++stats_->repacker.batch_send_total;
				out_sock_.send_message(b);
			};

			packet_batch_ptr batch = create_batch();
			dictionary_t *dictionary = globals_->dictionary();

			// processing loop

			nmsg_poller_t poller;

			// extra stats
			poller.before_poll([this](timeval_t now, duration_t wait_for)
			{
				// TODO: observing a lil more poll() calls than really needed
				// but to poll() low-ish time precision and maybe debug build
				// LOG_DEBUG(globals_->logger(), "before_poll; now: {0}, wait_for: {1}", now, wait_for);
				++stats_->repacker.poll_total;
			});

			// resetable periodic event, to 'idly' send batch at regular intervals
			auto batch_send_tick = poller.ticker_with_reset(conf_->batch_timeout, [&](timeval_t now)
			{
				if (!batch || batch->packet_count == 0)
					return;

				++stats_->repacker.batch_send_by_timer;

				try_send_batch(batch);
				batch = create_batch();
			});

			// periodically get rusage
			poller.ticker(1 * d_second, [this, thread_id](timeval_t now)
			{
				os_rusage_t const ru = os_unix::getrusage_ex(RUSAGE_THREAD);

				std::lock_guard<std::mutex> lk_(stats_->mtx);
				stats_thread_->ru_utime = timeval_from_os_timeval(ru.ru_utime);
				stats_thread_->ru_stime = timeval_from_os_timeval(ru.ru_stime);
			});

			// shutdown
			poller.read_nn_socket(shutdown_sock_, [this, &poller, &thr_name](timeval_t now)
			{
				LOG_DEBUG(globals_->logger(), "{0}; got shutdown signal", thr_name);
				poller.set_shutdown_flag();
			});

			// process incoming packets
			poller.read_nn_socket(in_sock_, [&](timeval_t now)
			{
				// receive in a loop with NN_DONTWAIT since we'd prefer
				// to process message ASAP and create batches by size limit (and save on polling / getting current time)
				while (true)
				{
					++stats_->repacker.recv_total;

					auto const req = in_sock_.recv<raw_request_ptr>(thr_name, NN_DONTWAIT);
					if (!req) { // EAGAIN
						++stats_->repacker.recv_eagain;
						break;
					}

					for (uint32_t i = 0; i < req->request_count; i++)
					{
						auto const *pb_req = req->requests[i];

						auto const vr = pinba_validate_request(pb_req);
						if (vr != request_validate_result::okay)
						{
							LOG_DEBUG(globals_->logger(), "request validation failed: {0}: {1}\n", vr, enum_as_str_ref(vr));
							continue;
						}

						packet_t *packet = pinba_request_to_packet(pb_req, dictionary, &batch->nmpa);

						++stats_->repacker.packets_processed;

						// append to current batch
						batch->packets[batch->packet_count] = packet;
						batch->packet_count++;

						if (batch->packet_count >= conf_->batch_size)
						{
							++stats_->repacker.batch_send_by_size;

							try_send_batch(batch);
							batch = create_batch();

							// reset idle batch send interval
							// to keep batch send ticker *interval* intact
							poller.reset_ticker(batch_send_tick, now);

							// keep batch send ticker *interval* intact
							//  but accept some time skew (due to time measurement taking some time by itself)
							// next_tick_tv = os_unix::clock_monotonic_now() + conf_->batch_timeout;
						}
					}
				}
			});

			poller.loop();
		}
	};
#endif
////////////////////////////////////////////////////////////////////////////////////////////////

	struct repacker_impl_t : public repacker_t
	{
	public:

		repacker_impl_t(pinba_globals_t *globals, repacker_conf_t *conf)
			: globals_(globals)
			, stats_(globals->stats())
			, conf_(conf)
		{
		}

		virtual void startup() override
		{
			out_sock_
				.open(AF_SP, NN_PUSH)
				.bind(conf_->nn_output);

			shutdown_sock_
				.open(AF_SP, NN_PULL)
				.bind(conf_->nn_shutdown);

			for (uint32_t i = 0; i < conf_->n_threads; i++)
			{
				stats_->repacker_threads.push_back({});

				// open and connect to producer in main thread, to make exceptions catch-able easily
				nmsg_socket_t in_sock;
				in_sock.open(AF_SP, NN_PULL);
				in_sock.connect(conf_->nn_input.c_str());

				if (conf_->nn_input_buffer > 0)
					in_sock.set_option(NN_SOL_SOCKET, NN_RCVBUF, sizeof(raw_request_t) * conf_->nn_input_buffer, conf_->nn_input);

				// start worker threads
				// since we're targeting c++11 - can't use lambda capture like 'in_sock = move(in_sock)' here :(
				int const sock_fd = in_sock.release();
				std::thread t([this, i, sock_fd]()
				{
					nmsg_socket_t sock(sock_fd);
					this->worker_thread(i, sock);
				});

				// t.detach();
				threads_.push_back(move(t));
			}
		}

		virtual void shutdown() override
		{
			nmsg_socket_t sock;
			sock.open(AF_SP, NN_PUSH).connect(conf_->nn_shutdown);
			sock.send(1); // there is no need to send multiple times, threads exit on poll signal

			for (uint32_t i = 0; i < conf_->n_threads; i++)
			{
				threads_[i].join();
			}
		}

	private:

		void worker_thread(uint32_t thread_id, nmsg_socket_t& in_sock)
		{
			std::string const thr_name = ff::fmt_str("repacker/{0}", thread_id);

		#ifdef PINBA_HAVE_PTHREAD_SETNAME_NP
			pthread_setname_np(pthread_self(), thr_name.c_str());
		#endif

			MEOW_DEFER(
				LOG_DEBUG(globals_->logger(), "{0}; exiting", thr_name);
			);

			// batch state
			auto const create_batch = [this]()
			{
				constexpr size_t nmpa_block_size = 64 * 1024;
				return meow::make_intrusive<packet_batch_t>(conf_->batch_size, nmpa_block_size);
			};

			auto const try_send_batch = [this](packet_batch_ptr& b)
			{
				++stats_->repacker.batch_send_total;
				out_sock_.send_message(b);
			};

			packet_batch_ptr batch = create_batch();
			dictionary_t *dictionary = globals_->dictionary();
			repacker_dictionary_t r_dictionary { globals_->dictionary() };

			// processing loop
			nmsg_poller_t poller;

			// extra stats
			poller.before_poll([this](timeval_t now, duration_t wait_for)
			{
				// TODO: observing a lil more poll() calls than really needed
				// but to poll() low-ish time precision and maybe debug build
				// LOG_DEBUG(globals_->logger(), "before_poll; now: {0}, wait_for: {1}", now, wait_for);
				++stats_->repacker.poll_total;
			});

			// resetable periodic event, to 'idly' send batch at regular intervals
			auto batch_send_tick = poller.ticker_with_reset(conf_->batch_timeout, [&](timeval_t now)
			{
				if (!batch || batch->packet_count == 0)
					return;

				++stats_->repacker.batch_send_by_timer;

				try_send_batch(batch);
				batch = create_batch();
			});

			// periodically get rusage
			poller.ticker(1 * d_second, [this, thread_id](timeval_t now)
			{
				os_rusage_t const ru = os_unix::getrusage_ex(RUSAGE_THREAD);

				std::lock_guard<std::mutex> lk_(stats_->mtx);
				stats_->repacker_threads[thread_id].ru_utime = timeval_from_os_timeval(ru.ru_utime);
				stats_->repacker_threads[thread_id].ru_stime = timeval_from_os_timeval(ru.ru_stime);
			});

			// shutdown
			poller.read_nn_socket(shutdown_sock_, [this, &poller, &thr_name](timeval_t now)
			{
				LOG_DEBUG(globals_->logger(), "{0}; got shutdown signal", thr_name);
				poller.set_shutdown_flag();
			});

			// process incoming packets
			poller.read_nn_socket(in_sock, [&](timeval_t now)
			{
				// receive in a loop with NN_DONTWAIT since we'd prefer
				// to process message ASAP and create batches by size limit (and save on polling / getting current time)
				while (true)
				{
					++stats_->repacker.recv_total;

					auto const req = in_sock.recv<raw_request_ptr>(thr_name, NN_DONTWAIT);
					if (!req) { // EAGAIN
						++stats_->repacker.recv_eagain;
						break;
					}

					for (uint32_t i = 0; i < req->request_count; i++)
					{
						auto const *pb_req = req->requests[i];

						++stats_->repacker.recv_packets;

						auto const vr = pinba_validate_request(pb_req);
						if (vr != request_validate_result::okay)
						{
							++stats_->repacker.packet_validate_err;
							LOG_DEBUG(globals_->logger(), "request validation failed: {0}: {1}", vr, enum_as_str_ref(vr));
							continue;
						}

						// packet_t *packet = pinba_request_to_packet(pb_req, dictionary, &batch->nmpa);
						packet_t *packet = pinba_request_to_packet(pb_req, &r_dictionary, &batch->nmpa);

						// append to current batch
						batch->packets[batch->packet_count] = packet;
						batch->packet_count++;

						if (batch->packet_count >= conf_->batch_size)
						{
							++stats_->repacker.batch_send_by_size;

							try_send_batch(batch);
							batch = create_batch();

							// reset idle batch send interval
							// to keep batch send ticker *interval* intact
							poller.reset_ticker(batch_send_tick, now);
						}
					}
				}
			});

			poller.loop();

			// thread exits here
		}

	private:
		nmsg_socket_t    out_sock_;
		nmsg_socket_t    shutdown_sock_;

		pinba_globals_t  *globals_;
		pinba_stats_t    *stats_;
		repacker_conf_t  *conf_;

		std::vector<std::thread> threads_;
	};

////////////////////////////////////////////////////////////////////////////////////////////////
}} // namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

repacker_ptr create_repacker(pinba_globals_t *globals, repacker_conf_t *conf)
{
	return meow::make_unique<aux::repacker_impl_t>(globals, conf);
}

#include "pinba_config.h"

#include <thread>
// #include <vector>

#include <tsl/robin_set.h>

#include <meow/defer.hpp>
#include <meow/stopwatch.hpp>
#include <meow/unix/resource.hpp> // getrusage_ex

#include "pinba/globals.h"
#include "pinba/os_symbols.h"
#include "pinba/dictionary.h"
#include "pinba/repacker_dictionary.h"
#include "pinba/collector.h"
#include "pinba/repacker.h"
#include "pinba/packet.h"
#include "pinba/packet_impl.h"

#include "pinba/nmsg_socket.h"
#include "pinba/nmsg_poller.h"

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

	struct repacker_state_impl_t : public repacker_state_t
	{
		// std::vector<repacker_dslice_ptr> dict_slices;

		struct hash_function_t
		{
			inline size_t operator()(repacker_dslice_ptr const& p) const
			{
				return (size_t)p.get();
			}
		};

		using hashtable_t = tsl::robin_set<
								  repacker_dslice_ptr
								, hash_function_t
								, std::equal_to<repacker_dslice_ptr>
								, std::allocator<repacker_dslice_ptr>
								, /*StoreHash=*/false
								, tsl::rh::prime_growth_policy>;

		hashtable_t ht;

	public:

		repacker_state_impl_t()
		{
		}

		repacker_state_impl_t(repacker_dslice_ptr ds)
		{
			// dict_slices.emplace_back(std::move(ds));
			ht.emplace(std::move(ds));
		}

		virtual repacker_state_ptr clone() override
		{
			auto result = std::make_shared<repacker_state_impl_t>();
			result->ht = ht;
			// result->dict_slices = dict_slices;
			return result;
		}

		virtual void merge_other(repacker_state_t& other_ref) override
		{
			auto& other = static_cast<repacker_state_impl_t&>(other_ref);

			ht.insert(other.ht.begin(), other.ht.end());

			// for (auto& other_ds : other.dict_slices)
			// {
			// 	auto const it = std::find(dict_slices.begin(), dict_slices.end(), other_ds);
			// 	if (it == dict_slices.end())
			// 	{
			// 		dict_slices.emplace_back(other_ds);
			// 	}
			// }
		}
	};

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

		~repacker_impl_t()
		{
			this->shutdown();
		}

		virtual void startup() override
		{
			out_sock_
				.open(AF_SP, NN_PUSH)
				.bind(conf_->nn_output);

			shutdown_sock_
				.open(AF_SP, NN_PULL)
				.bind(conf_->nn_shutdown);

			shutdown_cli_sock_
				.open(AF_SP, NN_PUSH)
				.connect(conf_->nn_shutdown);


			stats_->repacker_threads.resize(conf_->n_threads);

			for (uint32_t i = 0; i < conf_->n_threads; i++)
			{
				// open and connect to producer in main thread, to make exceptions catch-able easily
				nmsg_socket_t input_sock;
				input_sock
					.open(AF_SP, NN_PULL)
					.connect(conf_->nn_input.c_str());

				if (conf_->nn_input_buffer > 0)
					input_sock.set_option(NN_SOL_SOCKET, NN_RCVBUF, sizeof(raw_request_t) * conf_->nn_input_buffer, conf_->nn_input);

				// start worker threads
				std::thread t([this, i, input_sock = std::move(input_sock)]() mutable
				{
					this->worker_thread(i, input_sock);
				});

				// t.detach();
				threads_.push_back(std::move(t));
			}
		}

		virtual void shutdown() override
		{
			if (threads_.empty())
				return;

			{
				std::unique_lock<std::mutex> lk_(shutdown_mtx_);
				shutdown_cli_sock_.send(1); // there is no need to send multiple times, threads exit on poll signal
			}

			for (auto&& thr : threads_)
			{
				thr.join();
			}

			threads_.clear();
		}

	private:

		void worker_thread(uint32_t thread_id, nmsg_socket_t& input_sock)
		{
			std::string const thr_name = ff::fmt_str("repacker/{0}", thread_id);

			PINBA___OS_CALL(globals_, set_thread_name, thr_name);

			MEOW_DEFER(
				LOG_DEBUG(globals_->logger(), "{0}; exiting", thr_name);
			);

			// thread-local cache for global shared dictionary
			repacker_dictionary_t r_dictionary { globals_->dictionary() };

			// thread-local pointer to the global nameword dictionary
			// periodically reloaded in RCU style
			nameword_dictionary_ptr nw_dictionary { globals_->dictionary()->load_nameword_dict() };

			// batch state
			auto const create_batch = [&]()
			{
				constexpr size_t nmpa_block_size = 64 * 1024;
				auto batch = meow::make_intrusive<packet_batch_t>(conf_->batch_size, nmpa_block_size);
				batch->repacker_state = std::make_shared<repacker_state_impl_t>(r_dictionary.current_wordslice());
				return batch;
			};

			auto const try_send_batch = [&](packet_batch_ptr& batch)
			{
				r_dictionary.start_new_wordslice(); // make sure batch has only one wordslice

				++stats_->repacker.batch_send_total;
				out_sock_.send_message(batch);
			};

			packet_batch_ptr batch = create_batch();

			// processing loop
			nmsg_poller_t poller;

			// extra stats
			poller.before_poll([this](timeval_t now, duration_t wait_for)
			{
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

			// periodically re-load nameword dictionary, to see what's updated
			// this is basically a poor man's RCU scheme (dict is fully copied + appended to on insert)
			poller.ticker(1 * d_second, [&](timeval_t now)
			{
				nw_dictionary = globals_->dictionary()->load_nameword_dict();
				// LOG_DEBUG(globals_->logger(), "{0}; reloaded nw_dictionary: {1}", thr_name, nw_dictionary.get());
			});

			// reap old dictionary wordslices periodically
			// 250ms is hand-tuned with a synthetic test at ~400k random 32byte strings/sec
			// might be made tunable, but no need for now
			poller.ticker(250 * d_millisecond, [&](timeval_t now)
			{
				meow::stopwatch_t sw;

				auto const reap_stats = r_dictionary.reap_unused_wordslices();

				// LOG_DEBUG(globals_->logger(),
				// 	"{0}; reaping old dictionary wordslices; time: {1}, slices: {2}, words_local: {3}, words_global: {4}",
				// 	thr_name, sw.stamp(), reap_stats.reaped_slices, reap_stats.reaped_words_local, reap_stats.reaped_words_global);
			});

			// shutdown
			poller.read_nn_socket(shutdown_sock_, [this, &poller, &thr_name](timeval_t now)
			{
				LOG_DEBUG(globals_->logger(), "{0}; got shutdown signal", thr_name);
				poller.set_shutdown_flag();
			});

			// process incoming packets
			poller.read_nn_socket(input_sock, [&](timeval_t now)
			{
				constexpr size_t const max_batches_per_poll_iteration = 4;

				for (size_t i = 0; i < max_batches_per_poll_iteration; ++i)
				{
					++stats_->repacker.recv_total;

					// receive in a loop with NN_DONTWAIT to avoid hanging here when we're out of incoming data
					auto const req = input_sock.recv<raw_request_ptr>(thr_name, NN_DONTWAIT);
					if (!req) { // EAGAIN
						++stats_->repacker.recv_eagain;
						break;
					}

					for (uint32_t i = 0; i < req->request_count; i++)
					{
						++stats_->repacker.recv_packets;

						 // non-const, since pinba_validate_request() might change the packet
						auto *pb_req = req->requests[i];

						// validation should not fail, generally.
						// pinba is expected to be mostly receiving traffic from trusted sources (your code, mon!)
						auto const vr = pinba_validate_request(pb_req);
						if (vr != request_validate_result::okay)
						{
							++stats_->repacker.packet_validate_err;
							LOG_DEBUG(globals_->logger(), "request validation failed: {0}: {1}", vr, enum_as_str_ref(vr));
							continue;
						}

						packet_t *packet = pinba_request_to_packet(pb_req, nw_dictionary.get(), &r_dictionary, &batch->nmpa);

						if (globals_->options()->packet_debug)
						{
							static double curr_fraction = 1.0; // to start dumping immediately

							if (curr_fraction >= 1.0)
							{
								auto sink = meow::logging::logger_as_sink(*globals_->logger(), meow::logging::log_level::info, meow::line_mode::prefix);
								debug_dump_packet(sink, packet, globals_->dictionary(), &batch->nmpa);

								curr_fraction = globals_->options()->packet_debug_fraction;
							}
							else
							{
								curr_fraction += globals_->options()->packet_debug_fraction;
							}
						}

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
		nmsg_socket_t    shutdown_cli_sock_;
		std::mutex       shutdown_mtx_;

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

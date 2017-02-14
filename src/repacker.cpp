#include <thread>
#include <vector>

#include "pinba/globals.h"
#include "pinba/dictionary.h"
#include "pinba/collector.h"
#include "pinba/repacker.h"
#include "pinba/packet.h"

#include "pinba/nmsg_socket.h"
#include "pinba/nmsg_poller.h"

////////////////////////////////////////////////////////////////////////////////////////////////
namespace { namespace aux {
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

		void startup()
		{
			out_sock_
				.open(AF_SP, NN_PUSH)
				.bind(conf_->nn_output.c_str());

			for (uint32_t i = 0; i < conf_->n_threads; i++)
			{
				// open and connect to producer in main thread, to make exceptions catch-able easily
				nmsg_socket_t in_sock;
				in_sock.open(AF_SP, NN_PULL);
				in_sock.connect(conf_->nn_input.c_str());

				if (conf_->nn_input_buffer > 0)
					in_sock.set_option(NN_SOL_SOCKET, NN_RCVBUF, sizeof(raw_request_t) * conf_->nn_input_buffer, conf_->nn_input);

				// start detached worker thread
				// since we're targeting c++11 - can't use lambda capture like 'in_sock = move(in_sock)' here :(
				int const sock_fd = in_sock.release();
				std::thread t([this, i, sock_fd]()
				{
					this->worker_thread(i, nmsg_socket_t(sock_fd));
				});

				t.detach();
				threads_.push_back(move(t));
			}
		}

	private:

		void worker_thread(uint32_t thread_id, nmsg_socket_t in_sock)
		{
			{
				std::string const thr_name = ff::fmt_str("repacker/{0}", thread_id);
				pthread_setname_np(pthread_self(), thr_name.c_str());
			}

			auto const create_batch = [&]()
			{
				return packet_batch_ptr { new packet_batch_t(conf_->batch_size, 64 * 1024) };
			};

			auto const try_send_batch = [&](packet_batch_ptr& b)
			{
				out_sock_.send_message(b);
			};

			packet_batch_ptr batch = create_batch();
			timeval_t next_tick_tv = os_unix::clock_monotonic_now() + conf_->batch_timeout;

			// dictionary_t thread_local_dict;
			dictionary_t *dictionary = globals_->dictionary(); // &thread_local_dict;

			struct nn_pollfd pfd[] = {
				{ .fd = *in_sock, .events = NN_POLLIN, .revents = 0, },
			};
			size_t const pfd_size = sizeof(pfd) / sizeof(pfd[0]);

			while (true)
			{
				int const wait_for_ms = duration_from_timeval(next_tick_tv - os_unix::clock_monotonic_now()).nsec / d_millisecond.nsec;

				++stats_->repacker.poll_total;
				int r = nn_poll(pfd, pfd_size, wait_for_ms);

				if (r < 0)
				{
					ff::fmt(stderr, "nn_poll() failed: {0}:{1}\n", nn_errno(), nn_strerror(nn_errno()));
					break;
				}

				if (r == 0) // timeout
				{
					try_send_batch(batch);
					batch = create_batch();

					// TODO: maybe add tick skipping awareness here,
					// i.e. handle when we took so long to process packets, time is already past next (or multiple) ticks

					// keep tick interval and avoid time skew
					next_tick_tv += conf_->batch_timeout;
					continue;
				}

				// receive in a loop with NN_DONTWAIT since we'd prefer
				// to process message ASAP and create batches by size limit (and save on polling / getting current time)
				while (true)
				{
					++stats_->repacker.recv_total;

					auto const req = in_sock.recv<raw_request_ptr>(NN_DONTWAIT);
					if (!req) { // EAGAIN
						++stats_->repacker.recv_eagain;
						break; // next iteration of outer loop
					}

					for (uint32_t i = 0; i < req->request_count; i++)
					{
						auto const pb_req = req->requests[i];

						auto const vr = pinba_validate_request(pb_req);
						if (vr != request_validate_result::okay)
						{
							ff::fmt(stderr, "request validation failed: {0}: {1}\n", vr, enum_as_str_ref(vr));
							continue;
						}

						packet_t *packet = pinba_request_to_packet(pb_req, dictionary, &batch->nmpa);

						++stats_->repacker.packets_processed;

						// append to current batch
						batch->packets[batch->packet_count] = packet;
						batch->packet_count++;

						if (batch->packet_count >= conf_->batch_size)
						{
							// ff::fmt(stderr, "sending batch due to size limit {0}, {1}\n", batch->packet_count, conf_->batch_size);

							try_send_batch(batch);
							batch = create_batch();

							// keep batch send ticker *interval* intact
							//  but accept some time skew (due to time measurement taking some time by itself)
							next_tick_tv = os_unix::clock_monotonic_now() + conf_->batch_timeout;
						}
					}
				}
			}
		}

	private:
		nmsg_socket_t    out_sock_;

		pinba_globals_t  *globals_;
		pinba_stats_t    *stats_;
		repacker_conf_t  *conf_;

		dictionary_t     *dictionary_;

		std::vector<std::thread> threads_;
	};

////////////////////////////////////////////////////////////////////////////////////////////////
}} // namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

repacker_ptr create_repacker(pinba_globals_t *globals, repacker_conf_t *conf)
{
	return meow::make_unique<aux::repacker_impl_t>(globals, conf);
}

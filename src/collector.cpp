#include "auto_config.h"

#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h> // setsockopt

#include <stdexcept>
#include <thread>
#include <vector>

#include <nanomsg/nn.h>
#include <nanomsg/pipeline.h>

#include <meow/unix/fd_handle.hpp>
#include <meow/unix/socket.hpp>
#include <meow/unix/netdb.hpp>
#include <meow/format/format.hpp>

#include "pinba/collector.h"
#include "proto/pinba.pb-c.h"

#include "misc/nmpa.h"
#include "misc/nmpa_pba.h"

////////////////////////////////////////////////////////////////////////////////////////////////

namespace ff = meow::format;

////////////////////////////////////////////////////////////////////////////////////////////////
namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

	struct collector_impl_t : public collector_t
	{
		collector_impl_t(pinba_globals_t *globals, collector_conf_t *conf)
			: globals_(globals)
			, stats_(globals->stats())
			, conf_(conf)
		{
			if (conf_->n_threads == 0 || conf_->n_threads > 1024)
				throw std::runtime_error(ff::fmt_str("collector_conf_t::n_threads must be within [1, 1023]"));

			out_sock_
				.open(AF_SP, NN_PUSH)
				.bind(conf_->nn_output);

			this->try_bind();
		}

		virtual void startup() override
		{
			if (!threads_.empty())
				throw std::logic_error("collector_t::startup(): already started");

			for (uint32_t i = 0; i < conf_->n_threads; i++)
			{
				std::thread t([this, i]()
				{
					std::string const thr_name = ff::fmt_str("udp_reader/{0}", i);
					pthread_setname_np(pthread_self(), thr_name.c_str());

					this->eat_udp(i);
				});

				t.detach();
				threads_.push_back(move(t));
			}
		}

		virtual str_ref nn_output() override
		{
			return conf_->nn_output;
		}

	private:

		void try_bind()
		{
			os_addrinfo_list_ptr ai_list = os_unix::getaddrinfo_ex(conf_->address.c_str(), conf_->port.c_str(), AF_INET, SOCK_DGRAM, 0);
			os_addrinfo_t *ai = ai_list.get(); // take 1st item for now

			os_unix::fd_handle_t fd { os_unix::socket_ex(ai->ai_family, ai->ai_socktype, ai->ai_protocol) };
			os_unix::setsockopt_ex(fd.get(), SOL_SOCKET, SO_REUSEADDR, 1);
			os_unix::bind_ex(fd.get(), ai->ai_addr, ai->ai_addrlen);

			// commit if everything is ok
			fd_ = fd.release();
		}

		void send_current_batch(uint32_t thread_id, raw_request_ptr& req)
		{
			bool const success = out_sock_.send_message(req, NN_DONTWAIT);
			if (!success)
			{
				int const err = EAGAIN;
				ff::fmt(stderr, "nn_send(eat_udp:{0}) failed: {1}: {2}\n", thread_id, err, nn_strerror(err));
			}

			req.reset(); // signal the need to reinit
		}

		void eat_udp(uint32_t const thread_id)
		{
			// this->eat_udp_recv(thread_id);
			this->eat_udp_recvmmsg(thread_id);
		}

		void eat_udp_recv(uint32_t const thread_id)
		{
			static constexpr size_t const read_buffer_size = 64 * 1024; // max udp message size
			char buf[read_buffer_size];

			raw_request_ptr req;

			ProtobufCAllocator request_unpack_pba = {
				.alloc = nmpa___pba_alloc,
				.free = nmpa___pba_free,
				.allocator_data = NULL, // changed in progress
			};

			while (true)
			{
				// if we have a batch semi-filled -> be nonblocking to avoid hanging onto it for too long
				// EAGAIN handling will send the batch and reset the pointer, so we'll be blocking afterwards
				int const recv_flags = (req) ? MSG_DONTWAIT : 0;

				++stats_->udp.recv_total;

				if (recv_flags == MSG_DONTWAIT)
					++stats_->udp.recv_nonblocking;

				int const n = recv(fd_, buf, sizeof(buf), recv_flags);
				if (n > 0)
				{
					++stats_->udp.packets_received;

					if (!req)
					{
						req.reset(new raw_request_t(conf_->batch_size, 16 * 1024));
						request_unpack_pba.allocator_data = &req->nmpa;
					}


					Pinba__Request *request = pinba__request__unpack(&request_unpack_pba, n, (uint8_t*)buf);
					if (request == NULL) {
						ff::fmt(stderr, "packet decode failed\n");
						continue;
					}

					req->requests[req->request_count] = request;
					req->request_count++;

					if (req->request_count >= conf_->batch_size)
					{
						this->send_current_batch(thread_id, req);
					}

					continue;
				}

				if (n < 0) {
					if (errno == EINTR) {
						continue;
					}

					if (errno == EAGAIN)
					{
						++stats_->udp.recv_eagain;

						// need to send current batch if we've got anything
						if (req && req->request_count > 0)
						{
							this->send_current_batch(thread_id, req);
						}
						continue;
					}

					ff::fmt(stderr, "recv failed: {0}:{1}\n", errno, strerror(errno));
					continue;
				}

				if (n == 0) {
					return;
				}
			}
		}

		void eat_udp_recvmmsg(uint32_t const thread_id)
		{
			size_t const max_message_size   = 64 * 1024; // max udp message size
			size_t const max_dgrams_to_recv = conf_->batch_size; // FIXME: make a special setting for this

			auto *hdr = (struct mmsghdr*)calloc(max_dgrams_to_recv, sizeof(struct mmsghdr));
			auto *iov = (struct iovec*)calloc(max_dgrams_to_recv, sizeof(struct iovec));

			for (unsigned i = 0; i < max_dgrams_to_recv; i++)
			{
				iov[i].iov_base           = malloc(max_message_size);
				iov[i].iov_len            = max_message_size;

				hdr[i].msg_hdr.msg_iov    = &iov[i];
				hdr[i].msg_hdr.msg_iovlen = 1;
			}

			raw_request_ptr req;

			ProtobufCAllocator request_unpack_pba = {
				.alloc = nmpa___pba_alloc,
				.free = nmpa___pba_free,
				.allocator_data = NULL, // changed in progress
			};

			auto const recv_loop = [&]()
			{
				while (true)
				{
					++stats_->udp.recv_total;
					++stats_->udp.recv_nonblocking;

					int const n = recvmmsg(fd_, hdr, max_dgrams_to_recv, 0, NULL);
					if (n > 0)
					{
						for (int i = 0; i < n; i++)
						{
							++stats_->udp.packets_received;

							if (!req)
							{
								req.reset(new raw_request_t(conf_->batch_size, 16 * 1024));
								request_unpack_pba.allocator_data = &req->nmpa;
							}

							str_ref const dgram = { (char*)iov[i].iov_base, (size_t)hdr[i].msg_len };

							Pinba__Request *request = pinba__request__unpack(&request_unpack_pba, dgram.c_length(), (uint8_t*)dgram.data());
							if (request == NULL) {
								ff::fmt(stderr, "packet decode failed\n");
								return true;
							}

							req->requests[req->request_count] = request;
							req->request_count++;

							if (req->request_count >= conf_->batch_size)
							{
								this->send_current_batch(thread_id, req);
							}
						}

						continue;
					}

					if (n < 0)
					{
						if (errno == EINTR) {
							return true;
						}

						if (errno == EAGAIN)
						{
							++stats_->udp.recv_eagain;

							// need to send current batch if we've got anything
							if (req && req->request_count > 0)
							{
								this->send_current_batch(thread_id, req);
							}
							return true;
						}

						ff::fmt(stderr, "recv failed: {0}:{1}\n", errno, strerror(errno));
						return false;
					}

					if (n == 0) {
						return false;
					}
				} // recv loop
			};

			constexpr struct timespec const sleep_for = {
				.tv_sec = 0,
				.tv_nsec = 1 * 1000 * 1000,
			};

			while (true)
			{
				nanosleep(&sleep_for, NULL);

				bool const should_continue = recv_loop();
				if (!should_continue)
					return;
			} // sleep loop
		}

	private:
		int               fd_;
		nmsg_socket_t     out_sock_;

		pinba_globals_t   *globals_;
		pinba_stats_t     *stats_;
		collector_conf_t  *conf_;

		std::vector<std::thread> threads_;
	};

////////////////////////////////////////////////////////////////////////////////////////////////
}} // namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

collector_ptr create_collector(pinba_globals_t *globals, collector_conf_t *conf)
{
	return meow::make_unique<aux::collector_impl_t>(globals, conf);
}

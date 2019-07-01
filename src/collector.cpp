#include "pinba_config.h"

#include <fcntl.h>
#include <sys/types.h>
#include <sys/socket.h> // setsockopt

#include <stdexcept>
#include <thread>
#include <vector>

#include <nanomsg/nn.h>
#include <nanomsg/pipeline.h>

#include <meow/defer.hpp>
#include <meow/format/format.hpp>
#include <meow/unix/fd_handle.hpp>
#include <meow/unix/socket.hpp>
#include <meow/unix/netdb.hpp>
#include <meow/unix/resource.hpp>

#include "pinba/globals.h"
#include "pinba/os_symbols.h"
#include "pinba/collector.h"
#include "pinba/nmsg_socket.h"
#include "pinba/nmsg_poller.h"

#include "proto/pinba.pb-c.h"

#include "misc/nmpa.h"
#include "misc/nmpa_pba.h"

#ifdef PINBA_HAVE_LZ4
#include <lz4.h>
#endif

////////////////////////////////////////////////////////////////////////////////////////////////

namespace ff = meow::format;

////////////////////////////////////////////////////////////////////////////////////////////////
namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

	struct fd_handle_t
	{
		int fd_;

		fd_handle_t()
			: fd_(-1)
		{
		}

		explicit fd_handle_t(int fd)
			: fd_(fd)
		{
		}

		fd_handle_t(fd_handle_t&& other)
			: fd_(-1)
		{
			std::swap(fd_, other.fd_);
		}

		void operator=(fd_handle_t&& other)
		{
			reset();
			std::swap(fd_, other.fd_);
		}

		int operator*() const
		{
			assert(fd_ >= 0);
			return fd_;
		}

		int release()
		{
			int tmp_fd = fd_;
			fd_ = -1;
			return tmp_fd;
		}

		void reset()
		{
			while (fd_ >= 0)
			{
				int const r = close(fd_);
				if (0 == r)
				{
					fd_ = -1;
					break;
				}

				assert(r < 0);
				if (errno == EINTR)
					continue;

				// silently swallow the error otherwise, leaking the fd
				break;
			}
		}

		~fd_handle_t()
		{
			reset();
		}
	};

////////////////////////////////////////////////////////////////////////////////////////////////

	net_datagram_t parse_network_datagram(str_ref bytes)
	{
		net_datagram_t dgram = {};

		// v0 - is just raw protobuf
		// v1 - adds 4 byte header
		//      header format <version:4><flags:12><original_data_len:16>
		//
		// in v0 - high 4 bits are most probably 0, use that to distinguish between them

		assert(bytes.size() > 0 && "impossible to have empty datagram");
		uint8_t const v = (bytes[0] >> 4);

		// guessed v1 and sufficient length
		if ((v == 1) && (bytes.size() >= 4))
		{
			uint16_t const data_len = uint16_t(bytes[2]) | bytes[3];

			return net_datagram_t {
				.version = 1,
				.flags   = uint32_t(bytes[0] & 0x0f) | bytes[1],
				.data    = str_ref { bytes.begin() + 4, bytes.end() },
			};
		}

		// otherwise, let's try with v0 format
		return net_datagram_t {
			.version = 0,
			.flags   = 0,
			.data    = bytes,
		};
	}

	// decompress_network_datagram, decompresses `dgram->data` into `dst_buf`
	//  returns
	//    - true on success and modifies `dgram->data` to point to the relevant part of `dst_buf`
	//    - false on failure
	bool decompress_network_datagram(net_datagram_t *dgram, char *dst_buf, int dst_capacity)
	{
		assert(dgram != nullptr);

#if PINBA_HAVE_LZ4
		int const decompressed_size = LZ4_decompress_safe(dgram->data.data(), dst_buf, dgram->data.c_length(), dst_capacity);

		// error, no details from lz4
		// can be either
		// - malformed input (treat zero size input is malformed as well)
		// - insufficient buffer space
		if (decompressed_size <= 0)
			return false;

		dgram->data = str_ref { dst_buf, size_t(decompressed_size) };
		return true;
#else
		// not implemented, or not enabled - failure
		return false;
#endif
	}

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

			shutdown_sock_
				.open(AF_SP, NN_PULL)
				.bind(conf_->nn_shutdown);

			shutdown_cli_sock_
				.open(AF_SP, NN_PUSH)
				.connect(conf_->nn_shutdown);

			this->try_resolve_listen_addr_port();
		}

		~collector_impl_t()
		{
			this->shutdown();
		}

		virtual void startup() override
		{
			if (!threads_.empty())
				throw std::logic_error("collector_t::startup(): already started");

			stats_->collector_threads.resize(conf_->n_threads);

			for (uint32_t i = 0; i < conf_->n_threads; i++)
			{
				std::vector<fd_handle_t> fds;

				// per-thread SO_REUSEPORT bind
				MEOW_UNIX_ADDRINFO_LIST_FOR_EACH(curr_ai, ai_list_)
				{
					auto fd_h = this->try_bind_to_addr(curr_ai);
					fds.push_back(std::move(fd_h));
				}

				// TODO(antoxa): replace passing i, with proper thread contexts
				std::thread t([this, i, fds = std::move(fds)]()
				{
					std::string const thr_name = ff::fmt_str("udp_reader/{0}", i);

					PINBA___OS_CALL(globals_, set_thread_name, thr_name);

					MEOW_DEFER(
						LOG_DEBUG(globals_->logger(), "{0}; exiting", thr_name);
					);

					this->eat_udp(i, fds);
				});

				threads_.push_back(move(t));
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

		void try_resolve_listen_addr_port()
		{
			os_addrinfo_list_ptr ai_list = os_unix::getaddrinfo_ex(conf_->address.c_str(), conf_->port.c_str(), AF_UNSPEC, SOCK_DGRAM, 0);
			MEOW_UNIX_ADDRINFO_LIST_FOR_EACH(curr_addr, ai_list)
			{
				LOG_INFO(globals_->logger(), "udp_reader; resolved {0}:{1} -> {2}", conf_->address, conf_->port, curr_addr->ai_addr);
			}

			os_addrinfo_t *ai = ai_list.get(); // take 1st item for now

			// commit if everything is ok
			ai_list_ = std::move(ai_list);
		}

		fd_handle_t try_bind_to_addr(os_addrinfo_t *ai)
		{
			fd_handle_t fd { os_unix::socket_ex(ai->ai_family, ai->ai_socktype, ai->ai_protocol) };
			os_unix::setsockopt_ex(*fd, SOL_SOCKET, SO_REUSEADDR, 1);
			os_unix::setsockopt_ex(*fd, SOL_SOCKET, SO_REUSEPORT, 1);
			if (ai->ai_family == AF_INET6)
				os_unix::setsockopt_ex(*fd, IPPROTO_IPV6, IPV6_V6ONLY, 1);
			os_unix::bind_ex(*fd, ai->ai_addr, ai->ai_addrlen);

			return fd;
		}

	private: // per-thread stuff

		void send_current_batch(uint32_t thread_id, raw_request_ptr& req)
		{
			stats_->udp.batch_send_total++;
			stats_->udp.packet_send_total += req->request_count;

			bool const success = out_sock_.send_message(req, NN_DONTWAIT);
			if (!success)
			{
				stats_->udp.batch_send_err++;
				stats_->udp.packet_send_err += req->request_count;
			}

			req.reset(); // signal the need to reinit
		}

		void eat_udp(uint32_t const thread_id, std::vector<fd_handle_t> const& fds)
		{
			if (globals_->os_symbols()->has_recvmmsg())
				this->eat_udp_recvmmsg(thread_id, fds);
			else
				this->eat_udp_recv(thread_id, fds);
		}

		void eat_udp_recv(uint32_t const thread_id, std::vector<fd_handle_t> const& fds)
		{
			static constexpr size_t const read_buffer_size = 64 * 1024; // max udp message size
			char buf[read_buffer_size];

			raw_request_ptr req;

			ProtobufCAllocator request_unpack_pba = {
				.alloc = nmpa___pba_alloc,
				.free = nmpa___pba_free,
				.allocator_data = NULL, // changed in progress
			};

			nmsg_poller_t poller;

			// extra stats
			poller.before_poll([this](timeval_t now, duration_t wait_for)
			{
				++stats_->udp.poll_total;
			});

			// periodic rusage
			poller.ticker(1 * d_second, [&](timeval_t now)
			{
				os_rusage_t const ru = os_unix::getrusage_ex(RUSAGE_THREAD);

				std::lock_guard<std::mutex> lk_(stats_->mtx);
				stats_->collector_threads[thread_id].ru_utime = timeval_from_os_timeval(ru.ru_utime);
				stats_->collector_threads[thread_id].ru_stime = timeval_from_os_timeval(ru.ru_stime);
			});

			// shutdown
			poller.read_nn_socket(shutdown_sock_, [&](timeval_t)
			{
				LOG_DEBUG(globals_->logger(), "udp_reader/{0}; received shutdown request", thread_id);
				poller.set_shutdown_flag();
			});
#if 0
			// resetable periodic event, to 'idly' send batch at regular intervals
			auto batch_send_tick = poller.ticker_with_reset(conf_->batch_timeout, [&](timeval_t now)
			{
				if (!req || req->request_count == 0)
					return;

				this->send_current_batch(thread_id, req);
			});
#endif
			// process udp packets from the network
			for (auto const& fd : fds)
			{
				poller.read_plain_fd(*fd, [&](timeval_t now)
				{
					char decompress_buf[read_buffer_size]; // re-used buffer for decompression

					// try receiving as much as possible without blocking
					while (true)
					{
						++stats_->udp.recv_total;

						int const n = recv(*fd, buf, sizeof(buf), MSG_DONTWAIT);
						if (n > 0)
						{
							++stats_->udp.recv_packets;
							stats_->udp.recv_bytes += uint64_t(n);

							// parse incoming bytes, and maybe decompress them
							net_datagram_t dgram = parse_network_datagram(str_ref{ buf, size_t(n) });
							if (dgram.version == 1)
							{
								if ((dgram.flags & PINBA_NET_DATAGRAM_FLAG___COMPRESSED_LZ4) != 0)
								{
									bool const ok = decompress_network_datagram(&dgram, decompress_buf, sizeof(decompress_buf));
									if (!ok)
									{
										// TODO: ++stats_->udp.packet_decompress_err;
										++stats_->udp.packet_decode_err;
										continue;
									}
								}
							}

							// unpack protobuf and push packet into batch
							if (!req)
							{
								constexpr size_t nmpa_block_size = 16 * 1024;
								req = meow::make_intrusive<raw_request_t>(conf_->batch_size, nmpa_block_size);
								request_unpack_pba.allocator_data = &req->nmpa;
							}

							Pinba__Request *request = pinba__request__unpack(&request_unpack_pba, dgram.data.c_length(), (uint8_t*)dgram.data.data());
							if (request == NULL) {
								++stats_->udp.packet_decode_err;
								continue;
							}

							req->requests[req->request_count] = request;
							req->request_count++;

							if (req->request_count >= conf_->batch_size)
							{
								this->send_current_batch(thread_id, req);
								// poller.reset_ticker(batch_send_tick, now);
							}

							continue;
						}

						if (n < 0) {
							if (errno == EINTR)
								continue;

							if (errno == EAGAIN)
							{
								++stats_->udp.recv_eagain;

								// need to send current batch if we've got anything
								if (req && req->request_count > 0)
								{
									this->send_current_batch(thread_id, req);
									// poller.reset_ticker(batch_send_tick, now);
								}

								// sleep for at least 1ms, before polling again, to let more packets arrive
								// and save a ton on system calls
								constexpr struct timespec const sleep_for = {
									.tv_sec = 0,
									.tv_nsec = 1 * 1000 * 1000,
								};
								nanosleep(&sleep_for, NULL);

								return;
							}

							LOG_ERROR(globals_->logger(), "udp_reader/{0}; recv() failed, exiting: {1}:{2}", thread_id, errno, strerror(errno));
							poller.set_shutdown_flag();
							return;
						}

						// XXX: this will never happen, even if socket is closed from main thread
						if (n == 0)
						{
							LOG_INFO(globals_->logger(), "udp_reader/{0}; recv socket closed, exiting", thread_id);
							poller.set_shutdown_flag();
							return;
						}
					}
				});
			} // for

			poller.loop();
		}

		void eat_udp_recvmmsg(uint32_t const thread_id, std::vector<fd_handle_t> const& fds)
		{
			size_t const max_message_size   = 64 * 1024; // max udp message size
			size_t const max_dgrams_to_recv = conf_->batch_size; // FIXME: make a special setting for this

			std::unique_ptr<struct mmsghdr[]> hdr_p { new struct mmsghdr[max_dgrams_to_recv] };
			struct mmsghdr *hdr = hdr_p.get();

			std::unique_ptr<struct iovec[]> iov_p { new struct iovec[max_dgrams_to_recv] };
			struct iovec *iov = iov_p.get();

			std::unique_ptr<char[]> recv_buffer_p { new char[max_dgrams_to_recv * max_message_size] };
			char *recv_buffer = recv_buffer_p.get();

			// touch all network memory in advance
			memset(hdr, 0, max_dgrams_to_recv * sizeof(*hdr));
			memset(iov, 0, max_dgrams_to_recv * sizeof(*iov));
			memset(recv_buffer, 0, max_dgrams_to_recv * max_message_size);

			for (unsigned i = 0; i < max_dgrams_to_recv; i++)
			{
				iov[i].iov_base           = recv_buffer + i * max_message_size;
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

			nmsg_poller_t poller;

			// extra stats
			poller.before_poll([this](timeval_t now, duration_t wait_for)
			{
				++stats_->udp.poll_total;
			});

			// periodic rusage
			poller.ticker(1 * d_second, [&](timeval_t now)
			{
				os_rusage_t const ru = os_unix::getrusage_ex(RUSAGE_THREAD);

				std::lock_guard<std::mutex> lk_(stats_->mtx);
				stats_->collector_threads[thread_id].ru_utime = timeval_from_os_timeval(ru.ru_utime);
				stats_->collector_threads[thread_id].ru_stime = timeval_from_os_timeval(ru.ru_stime);
			});

			// shutdown
			poller.read_nn_socket(shutdown_sock_, [&](timeval_t)
			{
				LOG_INFO(globals_->logger(), "udp_reader/{0}; received shutdown request", thread_id);
				poller.set_shutdown_flag();
			});

			// resetable periodic event, to 'idly' send batch at regular intervals
			auto batch_send_tick = poller.ticker_with_reset(conf_->batch_timeout, [&](timeval_t now)
			{
				if (!req || req->request_count == 0)
					return;

				this->send_current_batch(thread_id, req);
			});

			for (auto const& fd : fds)
			{
				poller.read_plain_fd(*fd, [&](timeval_t now)
				{
					char decompress_buf[max_message_size]; // re-used buffer for decompression

					// recv as much as possible without blocking
					// but see comments in EAGAIN handling on sleep() and saving syscalls
					while (true)
					{
						++stats_->udp.recv_total;

						int const n = globals_->os_symbols()->recvmmsg(*fd, hdr, max_dgrams_to_recv, MSG_DONTWAIT, NULL);
						if (n > 0)
						{
							stats_->udp.recv_packets += uint64_t(n);

							for (int i = 0; i < n; i++)
							{
								str_ref const network_bytes = { (char*)iov[i].iov_base, (size_t)hdr[i].msg_len };

								stats_->udp.recv_bytes += network_bytes.size();

								net_datagram_t dgram = parse_network_datagram(network_bytes);

								// maybe decompress, use thread-local tmp buffer as destination
								if (dgram.version == 1)
								{
									if ((dgram.flags & PINBA_NET_DATAGRAM_FLAG___COMPRESSED_LZ4) != 0)
									{
										bool const ok = decompress_network_datagram(&dgram, decompress_buf, sizeof(decompress_buf));
										if (!ok)
										{
											// TODO: ++stats_->udp.packet_decompress_err;
											++stats_->udp.packet_decode_err;
											continue;
										}
									}
								}

								// unpack protobuf into current batch's nmpa and push parsed request
								if (!req)
								{
									constexpr size_t nmpa_block_size = 16 * 1024;
									req = meow::make_intrusive<raw_request_t>(conf_->batch_size, nmpa_block_size);
									request_unpack_pba.allocator_data = &req->nmpa;
								}

								Pinba__Request *request = pinba__request__unpack(&request_unpack_pba, dgram.data.c_length(), (uint8_t*)dgram.data.data());
								if (request == NULL) {
									++stats_->udp.packet_decode_err;
									continue;
								}

								req->requests[req->request_count] = request;
								req->request_count++;

								if (req->request_count >= conf_->batch_size)
								{
									this->send_current_batch(thread_id, req);
									poller.reset_ticker(batch_send_tick, now);
								}
							}

							continue;
						}

						if (n < 0)
						{
							if (errno == EINTR)
								continue;

							if (errno == EAGAIN)
							{
								++stats_->udp.recv_eagain;

								// need to send current batch if we've got anything
								if (req && req->request_count > 0)
								{
									this->send_current_batch(thread_id, req);
									poller.reset_ticker(batch_send_tick, now);
								}

								// sleep for at least 1ms, before polling again, to let more packets arrive
								// and save a ton on system calls
								constexpr struct timespec const sleep_for = {
									.tv_sec = 0,
									.tv_nsec = 1 * 1000 * 1000,
								};
								nanosleep(&sleep_for, NULL);

								return;
							}

							LOG_ERROR(globals_->logger(), "udp_reader/{0}; recvmmsg() failed, exiting: {1}:{2}", thread_id, errno, strerror(errno));
							poller.set_shutdown_flag();
							return;
						}

						// XXX: this will never happen, even if socket is closed from main thread
						if (n == 0)
						{
							LOG_INFO(globals_->logger(), "udp_reader/{0}; recv socket closed, exiting", thread_id);
							poller.set_shutdown_flag();
							return;
						}
					} // recv loop
				});
			} // for

			poller.loop();
		}

	private:
		os_addrinfo_list_ptr  ai_list_;

		nmsg_socket_t         out_sock_;

		nmsg_socket_t         shutdown_sock_;
		nmsg_socket_t         shutdown_cli_sock_;
		std::mutex            shutdown_mtx_;

		pinba_globals_t       *globals_;
		pinba_stats_t         *stats_;
		collector_conf_t      *conf_;

		std::vector<std::thread> threads_;
	};

////////////////////////////////////////////////////////////////////////////////////////////////
}} // namespace { namespace aux {
////////////////////////////////////////////////////////////////////////////////////////////////

collector_ptr create_collector(pinba_globals_t *globals, collector_conf_t *conf)
{
	return meow::make_unique<aux::collector_impl_t>(globals, conf);
}

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <thread>
#include <stdexcept>

#include <boost/noncopyable.hpp>

#include <meow/format/format_to_string.hpp>
#include <meow/hash/hash.hpp>
#include <meow/hash/hash_impl.hpp>
#include <meow/unix/time.hpp>
#include <meow/std_unique_ptr.hpp>

#include <nanomsg/nn.h>
#include <nanomsg/pipeline.h>
#include <nanomsg/reqrep.h>
#include <nanomsg/pubsub.h>

#include "misc/nmpa.h"
#include "misc/nmpa_pba.h"

#include "proto/pinba.pb-c.h"

#include "pinba/globals.h"
#include "pinba/collector.h"
#include "pinba/dictionary.h"
#include "pinba/packet.h"

#include "pinba/nmsg_socket.h"
#include "pinba/nmsg_ticker.h"
#include "pinba/nmsg_poller.h"

////////////////////////////////////////////////////////////////////////////////////////////////

// #include <atomic>

struct repacker_conf_t
{
	std::string  nn_input;         // read raw_request_t from this nanomsg pipe
	std::string  nn_output;        // send batched repacked packets to this nanomsg pipe

	size_t       nn_input_buffer;  // NN_RCVBUF for nn_input connection

	// dictionary_t dictionary;    // dictionary to use when repacking requests

	uint32_t     n_threads;        // threads to start

	uint32_t     batch_size;       // max packets in batch
	duration_t   batch_timeout;    // max delay between batches
};

struct packet_batch_t : public nmsg_message_ex_t<packet_batch_t>
{
	struct nmpa_s  nmpa;
	uint32_t       packet_count;
	packet_t      **packets;

	packet_batch_t(size_t max_packets, size_t nmpa_block_sz)
		: packet_count{0}
	{
		nmpa_init(&nmpa, nmpa_block_sz);
		packets = (packet_t**)nmpa_alloc(&nmpa, sizeof(packets[0]) * max_packets);
	}

	~packet_batch_t()
	{
		nmpa_free(&nmpa);
	}
};
typedef boost::intrusive_ptr<packet_batch_t> packet_batch_ptr;

struct repacker_t : private boost::noncopyable
{
public:

	repacker_t(pinba_globals_t *globals, repacker_conf_t *conf)
		: globals_(globals)
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
			std::thread t([i, this, sock_fd]()
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

		dictionary_t thread_local_dict;

		struct nn_pollfd pfd[] = {
			{ .fd = *in_sock, .events = NN_POLLIN, .revents = 0, },
		};
		size_t const pfd_size = sizeof(pfd) / sizeof(pfd[0]);

		while (true)
		{
			int const wait_for_ms = duration_from_timeval(next_tick_tv - os_unix::clock_monotonic_now()).nsec / d_millisecond.nsec;

			++globals_->repacker.poll_total;
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
				++globals_->repacker.recv_total;

				auto const req = in_sock.recv<raw_request_ptr>(NN_DONTWAIT);
				if (!req) { // EAGAIN
					++globals_->repacker.recv_eagain;
					break; // next iteration of outer loop
				}

				for (uint32_t i = 0; i < req->request_count; i++)
				{
					auto const pb_req = req->requests[i];

					auto const vr = validate_packet(pb_req);
					if (vr != packet_validate_result::okay)
					{
						ff::fmt(stderr, "packet validation failed: {0}\n", vr);
						continue;
					}

					// packet_t *packet = pinba_request_to_packet(pb_req, &dictionary_, &batch->nmpa);
					packet_t *packet = pinba_request_to_packet(pb_req, &thread_local_dict, &batch->nmpa);

					++globals_->repacker.packets_processed;

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
	repacker_conf_t  *conf_;

	dictionary_t     dictionary_;

	std::vector<std::thread> threads_;
};

////////////////////////////////////////////////////////////////////////////////////////////////

struct report_t
{
};
typedef std::unique_ptr<report_t> report_ptr;

struct report_conf_t
{
	report_ptr   report;
	std::string  report_name;
	duration_t   tick_interval;
};

struct report_handler_conf_t
{
	std::string nn_packets;         // get packet_batch_ptr from this endpoint as fast as possible (SUB, pair to coodinator PUB)
	size_t      nn_packets_buffer;  // NN_RCVBUF on nn_packets
};

struct report_handler_t : private boost::noncopyable
{
	pinba_globals_t        *globals_;
	report_handler_conf_t  *conf_;

	nmsg_socket_t          packets_sock_;

	nmsg_ticker_chan_ptr   ticker_chan_;

	report_ptr             report_;
	std::string            report_name_;

	uint64_t               packets_received;

public:

	report_handler_t(pinba_globals_t *globals, report_handler_conf_t *conf)
		: globals_(globals)
		, conf_(conf)
		, packets_received(0)
	{
		packets_sock_
			.open(AF_SP, NN_SUB)
			.set_option(NN_SUB, NN_SUB_SUBSCRIBE, "")
			.set_option(NN_SOL_SOCKET, NN_RCVBUF, sizeof(packet_batch_ptr) * conf_->nn_packets_buffer, "report_handler_t::in_sock")
			.connect(conf_->nn_packets.c_str());
	}

	void startup(uint32_t thread_id, report_conf_t *report_conf)
	{
		if (report_)
			throw std::logic_error("report handler is already started");

		report_ = move(report_conf->report);
		report_name_ = ff::fmt_str("rh/{0}/{1}", thread_id, report_conf->report_name);
		ticker_chan_ = globals_->ticker->subscribe(report_conf->tick_interval, report_name_);

		std::thread t([this, thread_id]()
		{
			nmsg_poller_t()
				.read(*ticker_chan_, [&](nmsg_ticker_chan_t& chan, timeval_t now)
				{
					chan.recv();
					// ff::fmt(stdout, "{0}; {1}; received {2} packets\n", report_name_, now, packets_received);
				})
				.read_sock(*packets_sock_, [&](timeval_t now)
				{
					auto const batch = packets_sock_.recv<packet_batch_ptr>();
					packets_received += batch->packet_count;
				})
				.loop();
		});
		t.detach();
	}
};
typedef std::unique_ptr<report_handler_t> report_handler_ptr;

////////////////////////////////////////////////////////////////////////////////////////////////

struct coordinator_conf_t
{
	std::string  nn_input;                // connect to this socket and receive packet_batch_ptr-s (PULL)
	size_t       nn_input_buffer;         // NN_RCVBUF for nn_input (can leav this small, due to low-ish traffic)

	std::string  nn_control;              // control messages received here (binds, REP)
	std::string  nn_report_output;        // broadcasts packet_batch_ptr-s for reports here (binds, PUB)
	size_t       nn_report_output_buffer; // report_handler uses this as NN_RCVBUF
};

#define COORD_REQ__ADD_REPORT 0
#define COORD_REQ__DEL_REPORT 1

struct coordinator_control_response_t
	: public nmsg_message_ex_t<coordinator_control_response_t>
{
	int status;
};
typedef boost::intrusive_ptr<coordinator_control_response_t> coordinator_control_response_ptr;


struct coordinator_control_request_t
	: public nmsg_message_ex_t<coordinator_control_request_t>
{
	int type;

	// add_report
	report_conf_t report_conf;

	// del_report
	std::string report_name;
};
typedef boost::intrusive_ptr<coordinator_control_request_t> coordinator_control_request_ptr;


struct coordinator_t : private boost::noncopyable
{
	struct rh_info_t
	{
		report_handler_conf_t conf;
		report_handler_ptr    rh;
	};
	typedef std::unique_ptr<rh_info_t> rh_info_ptr;

	coordinator_t(pinba_globals_t *globals, coordinator_conf_t *conf)
		: globals_(globals)
		, conf_(conf)
	{
		in_sock_ = nmsg_socket(AF_SP, NN_PULL);
		if (conf_->nn_input_buffer > 0)
			in_sock_.set_option(NN_SOL_SOCKET, NN_RCVBUF, conf_->nn_input_buffer * sizeof(packet_batch_ptr), conf_->nn_input);

		control_sock_ = nmsg_socket(AF_SP, NN_REP);
		control_sock_.bind(conf_->nn_control.c_str());

		report_sock_ = nmsg_socket(AF_SP, NN_PUB);
		report_sock_.bind(conf_->nn_report_output.c_str());
	}

	void startup()
	{
		in_sock_.connect(conf_->nn_input.c_str());

		std::thread t([&]()
		{
			{
				std::string const thr_name = ff::fmt_str("coordinator");
				pthread_setname_np(pthread_self(), thr_name.c_str());
			}

			auto const tick_chan = globals_->ticker->subscribe(1000 * d_millisecond, "coordinator_thread");

			uint64_t total_packets = 0;
			uint64_t n_small_batches = 0;
			uint64_t small_batch_packets = 0;

			nmsg_poller_t()
				.read(*tick_chan, [&](nmsg_ticker_chan_t& chan, timeval_t now)
				{
					chan.recv(); // MUST do this, or chan will stay readable
					ff::fmt(stdout, "{0}; {1}; processed {2} packets, n_sm_b: {3}, sb_pkt = {4}\n",
						chan.endpoint(), now, total_packets, n_small_batches, small_batch_packets);

					auto const *udp_stats = &globals_->udp;
					ff::fmt(stdout, "{0}; {1}; udp recv: {2}, nonblock: {3}, eagain: {4}, processed: {5}\n",
						chan.endpoint(), now, (uint64_t)udp_stats->recv_total, (uint64_t)udp_stats->recv_nonblocking,
						(uint64_t)udp_stats->recv_eagain, (uint64_t)udp_stats->packets_received);

					auto const *repack_stats = &globals_->repacker;
					ff::fmt(stdout, "{0}; {1}; repacker poll: {2}, recv: {3}, eagain: {4}, processed: {5}\n",
						chan.endpoint(), now, (uint64_t)repack_stats->poll_total, (uint64_t)repack_stats->recv_total,
						(uint64_t)repack_stats->recv_eagain, (uint64_t)repack_stats->packets_processed);
				})
				.read_sock(*in_sock_, [&](timeval_t now)
				{
					auto batch = in_sock_.recv<packet_batch_ptr>();

					if (batch->packet_count < 1024)
					{
						n_small_batches++;
						small_batch_packets += batch->packet_count;
						// ff::fmt(stdout, "{0}; {1}; batch {2} packets\n", "packet_counter", now, batch->packet_count);
					}

					total_packets += batch->packet_count;

					// FIXME: this is fundamentally broken with PUB/SUB sadly
					// since we have no idea if the report handler is slow, and in that case messages will be dropped
					// and ref counts will not be decremented and memory will leak and we won't have any stats about that too
					// (we also have no need for the pub/sub routing part at the moment and probably won't need it ever)
					// so should probably reimplement this with just a loop sending things to all reports
					// (might also try avoid blocking with threads/polls, but this might get too complicated)
					// and _add_ref() should be called only if send was successful (with NN_DONTWAIT ofc), to avoid the need to call _release()
					// note that this requires multiple calls to _add_ref() instead of single one
					{
						for (size_t i = 0; i < report_handlers_.size(); i++)
							intrusive_ptr_add_ref(batch.get());

						report_sock_.send_ex(batch, 0);
					}
				})
				.read_sock(*control_sock_, [&](timeval_t now)
				{
					auto const req = control_sock_.recv<coordinator_control_request_ptr>();

					ff::fmt(stdout, "coordinator_thread; received control request: {0}\n", req->type);

					switch (req->type)
					{
						case COORD_REQ__ADD_REPORT:
						{
							auto rhi = meow::make_unique<rh_info_t>();
							rhi->conf = report_handler_conf_t {
								.nn_packets        = conf_->nn_report_output,
								.nn_packets_buffer = conf_->nn_report_output_buffer,
							};
							rhi->rh = meow::make_unique<report_handler_t>(globals_, &rhi->conf);
							rhi->rh->startup(report_handlers_.size() + 10, &req->report_conf);

							report_handlers_.push_back(move(rhi));

							coordinator_control_response_ptr response { new coordinator_control_response_t() };
							response->status = 0;

							// control_sock_.send_intrusive_ptr(response);
							control_sock_.send_message(response);
						}
						break;

						default:
							ff::fmt(stderr, "unknown coordinator_control_request type: {0}\n", req->type);
							break;
					}
				})
				.loop();
		});
		t.detach();
	}

private:
	pinba_globals_t     *globals_;
	coordinator_conf_t  *conf_;

	std::vector<rh_info_ptr> report_handlers_;

	nmsg_socket_t    in_sock_;
	nmsg_socket_t    control_sock_;
	nmsg_socket_t    report_sock_;
};

////////////////////////////////////////////////////////////////////////////////////////////////

// typedef std::unordered_map<std::string, report_ptr> reports_map_t;
// reports_map_t reports;
// reports["by_script_name"] = meow::make_unique<report_by_key_t<report_traits___script_name>>();
// reports["by_server_name"] = meow::make_unique<report_by_key_t<report_traits___server_name>>();
// reports["by_by_timer_tag1__tag10"] = meow::make_unique<report_by_timer_tag1_t>("tag10");
// reports["by_by_timer_tagN__tag10_tag11"] = meow::make_unique<report_by_timer_tagN_t>(std::vector<std::string>{"tag10", "tag11"});

int main(int argc, char const *argv[])
try
{
	pinba_globals_t globals = {};
	globals.ticker = meow::make_unique<nmsg_ticker___single_thread_t>();

	collector_conf_t collector_conf = {
		.address       = "0.0.0.0",
		.port          = "3002",
		.nn_output     = "inproc://udp-collector",
		.n_threads     = 2,
		.batch_size    = 128,
		.batch_timeout = 10 * d_millisecond,
	};
	auto collector = create_collector(&globals, &collector_conf);

	repacker_conf_t repacker_conf = {
		.nn_input        = collector_conf.nn_output,
		.nn_output       = "inproc://repacker",
		.nn_input_buffer = 2 * 1024,
		.n_threads       = 2, // FIXME: should be == 1, since dictionary is shared between threads
		.batch_size      = 1024,
		.batch_timeout   = 100 * d_millisecond,
	};
	auto repacker = meow::make_unique<repacker_t>(&globals, &repacker_conf);

	coordinator_conf_t coordinator_conf = {
		.nn_input                = repacker_conf.nn_output,
		.nn_input_buffer         = 16,
		.nn_control              = "inproc://coordinator/control",
		.nn_report_output        = "inproc://coordinator/report-data",
		.nn_report_output_buffer = 16,
	};
	auto coordinator = meow::make_unique<coordinator_t>(&globals, &coordinator_conf);

	coordinator->startup();
	repacker->startup();
	collector->startup();

	// coordinator->add_report(report_conf_t{
	// 	.report_name    = "test",
	// 	.report         = meow::make_unique<report_t>(),
	// 	.tick_interval  = 100 * d_millisecond,
	// 	.total_time     = 60  * d_second,
	// });

	{
		nmsg_socket_t sock;
		sock.open(AF_SP, NN_REQ).connect(coordinator_conf.nn_control.c_str());

		for (uint32_t i = 0; i < 4; i++)
		{

			coordinator_control_request_ptr r { new coordinator_control_request_t() };
			r->type                      = COORD_REQ__ADD_REPORT;
			r->report_conf.report_name   = ff::fmt_str("test/{0}", i);
			r->report_conf.report        = meow::make_unique<report_t>();
			r->report_conf.tick_interval = 1000 * d_millisecond;

			sock.send_message(r);

			auto const response = sock.recv<coordinator_control_response_ptr>();
			ff::fmt(stdout, "got control response: {0}\n", response->status);
		}
	}

	getchar();

	return 0;
}
catch (std::exception const& e)
{
	ff::fmt(stderr, "error: {0}\n", e.what());
	return 1;
}
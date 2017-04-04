#ifndef PINBA__PINBA_MYSQL_H_
#define PINBA__PINBA_MYSQL_H_

#if defined(PINBA_ENGINE_DEBUG_ON) && !defined(DBUG_ON)
# undef DBUG_OFF
# define DBUG_ON
#endif

#if defined(PINBA_ENGINE_DEBUG_OFF) && !defined(DBUG_OFF)
# define DBUG_OFF
# undef DBUG_ON
#endif

////////////////////////////////////////////////////////////////////////////////////////////////
// variables configured through mysql config file

struct pinba_variables_t
{
	char      *address                  = nullptr;
	int       port                      = 0;
	unsigned  default_history_time_sec  = 0;
	unsigned  udp_reader_threads        = 0;
	unsigned  repacker_threads          = 0;
	unsigned  repacker_input_buffer     = 0;
	unsigned  repacker_batch_messages   = 0;
	unsigned  repacker_batch_timeout_ms = 0;
	unsigned  report_input_buffer       = 0;
};

pinba_variables_t* pinba_variables();

////////////////////////////////////////////////////////////////////////////////////////////////
// status variables reported through show global status and the likes of it

// use only standard c99 types here, like long, etc. to be compatible with mysql internals

struct pinba_status_variables_t
{
	double              uptime;

	unsigned long long  udp_poll_total;
	unsigned long long  udp_recv_total;
	unsigned long long  udp_recv_eagain;
	unsigned long long  udp_recv_bytes;
	unsigned long long  udp_recv_packets;
	unsigned long long  udp_packet_decode_err;
	unsigned long long  udp_batch_send_total;
	unsigned long long  udp_batch_send_err;
	double              udp_ru_utime;
	double              udp_ru_stime;

	unsigned long long  repacker_poll_total;
	unsigned long long  repacker_recv_total;
	unsigned long long  repacker_recv_eagain;
	unsigned long long  repacker_recv_packets;
	unsigned long long  repacker_packet_validate_err;
	unsigned long long  repacker_batch_send_total;
	unsigned long long  repacker_batch_send_by_timer;
	unsigned long long  repacker_batch_send_by_size;
	double              repacker_ru_utime;
	double              repacker_ru_stime;

	unsigned long long  coordinator_batches_received;
	unsigned long long  coordinator_batch_send_total;
	unsigned long long  coordinator_batch_send_err;
	unsigned long long  coordinator_control_requests;
	double              coordinator_ru_utime;
	double              coordinator_ru_stime;

	unsigned long long  dictionary_size;
	unsigned long long  dictionary_mem_used;
};

void                      pinba_update_status_variables(); // plugin.cpp
pinba_status_variables_t* pinba_status_variables();        // handler.cpp

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__PINBA_MYSQL_H_

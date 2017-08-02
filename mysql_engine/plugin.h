#ifndef PINBA__MYSQL_ENGINE__PLUGIN_H_
#define PINBA__MYSQL_ENGINE__PLUGIN_H_

#include "mysql_engine/pinba_mysql.h"

#ifdef PINBA_USE_MYSQL_SOURCE
#	include <mysql_version.h>
#	include <mysql/plugin.h>
#else
#	include <mysql/mysql_version.h>
#	include <mysql/plugin.h>
#endif // PINBA_USE_MYSQL_SOURCE

////////////////////////////////////////////////////////////////////////////////////////////////
// variables configured through mysql config file

struct pinba_variables_t
{
	char      *address                  = nullptr;
	int       port                      = 0;
	char      *log_level                = nullptr;
	unsigned  default_history_time_sec  = 0;
	unsigned  udp_reader_threads        = 0;
	unsigned  repacker_threads          = 0;
	unsigned  repacker_input_buffer     = 0;
	unsigned  repacker_batch_messages   = 0;
	unsigned  repacker_batch_timeout_ms = 0;
	unsigned  coordinator_input_buffer  = 0;
	unsigned  report_input_buffer       = 0;
	char      packet_debug              = 0;
	double    packet_debug_fraction     = 0.01;
};

pinba_variables_t* pinba_variables();

////////////////////////////////////////////////////////////////////////////////////////////////
// status variables reported through show global status and the likes of it

// use only standard c99 types here, like long, etc. to be compatible with mysql internals
// when changing this struct - also fix plugin.cpp vars list and handler.cpp stats table view

struct pinba_status_variables_t
{
	double              uptime;
	double              ru_utime;
	double              ru_stime;

	unsigned long long  udp_poll_total;
	unsigned long long  udp_recv_total;
	unsigned long long  udp_recv_eagain;
	unsigned long long  udp_recv_bytes;
	unsigned long long  udp_recv_packets;
	unsigned long long  udp_packet_decode_err;
	unsigned long long  udp_batch_send_total;
	unsigned long long  udp_batch_send_err;
	unsigned long long  udp_packet_send_total;
	unsigned long long  udp_packet_send_err;
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
	unsigned long long  dictionary_mem_hash;
	unsigned long long  dictionary_mem_list;
	unsigned long long  dictionary_mem_strings;

	char                extra[1024];

	char                version_info[1024];
	char                build_string[1024];
};
using pinba_status_variables_ptr = std::unique_ptr<pinba_status_variables_t>;

pinba_status_variables_ptr pinba_collect_status_variables();
pinba_status_variables_t*  pinba_status_variables();

////////////////////////////////////////////////////////////////////////////////////////////////

#endif // PINBA__MYSQL_ENGINE__PLUGIN_H_

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

#endif // PINBA__PINBA_MYSQL_H_

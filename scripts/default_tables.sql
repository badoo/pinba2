# create default tables, that you might want to use in most circumstances

use pinba;

-- experimental
-- create view status_variables as
--   select
--     (select VARIABLE_VALUE from information_schema.global_status where VARIABLE_NAME='PINBA_UPTIME') as uptime,
--     (select VARIABLE_VALUE from information_schema.global_status where VARIABLE_NAME='PINBA_UDP_POLL_TOTAL') as udp_poll_total,
--     (select VARIABLE_VALUE from information_schema.global_status where VARIABLE_NAME='PINBA_UDP_RECV_PACKETS') as udp_recv_packets;

CREATE TABLE if not exists `stats` (
  `uptime` double NOT NULL,
  `udp_poll_total` bigint(20) unsigned NOT NULL,
  `udp_recv_total` bigint(20) unsigned NOT NULL,
  `udp_recv_eagain` bigint(20) unsigned NOT NULL,
  `udp_recv_bytes` bigint(20) unsigned NOT NULL,
  `udp_packets_received` bigint(20) unsigned NOT NULL,
  `udp_packets_decode_err` bigint(20) unsigned NOT NULL,
  `udp_batches` bigint(20) unsigned NOT NULL,
  `udp_batches_lost` bigint(20) unsigned NOT NULL,
  `repacker_ru_utime` double NOT NULL,
  `repacker_ru_stime` double NOT NULL
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/stats';

CREATE TABLE if not exists `active` (
  `table_name` varchar(128) NOT NULL,
  `internal_name` varchar(128) NOT NULL,
  `kind` varchar(64) NOT NULL,
  `uptime` double unsigned NOT NULL,
  `time_window_sec` int(10) unsigned NOT NULL,
  `tick_count` int(10) NOT NULL,
  `approx_row_count` int(10) unsigned NOT NULL,
  `approx_mem_used` bigint(20) unsigned NOT NULL,
  `packets_received` bigint(20) unsigned NOT NULL,
  `packets_lost` bigint(20) unsigned NOT NULL,
  `ru_utime` double NOT NULL,
  `ru_stime` double NOT NULL,
  `last_tick_time` double NOT NULL,
  `last_tick_prepare_duration` double NOT NULL,
  `last_snapshot_merge_duration` double NOT NULL
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/active';

CREATE TABLE if not exists `info` (
  `req_count` bigint(20) unsigned NOT NULL,
  `timer_count` bigint(20) unsigned NOT NULL,
  `time_total` double NOT NULL,
  `ru_utime_total` double NOT NULL,
  `ru_stime_total` double NOT NULL,
  `traffic_kb` bigint(20) unsigned NOT NULL,
  `memory_usage` bigint(20) unsigned NOT NULL
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/packet/default_history_time/no_keys/no_percentiles/no_filters';


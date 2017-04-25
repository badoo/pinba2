# create default tables, that you might want to use in most circumstances

use pinba;

-- experimental
-- create view status_variables as
--   select
--     (select VARIABLE_VALUE from information_schema.global_status where VARIABLE_NAME='PINBA_UPTIME') as uptime,
--     (select VARIABLE_VALUE from information_schema.global_status where VARIABLE_NAME='PINBA_UDP_POLL_TOTAL') as udp_poll_total,
--     (select VARIABLE_VALUE from information_schema.global_status where VARIABLE_NAME='PINBA_UDP_RECV_PACKETS') as udp_recv_packets;

CREATE TABLE if not exists `stats` (
  `uptime` DOUBLE NOT NULL,
  `udp_poll_total` BIGINT(20) UNSIGNED NOT NULL,
  `udp_recv_total` BIGINT(20) UNSIGNED NOT NULL,
  `udp_recv_eagain` BIGINT(20) UNSIGNED NOT NULL,
  `udp_recv_bytes` BIGINT(20) UNSIGNED NOT NULL,
  `udp_recv_packets` BIGINT(20) UNSIGNED NOT NULL,
  `udp_packet_decode_err` BIGINT(20) UNSIGNED NOT NULL,
  `udp_batch_send_total` BIGINT(20) UNSIGNED NOT NULL,
  `udp_batch_send_err` BIGINT(20) UNSIGNED NOT NULL,
  `udp_ru_utime` DOUBLE NOT NULL,
  `udp_ru_stime` DOUBLE NOT NULL,
  `repacker_poll_total` BIGINT(20) UNSIGNED NOT NULL,
  `repacker_recv_total` BIGINT(20) UNSIGNED NOT NULL,
  `repacker_recv_eagain` BIGINT(20) UNSIGNED NOT NULL,
  `repacker_recv_packets` BIGINT(20) UNSIGNED NOT NULL,
  `repacker_packet_validate_err` BIGINT(20) UNSIGNED NOT NULL,
  `repacker_batch_send_total` BIGINT(20) UNSIGNED NOT NULL,
  `repacker_batch_send_by_timer` BIGINT(20) UNSIGNED NOT NULL,
  `repacker_batch_send_by_size` BIGINT(20) UNSIGNED NOT NULL,
  `repacker_ru_utime` DOUBLE NOT NULL,
  `repacker_ru_stime` DOUBLE NOT NULL,
  `coordinator_batches_received` BIGINT(20) UNSIGNED NOT NULL,
  `coordinator_batch_send_total` BIGINT(20) UNSIGNED NOT NULL,
  `coordinator_batch_send_err` BIGINT(20) UNSIGNED NOT NULL,
  `coordinator_control_requests` BIGINT(20) UNSIGNED NOT NULL,
  `coordinator_ru_utime` DOUBLE NOT NULL,
  `coordinator_ru_stime` DOUBLE NOT NULL,
  `dictionary_size` BIGINT(20) UNSIGNED NOT NULL,
  `dictionary_mem_used` BIGINT(20) UNSIGNED NOT NULL
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
  `packets_aggregated` bigint(20) unsigned NOT NULL,
  `packets_dropped_by_bloom` bigint(20) unsigned NOT NULL,
  `packets_dropped_by_filters` bigint(20) unsigned NOT NULL,
  `packets_dropped_by_rfield` bigint(20) unsigned NOT NULL,
  `packets_dropped_by_rtag` bigint(20) unsigned NOT NULL,
  `packets_dropped_by_timertag` bigint(20) unsigned NOT NULL,
  `timers_scanned` bigint(20) unsigned NOT NULL,
  `timers_aggregated` bigint(20) unsigned NOT NULL,
  `timers_skipped_by_filters` bigint(20) unsigned NOT NULL,
  `timers_skipped_by_tags` bigint(20) unsigned NOT NULL,
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


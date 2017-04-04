# create default tables, that you might want to use in most circumstances

use pinba;

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

CREATE TABLE if not exists `active_reports` (
  `table_name` varchar(128) NOT NULL,
  `internal_name` varchar(128) NOT NULL,
  `kind` int(10) unsigned NOT NULL,
  `needs_engine` tinyint(1) NOT NULL,
  `is_active` tinyint(1) NOT NULL,
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/active';

CREATE TABLE if not exists `packet_info` (
  `req_count` bigint(20) unsigned NOT NULL,
  `timer_count` bigint(20) unsigned NOT NULL,
  `time_total` double NOT NULL,
  `ru_utime_total` double NOT NULL,
  `ru_stime_total` double NOT NULL,
  `traffic_kb` bigint(20) unsigned NOT NULL,
  `memory_usage` bigint(20) unsigned NOT NULL
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/packet/default_history_time/no_keys/no_percentiles/no_filters';


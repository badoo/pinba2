USE pinba;

CREATE TABLE if not exists `stats` (
  `uptime` double NOT NULL,
  `udp_recv_total` bigint(20) unsigned NOT NULL,
  `udp_recv_nonblocking` bigint(20) unsigned NOT NULL,
  `udp_recv_eagain` bigint(20) unsigned NOT NULL,
  `udp_packets_received` bigint(20) unsigned NOT NULL,
  `udp_packets_decode_err` bigint(20) unsigned NOT NULL,
  `udp_batches` bigint(20) unsigned NOT NULL,
  `udp_batches_lost` bigint(20) unsigned NOT NULL
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/stats';

CREATE TABLE if not exists `active_reports` (
  `table_name` varchar(128) NOT NULL,
  `internal_name` varchar(128) NOT NULL,
  `kind` int(10) unsigned NOT NULL,
  `needs_engine` tinyint(1) NOT NULL,
  `is_active` tinyint(1) NOT NULL
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/active';

CREATE TABLE `report_by_host_name` (
  `script` varchar(64) NOT NULL,
  `req_count` int(10) unsigned NOT NULL,
  `time_total` double NOT NULL,
  `ru_utime_total` double NOT NULL,
  `ru_stime_total` double NOT NULL
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/request/default_history_time/~host/no_percentiles/no_filters';

CREATE TABLE `report_by_script_name` (
  `script` varchar(64) NOT NULL,
  `req_count` int(10) unsigned NOT NULL,
  `time_total` double NOT NULL,
  `ru_utime_total` double NOT NULL,
  `ru_stime_total` double NOT NULL
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/request/default_history_time/~script/no_percentiles/no_filters';

CREATE TABLE `report_by_server_name` (
  `script` varchar(64) NOT NULL,
  `req_count` int(10) unsigned NOT NULL,
  `time_total` double NOT NULL,
  `ru_utime_total` double NOT NULL,
  `ru_stime_total` double NOT NULL
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/request/default_history_time/~server/no_percentiles/no_filters';

# default reports you might want to use (these try to mimic original pinba reports)
#
# NOTE
# these reports are created with default_history_time,
# so they will use whatever time is configured in my.cnf (60sec by default)
#

USE pinba;

CREATE TABLE `report_by_host_name` (
  `host` varchar(64) NOT NULL,
  `req_count` int(10) unsigned NOT NULL,
  `req_per_sec` float NOT NULL,
  `req_percent` float,
  `req_time_total` float NOT NULL,
  `req_time_per_sec` float NOT NULL,
  `req_time_percent` float,
  `ru_utime_total` float NOT NULL,
  `ru_utime_per_sec` float NOT NULL,
  `ru_utime_percent` float,
  `ru_stime_total` float NOT NULL,
  `ru_stime_per_sec` float NOT NULL,
  `ru_stime_percent` float,
  `traffic_total` bigint(20) unsigned NOT NULL,
  `traffic_per_sec` float NOT NULL,
  `traffic_percent` float,
  `memory_footprint` bigint(20) NOT NULL,
  `memory_per_sec` float NOT NULL,
  `memory_percent` float
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/request/default_history_time/~host/no_percentiles/no_filters';

CREATE TABLE `report_by_script_name` (
  `script` varchar(64) NOT NULL,
  `req_count` int(10) unsigned NOT NULL,
  `req_per_sec` float NOT NULL,
  `req_percent` float,
  `req_time_total` float NOT NULL,
  `req_time_per_sec` float NOT NULL,
  `req_time_percent` float,
  `ru_utime_total` float NOT NULL,
  `ru_utime_per_sec` float NOT NULL,
  `ru_utime_percent` float,
  `ru_stime_total` float NOT NULL,
  `ru_stime_per_sec` float NOT NULL,
  `ru_stime_percent` float,
  `traffic_total` bigint(20) unsigned NOT NULL,
  `traffic_per_sec` float NOT NULL,
  `traffic_percent` float,
  `memory_footprint` bigint(20) NOT NULL,
  `memory_per_sec` float NOT NULL,
  `memory_percent` float
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/request/default_history_time/~script/no_percentiles/no_filters';

CREATE TABLE `report_by_server_name` (
  `server` varchar(64) NOT NULL,
  `req_count` int(10) unsigned NOT NULL,
  `req_per_sec` float NOT NULL,
  `req_percent` float,
  `req_time_total` float NOT NULL,
  `req_time_per_sec` float NOT NULL,
  `req_time_percent` float,
  `ru_utime_total` float NOT NULL,
  `ru_utime_per_sec` float NOT NULL,
  `ru_utime_percent` float,
  `ru_stime_total` float NOT NULL,
  `ru_stime_per_sec` float NOT NULL,
  `ru_stime_percent` float,
  `traffic_total` bigint(20) unsigned NOT NULL,
  `traffic_per_sec` float NOT NULL,
  `traffic_percent` float,
  `memory_footprint` bigint(20) NOT NULL,
  `memory_per_sec` float NOT NULL,
  `memory_percent` float
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/request/default_history_time/~server/no_percentiles/no_filters';

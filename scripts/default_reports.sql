# default reports you might want to use (these try to mimic original pinba reports)
#
# NOTE
# these reports are created with default_history_time,
# so they will use whatever time is configured in my.cnf (60sec by default)
#

use pinba;

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

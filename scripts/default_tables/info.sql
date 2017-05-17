CREATE TABLE IF NOT EXISTS `pinba`.`info` (
  `req_count` bigint(20) unsigned NOT NULL,
  `timer_count` bigint(20) unsigned NOT NULL,
  `time_total` double NOT NULL,
  `ru_utime_total` double NOT NULL,
  `ru_stime_total` double NOT NULL,
  `traffic_kb` bigint(20) unsigned NOT NULL,
  `memory_usage` bigint(20) unsigned NOT NULL
) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/packet/default_history_time/no_keys/no_percentiles/no_filters';
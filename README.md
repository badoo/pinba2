# Pinba2
An attempt to rethink internal implementation and some features of excellent https://github.com/tony2001/pinba_engine by @tony2001.

# Building
requires
- gcc 4.9.4+ (but will increase this requirement to something like gcc6+ soon)
- boost: http://boost.org/
- meow: https://github.com/anton-povarov/meow
- nanomsg: http://nanomsg.org/
- google sparsehash: https://github.com/sparsehash/sparsehash

To build, run

    $ ./buildconf.sh

    $ ./configure
        --prefix=<path>
        --with-mysql=<path to mysql source code (you *MUST* have run configure in there first)>
        --with-nanomsg=<nanomsg install dir>
        --with-sparsehash=<path>
        --with-meow=<path>
        --with-boost=<path (need headers only)>

# SQL
Stats report

	mysql> CREATE TABLE `stats` (
			  `uptime` double NOT NULL,
			  `udp_recv_total` bigint(20) unsigned NOT NULL,
			  `udp_recv_nonblocking` bigint(20) unsigned NOT NULL,
			  `udp_recv_eagain` bigint(20) unsigned NOT NULL,
			  `udp_packets_received` bigint(20) unsigned NOT NULL
			) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/stats';

	mysql> select uptime, udp_recv_total, udp_packets_received, udp_packets_received/uptime as udp_packets_per_sec from stats;
	+--------------+----------------+----------------------+---------------------+
	| uptime       | udp_recv_total | udp_packets_received | udp_packets_per_sec |
	+--------------+----------------+----------------------+---------------------+
	| 74.323456902 |          96987 |              2000002 |    26909.4318720552 |
	+--------------+----------------+----------------------+---------------------+
	1 row in set (0.00 sec)

Request data report (report by script name only here)

	mysql> CREATE TABLE `report_by_script_name` (
			  `script` varchar(64) NOT NULL,
			  `req_count` int(10) unsigned NOT NULL,
			  `time_total` double NOT NULL,
			  `ru_utime_total` double NOT NULL,
			  `ru_stime_total` double NOT NULL
			) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/request/60/~script/no_percentiles/no_filters'

	mysql> select * from report_by_script_name;
	+----------------+-----------+------------+----------------+----------------+
	| script         | req_count | time_total | ru_utime_total | ru_stime_total |
	+----------------+-----------+------------+----------------+----------------+
	| script-0.phtml |      5721 |      5.721 |              0 |              0 |
	| script-6.phtml |      5724 |      5.724 |              0 |              0 |
	| script-3.phtml |      5719 |      5.719 |              0 |              0 |
	| script-5.phtml |      5720 |       5.72 |              0 |              0 |
	| script-4.phtml |      5729 |      5.729 |              0 |              0 |
	| script-8.phtml |      5721 |      5.721 |              0 |              0 |
	| script-9.phtml |      5723 |      5.723 |              0 |              0 |
	| script-1.phtml |      5725 |      5.725 |              0 |              0 |
	| script-2.phtml |      5723 |      5.723 |              0 |              0 |
	| script-7.phtml |      5718 |      5.718 |              0 |              0 |
	+----------------+-----------+------------+----------------+----------------+
	10 rows in set (0.01 sec)

Timer data report (grouped by hostname,scriptname,servername and value timer tag "tag10")
	mysql> CREATE TABLE `report_host_script_server_tag10` (
			  `host` varchar(64) NOT NULL,
			  `script` varchar(64) NOT NULL,
			  `server` varchar(64) NOT NULL,
			  `tag10` varchar(64) NOT NULL,
			  `req_count` int(10) unsigned NOT NULL,
			  `hit_count` int(10) unsigned NOT NULL,
			  `time_total` double NOT NULL,
			  `ru_utime_total` double NOT NULL,
			  `ru_stime_total` double NOT NULL
			) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/timer/60/~host,~script,~server,tag10/no_percentiles/no_filters'

	mysql> select * from report_host_script_server_tag10;
	+-----------+----------------+-------------+-----------+-----------+-----------+------------+----------------+----------------+
	| host      | script         | server      | tag10     | req_count | hit_count | time_total | ru_utime_total | ru_stime_total |
	+-----------+----------------+-------------+-----------+-----------+-----------+------------+----------------+----------------+
	| localhost | script-3.phtml | antoxa-test | select    |       806 |       806 |      5.642 |              0 |              0 |
	| localhost | script-6.phtml | antoxa-test | select    |       805 |       805 |      5.635 |              0 |              0 |
	| localhost | script-0.phtml | antoxa-test | something |       800 |       800 |         12 |              0 |              0 |
	| localhost | script-1.phtml | antoxa-test | select    |       804 |       804 |      5.628 |              0 |              0 |
	| localhost | script-2.phtml | antoxa-test | something |       797 |       797 |     11.955 |              0 |              0 |
	| localhost | script-8.phtml | antoxa-test | select    |       803 |       803 |      5.621 |              0 |              0 |
	| localhost | script-6.phtml | antoxa-test | something |       805 |       805 |     12.075 |              0 |              0 |
	| localhost | script-4.phtml | antoxa-test | select    |       798 |       798 |      5.586 |              0 |              0 |
	| localhost | script-4.phtml | antoxa-test | something |       798 |       798 |      11.97 |              0 |              0 |
	| localhost | script-3.phtml | antoxa-test | something |       806 |       806 |      12.09 |              0 |              0 |
	| localhost | script-1.phtml | antoxa-test | something |       804 |       804 |      12.06 |              0 |              0 |
	| localhost | script-2.phtml | antoxa-test | select    |       797 |       797 |      5.579 |              0 |              0 |
	| localhost | script-9.phtml | antoxa-test | something |       806 |       806 |      12.09 |              0 |              0 |
	| localhost | script-7.phtml | antoxa-test | select    |       801 |       801 |      5.607 |              0 |              0 |
	| localhost | script-5.phtml | antoxa-test | select    |       802 |       802 |      5.614 |              0 |              0 |
	| localhost | script-5.phtml | antoxa-test | something |       802 |       802 |      12.03 |              0 |              0 |
	| localhost | script-9.phtml | antoxa-test | select    |       806 |       806 |      5.642 |              0 |              0 |
	| localhost | script-0.phtml | antoxa-test | select    |       800 |       800 |        5.6 |              0 |              0 |
	| localhost | script-8.phtml | antoxa-test | something |       803 |       803 |     12.045 |              0 |              0 |
	| localhost | script-7.phtml | antoxa-test | something |       801 |       801 |     12.015 |              0 |              0 |
	+-----------+----------------+-------------+-----------+-----------+-----------+------------+----------------+----------------+


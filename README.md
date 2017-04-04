# Pinba2
An attempt to rethink internal implementation and some features of excellent https://github.com/tony2001/pinba_engine by @tony2001.

# Docker
- [Fedora 25](docker/fedora-25/) (kinda works)
- [Mariadb/debian](docker/debian-mariadb/) (unfinished)

# Building
requires
- gcc 4.9.4+ (but will increase this requirement to something like gcc6+ soon)
- meow: https://github.com/anton-povarov/meow
- boost: http://boost.org/ (or just install from packages for your distro)
- nanomsg: http://nanomsg.org/ (or https://github.com/nanomsg/nanomsg/releases, or just pull master)

To build, run

    $ ./buildconf.sh

    $ ./configure
        --prefix=<path>
        --with-mysql=<path to mysql source code (you *MUST* have run configure in there first)>
        --with-nanomsg=<nanomsg install dir>
        --with-meow=<path>
        --with-boost=<path (need headers only)>

# Installation
Unfinished, use containers or
- copy mysql_engine/.libs/libpinba_engine2.so to mysql plugin directory
- install plugin
- create database pinba
- create default tables + reports

Something like this

	$ rsync -av mysql_engine/.libs/libpinba_engine2.so `mysql_config --plugindir`
	$ echo "install plugin pinba soname 'libpinba_engine2.so';" | mysql
	$ (maybe change my.cnf here, restart server if you have made changes)
	$ echo "create database pinba;" | mysql
	$ cat scripts/default_tables.sql | mysql
	$ cat scripts/default_reports.sql | mysql


# SQL
All pinba tables are created with sql comment to tell the engine about table purpose and structure,
general syntax for comment is as follows (not all reports use all the fields).

	> COMMENT='v2/<report_type>/<aggregation_window>/<keys>/<histogram+percentiles>/<filters>';

- &lt;aggregation_window&gt;: time window we aggregate data in. values are
	- 'default_history_time' to use global setting (= 60 seconds)
	- (number of seconds) - whatever you want >0
- &lt;keys&gt;: keys we aggregate incoming data on
	- 'no_keys': key based aggregation not needed / not supported (packet report only)
	- &lt;key_spec&gt;[,&lt;key_spec&gt;[,...]]
		- ~field_name: any of 'host', 'script', 'server', 'schema'
		- +request_tag_name: use this request tag's value as key
		- timer_tag_name: use this timer tag's value as key (timer reports only)
	- example: '~host,~script,~server,group,server'
		- will aggregate on 5 keys
		- 'hostname', 'scriptname', 'servername' global fields, plus 'group' and 'server' timer tag values
- &lt;histogram+percentiles&gt;: histogram time and percentiles definition
	- 'no_percentiles': disable
	- syntax: 'hv=&lt;min_time_ms&gt;:&lt;max_time_ms&gt;:&lt;bucket_count&gt;,&lt;percentiles&gt;'
		- &lt;percentiles&gt;=p&lt;number&gt;[,p&lt;number&gt;[...]]
	- example: 'hv=0:2000:20000,p99,p100'
		- this uses histogram for time range [0,2000) millseconds, with 20000 buckets, so each bucket is 0.1 ms 'wide'
		- also adds 2 percentiles to report 99th and 100th, percentile calculation precision is 0.1ms given above
		- report uses 'request_time' from incoming packets for percentiles calculation
- &lt;filters&gt;: accept only packets maching these filters into this report
	- to disable: put 'no_filters' here, report will accept all packets
	- any of (separate with commas):
		- 'min_time=&lt;milliseconds&gt;'
		- 'max_time=&lt;milliseconds&gt;'
		- '&lt;tag_spec&gt;=<value&gt;' - check that packet has tags with given values and accept only those


**Stats report (unfinished, might replace with status variables)**

This table contains internal stats, useful for monitoring/debugging/performance tuning.

Table comment syntax

	> 'v2/stats'

example

	mysql> CREATE TABLE if not exists `stats` (
			  `uptime` double NOT NULL,
			  `udp_poll_total` bigint(20) unsigned NOT NULL,
			  `udp_recv_total` bigint(20) unsigned NOT NULL,
			  `udp_recv_eagain` bigint(20) unsigned NOT NULL,
			  `udp_recv_bytes` bigint(20) unsigned NOT NULL,
			  `udp_packets_received` bigint(20) unsigned NOT NULL,
			  `udp_packets_decode_err` bigint(20) unsigned NOT NULL,
			  `udp_batches` bigint(20) unsigned NOT NULL,
			  `udp_batches_lost` bigint(20) unsigned NOT NULL
			) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/stats';

	mysql> select uptime, udp_recv_total, udp_recv_bytes, udp_packets_received, udp_packets_decode_err, udp_recv_bytes/uptime as bytes_per_sec, udp_packets_received/uptime as packets_per_sec, udp_recv_bytes/udp_packets_received as bytes_per_packet from stats;
	+----------------+----------------+----------------+----------------------+------------------------+-------------------+-------------------+------------------+
	| uptime         | udp_recv_total | udp_recv_bytes | udp_recv_packets | udp_packets_decode_err | bytes_per_sec     | packets_per_sec   | bytes_per_packet |
	+----------------+----------------+----------------+----------------------+------------------------+-------------------+-------------------+------------------+
	| 2920.189254705 |        2982158 |    21280114623 |         23070611 |                      0 | 7287238.177701511 | 7900.382128599611 |         922.3906 |
	+----------------+----------------+----------------+----------------------+------------------------+-------------------+-------------------+------------------+
	1 row in set (0.00 sec)

**Active reports (unfinished)**

This table lists all reports/tables known to the engine with additional information about them.

| Field  | Description |
|:------ |:----------- |
| table_name | mysql fully qualified table name (including database) |
| internal_name | the name known to the engine (it never changes with table renames, but you shouldn't really care about that). |
| kind | internal report type (this will become a string at some point :) ) |
| needs_engine | if 0 - this table is just known to mysql, and has no underlying report inside pinba engine |
| is_active | has this report been activated (i.e. are we gathering/aggregating data) |
| time_window | (not implemented yet) time window this reports aggregates data for (that you specify when creating a table) |
| tick_count | (not implemented yet) number of ticks, time_window is split into |
| row_count | (not implemented yet) approximate row count (counting this requires is roughly equivalent to select count(*) from 'some_report_table' ) |

Table comment syntax

	> 'v2/active'

example

	mysql> CREATE TABLE `active_reports` (
			  `table_name` varchar(128) NOT NULL,
			  `internal_name` varchar(128) NOT NULL,
			  `kind` int(10) unsigned NOT NULL,
			  `needs_engine` tinyint(1) NOT NULL,
			  `is_active` tinyint(1) NOT NULL
			) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/active'

	mysql> select * from active_reports;
	+-----------------------------------------+-----------------------------------------+------+--------------+-----------+
	| table_name                              | internal_name                           | kind | needs_engine | is_active |
	+-----------------------------------------+-----------------------------------------+------+--------------+-----------+
	| ./pinba/stats                           | <virtual table: 0>                      |    0 |            0 |         0 |
	| ./pinba/report_host_script_server_tag10 | ./pinba/report_host_script_server_tag10 |    3 |            1 |         0 |
	| ./pinba/report_by_script_name           | ./pinba/report_by_script_name           |    2 |            1 |         1 |
	| ./pinba/active_reports                  | <virtual table: 1>                      |    1 |            0 |         0 |
	+-----------------------------------------+-----------------------------------------+------+--------------+-----------+
	4 rows in set (0.00 sec)

**Packet reports (like info in tony2001/pinba_engine)**

General information about incoming packets

Table comment syntax

	> 'v2/packet/<aggregation_window>/no_keys/<histogram+percentiles>/<filters>';

example

	mysql> CREATE TABLE `info` (
			  `req_count` bigint(20) unsigned NOT NULL,
			  `timer_count` bigint(20) unsigned NOT NULL,
			  `time_total` double NOT NULL,
			  `ru_utime_total` double NOT NULL,
			  `ru_stime_total` double NOT NULL,
			  `traffic_kb` bigint(20) unsigned NOT NULL,
			  `memory_usage` bigint(20) unsigned NOT NULL
			) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/packet/default_history_time/no_keys/no_percentiles/no_filters'

	mysql> select * from info;
	+-----------+-------------+------------+----------------+----------------+------------+--------------+
	| req_count | timer_count | time_total | ru_utime_total | ru_stime_total | traffic_kb | memory_usage |
	+-----------+-------------+------------+----------------+----------------+------------+--------------+
	|    254210 |      508420 |     254.21 |              0 |              0 |          0 |            0 |
	+-----------+-------------+------------+----------------+----------------+------------+--------------+
	1 row in set (0.00 sec)


**Request data report**

Table comment syntax

	> 'v2/packet/<aggregation_window>/<key_spec>/<histogram+percentiles>/<filters>';

example (report by script name only here)

	mysql> CREATE TABLE `report_by_script_name` (
			  `script` varchar(64) NOT NULL,
			  `req_count` int(10) unsigned NOT NULL,
			  `req_per_sec` double NOT NULL,
			  `req_time_total` double NOT NULL,
			  `req_time_per_sec` double NOT NULL,
			  `ru_utime_total` double NOT NULL,
			  `ru_utime_per_sec` double NOT NULL,
			  `ru_stime_total` double NOT NULL,
			  `ru_stime_per_sec` double NOT NULL,
			  `traffic_total` bigint(20) unsigned NOT NULL,
			  `traffic_per_sec` double NOT NULL,
			  `memory_footprint` bigint(20) NOT NULL
  			) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/request/60/~script/no_percentiles/no_filters';

	mysql> select * from report_by_script_name; -- skipped some fields for brevity
	+----------------+-----------+-------------+----------------+------------------+----------------+------------------+-----------------+------------------+
	| script         | req_count | req_per_sec | req_time_total | req_time_per_sec | ru_utime_total | ru_stime_per_sec | traffic_per_sec | memory_footprint |
	+----------------+-----------+-------------+----------------+------------------+----------------+------------------+-----------------+------------------+
	| script-0.phtml |    200001 |     3333.35 |        200.001 |          3.33335 |              0 |                0 |               0 |                0 |
	| script-6.phtml |    200000 |     3333.33 |            200 |          3.33333 |              0 |                0 |               0 |                0 |
	| script-3.phtml |    200000 |     3333.33 |            200 |          3.33333 |              0 |                0 |               0 |                0 |
	| script-5.phtml |    200000 |     3333.33 |            200 |          3.33333 |              0 |                0 |               0 |                0 |
	| script-4.phtml |    200000 |     3333.33 |            200 |          3.33333 |              0 |                0 |               0 |                0 |
	| script-8.phtml |    200000 |     3333.33 |            200 |          3.33333 |              0 |                0 |               0 |                0 |
	| script-9.phtml |    200000 |     3333.33 |            200 |          3.33333 |              0 |                0 |               0 |                0 |
	| script-1.phtml |    200001 |     3333.35 |        200.001 |          3.33335 |              0 |                0 |               0 |                0 |
	| script-2.phtml |    200000 |     3333.33 |            200 |          3.33333 |              0 |                0 |               0 |                0 |
	| script-7.phtml |    200000 |     3333.33 |            200 |          3.33333 |              0 |                0 |               0 |                0 |
	+----------------+-----------+-------------+----------------+------------------+----------------+------------------+-----------------+------------------+
	10 rows in set (0.00 sec)

**Timer data report**

Table comment syntax

	> 'v2/packet/<aggregation_window>/<key_spec>/<histogram+percentiles>/<filters>';

example (grouped by hostname,scriptname,servername and value timer tag "tag10")

	mysql> CREATE TABLE `report_host_script_server_tag10` (
			  `host` varchar(64) NOT NULL,
			  `script` varchar(64) NOT NULL,
			  `server` varchar(64) NOT NULL,
			  `tag10` varchar(64) NOT NULL,
			  `req_count` int(10) unsigned NOT NULL,
			  `req_per_sec` float NOT NULL,
			  `hit_count` int(10) unsigned NOT NULL,
			  `hit_per_sec` float NOT NULL,
			  `time_total` float NOT NULL,
			  `time_per_sec` float NOT NULL,
			  `ru_utime_total` float NOT NULL,
			  `ru_utime_per_sec` float NOT NULL,
			  `ru_stime_total` float NOT NULL,
			  `ru_stime_per_sec` float NOT NULL
			) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/timer/60/~host,~script,~server,tag10/no_percentiles/no_filters';

	mysql> select * from report_host_script_server_tag10; -- skipped some fields for brevity
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

**Status Variables**

The upside for using this, instead of separate (kinda sorta built-in) table is that these are easier to immplement and support :)
But not by much.
The downside - it's ugly in selects (to calculate something like packets/sec).

Example (var combo)

	mysql> select
		(select VARIABLE_VALUE from information_schema.global_status where VARIABLE_NAME='PINBA_UDP_RECV_PACKETS')
		/ (select VARIABLE_VALUE from information_schema.global_status where VARIABLE_NAME='PINBA_UPTIME')
		as packets_per_sec;
	+-------------------+
	| packets_per_sec   |
	+-------------------+
	| 54239.48988125529 |
	+-------------------+
	1 row in set (0.00 sec)


Example (all vars)

	mysql> show status where Variable_name like 'Pinba%';
	+------------------------------------+-----------+
	| Variable_name                      | Value     |
	+------------------------------------+-----------+
	| Pinba_uptime                       | 30.312758 |
	| Pinba_udp_poll_total               | 99344     |
	| Pinba_udp_recv_total               | 227735    |
	| Pinba_udp_recv_eagain              | 99299     |
	| Pinba_udp_recv_bytes               | 367344280 |
	| Pinba_udp_recv_packets             | 1642299   |
	| Pinba_udp_packet_decode_err        | 0         |
	| Pinba_udp_batch_send_total         | 94382     |
	| Pinba_udp_batch_send_err           | 0         |
	| Pinba_udp_ru_utime                 | 24.052000 |
	| Pinba_udp_ru_stime                 | 32.820000 |
	| Pinba_repacker_poll_total          | 94711     |
	| Pinba_repacker_recv_total          | 188709    |
	| Pinba_repacker_recv_eagain         | 94327     |
	| Pinba_repacker_recv_packets        | 1642299   |
	| Pinba_repacker_packet_validate_err | 0         |
	| Pinba_repacker_batch_send_total    | 1622      |
	| Pinba_repacker_batch_send_by_timer | 189       |
	| Pinba_repacker_batch_send_by_size  | 1433      |
	| Pinba_repacker_ru_utime            | 59.148000 |
	| Pinba_repacker_ru_stime            | 23.564000 |
	| Pinba_coordinator_batches_received | 1622      |
	| Pinba_coordinator_batch_send_total | 1104      |
	| Pinba_coordinator_batch_send_err   | 0         |
	| Pinba_coordinator_control_requests | 9         |
	| Pinba_coordinator_ru_utime         | 0.040000  |
	| Pinba_coordinator_ru_stime         | 0.032000  |
	| Pinba_dictionary_size              | 364       |
	| Pinba_dictionary_mem_used          | 6303104   |
	+------------------------------------+-----------+
	29 rows in set (0.00 sec)


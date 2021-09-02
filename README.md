Pinba2
======

An attempt to rethink internal implementation and some features of excellent https://github.com/tony2001/pinba_engine by @tony2001.


Pinba (PHP Is Not A Bottleneck Anymore) is a statistics server using MySQL as an interface.

It accumulates and processes data sent over UDP and displays statistics in human-readable form of simple "reports" (like what are my slowest scripts or sql queries).
This is not limited to PHP, there are clients for multiple languages and nginx module.


Key differences from original implementation
--------------------------------------------

- no raw data tables (i.e. requests, timers) support, yet (can be implemented)
    - raw data tables have VERY high memory usage requirements and uses are limited
- simpler, more flexible report configuration
    - all use cases from original pinba are covered by only 3 kinds of reports (of which you mostly need one: timer)
    - simple aggregation keys specification, can mix different types, i.e. ~script,~server,+request_tag,@timer_tag
        - supports 15 keys max at the moment (never seen anyone using more than 5 anyway)
        - performance does not degrade when adding more keys to reports
    - more options can be configured per report now
        - stats gathering history: i.e. some reports can aggregate over 60sec, while others - over 300sec, as needed
        - histograms+percentiles: some reports might need very detailed histograms, while others - coarse
- simpler to maintain
    - no 'pools' to configure, aka no re-configuration is required when traffic grows
    - no limits on tag name/value sizes (but keep it reasonable)
- aggregation performance improved, reduced cpu/memory usage
    - currently handles ~72k simple packets/sec (~200mbps) with 5 medium-complexity reports (4 keys aggregation) @ ~40% overall cpu usage
    - handles up to 1.4 million packets/sec (~3 gbps) on internal setups on commodity hardware from 2015
    - uses significantly less memory (orders of magnitude) for common cases, since we don't store raw requests by default
    - current goal is to be able to handle 10gpbs of incoming traffic with hundreds of reports
- select performance - might be slower
  - selects from complex reports never slow down new data aggregation
  - selects in general will be slower for complex reports with thousands of rows and high percision percentiles
    - select * from 30k rows report without percentiles takes at least ~200 milliseconds or so
    - with percentiles (say histogram with 10k entries) - will add ~300ms to that
- misc
  - traffic and memory_footprint are measured in bytes (original pinba truncates to kilobytes)
  - raw histogram data is available as an extra field in existing report (not as a separate table)


Client libraries
----------------

Same client libraries can be used with this pinba implementation

list from http://pinba.org/

- PHP: https://github.com/tony2001/pinba_extension
- Nginx: https://github.com/tony2001/ngx_http_pinba_module
- NodeJS: https://github.com/Sannis/node-pinba
- Ruby: https://github.com/prepor/pinbo
- Python: https://github.com/IsCoolEntertainment/pynba
- Java: https://github.com/alex-krash/jpinba
- Go: https://github.com/mkevac/gopinba


Migrating from original Pinba
-----------------------------

We've got some scripts to help [in scripts directory](scripts).
Convert mysqldump of your old tables to new format with [this script](scripts/convert_mysqldump.php).



More Info
--------

- [TODO](TODO.md)
- [Building](docs/index.md#building) - use docker, really
- [Installing](docs/index.md#installation) - use docker, really
- [Configuration](docs/index.md#configuration) - optional, should run fine with default settings
- [User-defined reports + examples](#user-defined-reports)


Docker
------

### Fedora 25

[`Dockerfile`](Dockerfile)

Basics
------

**Requests**

We get these over UDP, each request contains metrics data gathered by your application (like serving pages to users, or performing db queries).


Data comes in three forms

- **request fields** (these are predefined and hardcoded since the dawn of original pinba)
    - `host_name`: name of the physical host (like "subdomain.mycoolserver.localnetwork")
    - `script_name`: name of the script
    - `server_name`: name of the logical host (like "example.com")
    - `schema`: usually "http" or "https"
    - `status`: usually http status (this one is 32-bit integer)
    - `request_time`: wall-clock time it took to execute the whole request
    - `rusage_user`: rusage in user space for this request
    - `rusage_system`: rusage in kernel space for this request
    - `document_size`: size of result doc
    - `memory_footprint`: amount of memory used
- **request tags** - this is just a bunch of `key -> value` pairs attached to request as a whole
    - ex. in pseudocode `[ 'application' -> 'my_cool_app', 'environment' -> 'production' ]`
- **timers** - a bunch is sub-action measurements, for example: time it took to execute some db query, or process some user input.
    - number of timers is not limited, track all db/memcached queries
    - each timer can also have tags!
        - ex. `[ 'group' -> 'db', 'server' -> 'db1.lan', 'op_type' -> 'update' ]`
        - ex. `[ 'group' -> 'memcache', 'server' -> 'mmc1.lan', 'op_type' -> 'get' ]`


**Reports**

Report is a read-only view of incoming data, aggregated within specified time window.
One can think of it as a table of key/value pairs: `Aggregation_key value` -> `Aggregated_data` + `Percentiles`.

- `Aggregation_key` - configured when report is created.
    - key *names* are set by the user, *values* for those keys are taken from requests for aggregation
    - key *name* is a combination of
        - request fields: `~host`, `~script`, `~server`, `~schema`, `~status`
        - request tags: `+whatever_name_you_want`
        - timer tags: `@some_timer_tag_name`
- `Aggregation_key value` - is the set of values, corresponding to key names set in `Aggregation_key`
    - ex. if `Aggregation_key` is `~host`, there'll be a key/value pair per unique `host` we see in request stream
    - ex. if `Aggregation_key` is `~host,+req_tag`, there'll be a key/value pair per unique `[host, req_tag_value]` pair
- `Aggregated_data` is report-specific (i.e. a structure with fields like: req_count, hit_count, total_time, etc.).
- `Percentiles` is a bunch of fields with specific percentiles, calculated over data from `request_time` or `timer_value`
- `Histogram` is a field where engine exports raw histogram data (that we calculate percentiles from) in text form

There are 3 kinds of reports: packet, request, timer. The difference between those boils down to

- How `Aggregation_key values`-s are extracted and matched
- How `Aggregated_data` is populated (i.e. if you aggregate on request tags, there is no need/way to aggregate timer data)
- What value we use for `Histogram` and `Percentiles`


**SQL tables**

Reports are exposed to the user as SQL tables.

All report tables have same simple structure

- `Aggregation_key`, one table field per key part (i.e. ~script,~host,@timer_tag needs 3 fields with appropriate types)
- `Aggregated_data`, 3 fields per data field (field_value, field_value_per_sec, field_value_percent) (i.e. request report needs 7*3 fields = 21 data fields)
- `Percentiles`, one field per configured percentile (optional)
- `Histogram`, one text field for raw histogram data that percentiles are calculated from (optional)

ASCII art!

                              ----------------           -------------------------------------------------------------
                              | key -> value |           | key_part_1 | ... | data_part_1 | ... | percentile_1 | ... |
    ------------              ----------------           -------------------------------------------------------------
    | Requests |  aggregate>  |  .........   |  select>  |    ...................................................    |
    ------------              ----------------           -------------------------------------------------------------
                              | key -> value |           | key_part_1 | ... | data_part_1 | ... | percentile_1 | ... |
                              ----------------           -------------------------------------------------------------


**SQL table comments**

All pinba tables are created with sql comment to tell the engine about table purpose and structure,
general syntax for comment is as follows (not all reports use all the fields).

    > COMMENT='v2/<report_type>/<aggregation_window>/<keys>/<histogram+percentiles>/<filters>';

[Take a look at examples first](#user-defined-reports)

- &lt;aggregation_window&gt;: time window we aggregate data in. values are
    - 'default_history_time' to use global setting (= 60 seconds)
    - (number of seconds) - whatever you want >0
- &lt;keys&gt;: keys we aggregate incoming data on
    - 'no_keys': key based aggregation not needed / not supported (packet report only)
    - &lt;key_spec&gt;[,&lt;key_spec&gt;[,...]]
        - ~field_name: any of 'host', 'script', 'server', 'schema'
        - +request_tag_name: use this request tag's value as key
        - @timer_tag_name: use this timer tag's value as key (timer reports only)
    - example: '~host,~script,+application,@group,@server'
        - will aggregate on 5 keys
        - 'host_name', 'script_name' global fields, 'application' request tag, plus 'group' and 'server' timer tag values
- &lt;histogram+percentiles&gt;: histogram time and percentiles definition
    - 'no_percentiles': disable
    - syntax: 'hv=&lt;min_time_ms&gt;:&lt;max_time_ms&gt;:&lt;bucket_count&gt;,&lt;percentiles&gt;'
        - &lt;percentiles&gt;=p&lt;double&gt;[,p&lt;double&gt;[...]]
        - (alt syntax) &lt;percentiles&gt;='percentiles='&lt;double&gt;[:&lt;double&gt;[...]]
    - example: 'hv=0:2000:20000,p99,p99.9,p100'
        - this uses histogram for time range [0,2000) millseconds, with 20000 buckets, so each bucket is 0.1 ms 'wide'
        - also adds 3 percentiles to report 99th, 99.9th and 100th, percentile calculation precision is 0.1ms given above
        - uses 'request_time' (for packet/request reports) or 'timer_value' (for timer reports) from incoming packets for percentiles calculation
    - example (alt syntax): 'hv=0:2000:20000,percentiles=99:99.9:100'
        - same effect as above
- &lt;filters&gt;: accept only packets maching these filters into this report
    - to disable: put 'no_filters' here, report will accept all packets
    - any of (separate with commas):
        - 'min_time=&lt;milliseconds&gt;'
        - 'max_time=&lt;milliseconds&gt;'
        - '&lt;tag_spec&gt;=&lt;value&gt;' - check that packet has fields, request or timer tags with given values and accept only those
    - &lt;tag_spec&gt; is the same as &lt;key_spec&gt; above, i.e. ~request_field,+request_tag,@timer_tag
    - example: min_time=0,max_time=1000,+browser=chrome
        - will accept only requests with request_time in range [0, 1000)ms with request tag 'browser' present and value 'chrome'
        - there is currently no way to filter timers by their timer_value, can't think of a use case really


User-defined reports
--------------------

**Packet report (like info in tony2001/pinba_engine)**

General information about incoming packets

- just aggregates everything into single item (mostly used to gauge general traffic)
- Aggregation_key is always empty
- Aggregated_data is global packet totals: { req_count, timer_count, hit_count, total_time, ru_utime, ru_stime, traffic, memory_footprint }
- Histogram and Percentiles are calculated from data in request_time field

Table comment syntax

    > 'v2/packet/<aggregation_window>/no_keys/<histogram+percentiles>/<filters>';

Example

```sql
mysql> CREATE TABLE `info` (
      `req_count` bigint(20) unsigned NOT NULL,
      `timer_count` bigint(20) unsigned NOT NULL,
      `time_total` double NOT NULL,
      `ru_utime_total` double NOT NULL,
      `ru_stime_total` double NOT NULL,
      `traffic` bigint(20) unsigned NOT NULL,
      `memory_footprint` bigint(20) unsigned NOT NULL
    ) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/packet/default_history_time/no_keys/no_percentiles/no_filters'

mysql> select * from info;
+-----------+-------------+-------------------+------------------+-----------------+-----------+------------------+
| req_count | timer_count | time_total        | ru_utime_total   | ru_stime_total  | traffic   | memory_footprint |
+-----------+-------------+-------------------+------------------+-----------------+-----------+------------------+
|   3940547 |    59017168 | 6982620.849607239 | 128279.101920963 | 18963.268457099 | 141734072 |  317514981871616 |
+-----------+-------------+-------------------+------------------+-----------------+-----------+------------------+
1 row in set (0.00 sec)
```


**Request data report**

- aggregates at request level, never touching timers at all
- Aggregation_key is a combination of request_field (host, script, etc.) and request_tags (must NOT have timer_tag keys)
- Aggregated_data is request-based
    - req_count, req_time_total, req_ru_utime, req_ru_stime, traffic_kb, mem_usage
- Histogram and Percentiles are calculated from data in request_time field

Table comment syntax

    > 'v2/packet/<aggregation_window>/<key_spec>/<histogram+percentiles>/<filters>';

example (report by script name only here)

```sql
mysql> CREATE TABLE `report_by_script_name` (
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
```


**Timer data report**

This is the one you need for 95% uses

- aggregates at request + timer levels
- Aggregation_key is a combination of request_field (host, script, etc.), request_tags and timer_tags (must have at least one timer_tag key)
- Aggregated_data is timer-based (aka taken from timer data)
    - req_count, timer_hit_count, timer_time_total, timer_ru_utime, timer_ru_stime
- Histogram and Percentiles are calculated from data in timer_value

Table comment syntax

    > 'v2/packet/<aggregation_window>/<key_spec>/<histogram+percentiles>/<filters>';

example (some complex report)

```sql
mysql> CREATE TABLE `tag_info_pinger_call_from_wwwbmamlan` (
      `pinger_dst_cluster` varchar(64) NOT NULL,
      `pinger_src_host` varchar(64) NOT NULL,
      `pinger_dst_host` varchar(64) NOT NULL,
      `req_count` int(11) NOT NULL,
      `req_per_sec` float NOT NULL,
      `req_percent` float,
      `hit_count` int(11) NOT NULL,
      `hit_per_sec` float NOT NULL,
      `hit_percent` float,
      `time_total` float NOT NULL,
      `time_per_sec` float NOT NULL,
      `time_percent` float,
      `ru_utime_total` float NOT NULL,
      `ru_utime_per_sec` float NOT NULL,
      `ru_utime_percent` float,
      `ru_stime_total` float NOT NULL,
      `ru_stime_per_sec` float NOT NULL,
      `ru_stime_percent` float,
      `p50` float NOT NULL,
      `p75` float NOT NULL,
      `p95` float NOT NULL,
      `p99` float NOT NULL,
      `p100` float NOT NULL,
      `histogram_data` text NOT NULL
    ) ENGINE=PINBA DEFAULT CHARSET=latin1
      COMMENT='v2/timer/60/@pinger_dst_cluster,@pinger_src_host,@pinger_dst_host/hv=0:1000:100000,p50,p75,p95,p99,p100/+pinger_phase=call,+pinger_src_cluster=wwwbma.mlan';
```

example (grouped by host_name, script_name, server_name and value timer tag "tag10")

```sql
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
    ) ENGINE=PINBA DEFAULT CHARSET=latin1
      COMMENT='v2/timer/60/~host,~script,~server,@tag10/no_percentiles/no_filters';

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
```


System Reports
--------------

**Active reports information table**

This table lists all reports known to the engine with additional information about them.

| Field  | Description |
|:------ |:----------- |
| id | internal id, useful for matching reports with system threads. report calls pthread_setname_np("rh/[id]") |
| table_name | mysql fully qualified table name (including database) |
| internal_name | the name known to the engine (it never changes with table renames, but you shouldn't really care about that). |
| kind | internal report kind (one of the kinds described in this doc, like stats, active, etc.) |
| uptime | time since report creation (seconds) |
| time_window | time window this reports aggregates data for (that you specify when creating a table) |
| tick_count | number of ticks, time_window is split into |
| approx_row_count | approximate row count |
| approx_mem_used | approximate memory usage |
| batches_sent | number of packet batches sent from coordinator to report thread |
| batches_received | number of packet batches received by report thread (if you have != 0 here, you're losing batches and packets) |
| packets_received | packets received and processed |
| packets_lost | packets that could not be processed and had to be dropped (aka, report couldn't cope with such packet rate) |
| packets_aggregated | number of packets that we took useful information from |
| packets_dropped_by_bloom | number of packets dropped by packet-level bloom filter |
| packets_dropped_by_filters | number of packets dropped by packet-level filters |
| packets_dropped_by_rfield | number of packets dropped by request_field aggregation |
| packets_dropped_by_rtag | number of packets dropped by request_tag aggregation |
| packets_dropped_by_timertag | number of packets dropped by timer_tag aggregation (i.e. no useful timers) |
| timers_scanned | number of timers scanned |
| timers_aggregated | number of timers that we took useful information from |
| timers_skipped_by_bloom | number of timers skipped by timer-level bloom filter |
| timers_skipped_by_filters | number of timers skipped by timertag filters |
| timers_skipped_by_tags | number of timers skipped by not having required tags present |
| ru_utime | rusage: user time |
| ru_stime | rusage: system time |
| last_tick_time | time we last merged temporary data to selectable data |
| last_tick_prepare_duration | time it took to prepare to merge temp data to selectable data |
| last_snapshot_merge_duration | time it took to prepare last select (not implemented yet) |

Table comment syntax

    > 'v2/active'

example

```sql
mysql> CREATE TABLE IF NOT EXISTS `pinba`.`active` (
      `id` int(10) unsigned NOT NULL,
      `table_name` varchar(128) NOT NULL,
      `internal_name` varchar(128) NOT NULL,
      `kind` varchar(64) NOT NULL,
      `uptime` double unsigned NOT NULL,
      `time_window_sec` int(10) unsigned NOT NULL,
      `tick_count` int(10) NOT NULL,
      `approx_row_count` int(10) unsigned NOT NULL,
      `approx_mem_used` bigint(20) unsigned NOT NULL,
      `batches_sent` bigint(20) unsigned NOT NULL,
      `batches_received` bigint(20) unsigned NOT NULL,
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
      `timers_skipped_by_bloom` bigint(20) unsigned NOT NULL,
      `timers_skipped_by_filters` bigint(20) unsigned NOT NULL,
      `timers_skipped_by_tags` bigint(20) unsigned NOT NULL,
      `ru_utime` double NOT NULL,
      `ru_stime` double NOT NULL,
      `last_tick_time` double NOT NULL,
      `last_tick_prepare_duration` double NOT NULL,
      `last_snapshot_merge_duration` double NOT NULL
    ) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/active';


mysql> select *, packets_received/uptime as packets_per_sec, timers_scanned/uptime as timers_per_sec, ru_utime/uptime utime_per_sec from active\G
*************************** 1. row ***************************
                          id: 1
                  table_name: ./pinba/tag_report_perf___10us
               internal_name: ./pinba/tag_report_perf___10us
                        kind: report_by_timer_data
                      uptime: 2316.135996475
             time_window_sec: 60
                  tick_count: 60
            approx_row_count: 10830
             approx_mem_used: 117561688
                batches_sent: 185186
            batches_received: 185186
            packets_received: 38144533
                packets_lost: 0
          packets_aggregated: 6634543
    packets_dropped_by_bloom: 31509990
  packets_dropped_by_filters: 0
   packets_dropped_by_rfield: 0
     packets_dropped_by_rtag: 0
 packets_dropped_by_timertag: 0
              timers_scanned: 1455859105
           timers_aggregated: 1097993658
     timers_skipped_by_bloom: 357865447
   timers_skipped_by_filters: 0
      timers_skipped_by_tags: 0
                    ru_utime: 182.947086
                    ru_stime: 4.105066
              last_tick_time: 1525363484.9716723
  last_tick_prepare_duration: 0.006995009000000001
last_snapshot_merge_duration: 0.000000266
             packets_per_sec: 16469.038544391762      // 16.5k packets/sec
              timers_per_sec: 628896.7355846566       // 628k timers/sec, ~38 timers/packet
               utime_per_sec: 0.0789880586798154      // at ~8% cpu!
1 row in set (0.01 sec)
```


**Stats (see also: status variables)**

This table contains internal stats, useful for monitoring/debugging/performance tuning.

Table comment syntax

    > 'v2/stats'

example

```sql
mysql> CREATE TABLE IF NOT EXISTS `stats` (
      `uptime` DOUBLE NOT NULL,
      `ru_utime` DOUBLE NOT NULL,
      `ru_stime` DOUBLE NOT NULL,
      `udp_poll_total` BIGINT(20) UNSIGNED NOT NULL,
      `udp_recv_total` BIGINT(20) UNSIGNED NOT NULL,
      `udp_recv_eagain` BIGINT(20) UNSIGNED NOT NULL,
      `udp_recv_bytes` BIGINT(20) UNSIGNED NOT NULL,
      `udp_recv_packets` BIGINT(20) UNSIGNED NOT NULL,
      `udp_packet_decode_err` BIGINT(20) UNSIGNED NOT NULL,
      `udp_batch_send_total` BIGINT(20) UNSIGNED NOT NULL,
      `udp_batch_send_err` BIGINT(20) UNSIGNED NOT NULL,
      `udp_packet_send_total` BIGINT(20) UNSIGNED NOT NULL,
      `udp_packet_send_err` BIGINT(20) UNSIGNED NOT NULL,
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
      `dictionary_mem_hash` BIGINT(20) UNSIGNED NOT NULL,
      `dictionary_mem_list` BIGINT(20) UNSIGNED NOT NULL,
      `dictionary_mem_strings` BIGINT(20) UNSIGNED NOT NULL,
      `version_info` text(1024) NOT NULL,
      `build_string` text(1024) NOT NULL
    ) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/stats';
```

```sql
mysql> select *, (repacker_ru_utime/uptime) as repacker_ru_utime_per_sec from stats\G
*************************** 1. row ***************************
                      uptime: 12.482723834
                    ru_utime: 2.248
                    ru_stime: 1.12
              udp_poll_total: 20924
              udp_recv_total: 49753
             udp_recv_eagain: 20904
              udp_recv_bytes: 192375675
            udp_recv_packets: 870451
       udp_packet_decode_err: 0
        udp_batch_send_total: 20915
          udp_batch_send_err: 0
       udp_packet_send_total: 870451
         udp_packet_send_err: 0
                udp_ru_utime: 0.8680000000000001
                udp_ru_stime: 0.8240000000000001
         repacker_poll_total: 20948
         repacker_recv_total: 41827
        repacker_recv_eagain: 20912
       repacker_recv_packets: 870451
repacker_packet_validate_err: 0
   repacker_batch_send_total: 849
repacker_batch_send_by_timer: 0
 repacker_batch_send_by_size: 849
           repacker_ru_utime: 1.1720000000000002
           repacker_ru_stime: 0.07200000000000001
coordinator_batches_received: 849
coordinator_batch_send_total: 0
  coordinator_batch_send_err: 0
coordinator_control_requests: 0
        coordinator_ru_utime: 0.032
        coordinator_ru_stime: 0
             dictionary_size: 444
         dictionary_mem_hash: 6311251
         dictionary_mem_list: 14208
      dictionary_mem_strings: 5587
                version_info: pinba 2.0.8, git: 1afd7eb872a6ef95e34efbbe730aea3926489798, modified: 1
                build_string: whatever-string-from-configure
```


**Status Variables**

Same values as in stats table, but 'built-in' (no need to create the table), but uglier to use in selects.

Example (all vars)

```sql
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
```


Example (var combo)

```sql
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
```

Histograms and Percentiles
--------------------------

TODO (need help describing details here).

You don't need to understand this to use the engine.

For all incoming time data (request_time or timer_value) - we build a histogram representing time values distribution for each 'row' in the report.
This allows us to calculate percentiles (with some accuracy, that is given by histogram range and bucket count).

So each row in every report that has percentiles configured will have a histogram associated with it.
When selecting data from that report, the engine processes the histogram to get percentile values.

**Histogram**

config defines the range and bucket count: `hv=<min_value_ms>:<max_value_ms>:<bucket_count>`.
This defines a histogram with the following structure

```
given
  <hv_range>     = <max_value_ms> - <min_value_ms>
  <bucket_width> = <hv_range> / <bucket_count>

histogram looks like this
  [negative_infinity bucket] -> number of time values in range (-inf, <min_value_ms>]
  [0 bucket]                 -> number of time values in range (<min_value_ms>, <min_value_ms> + <bucket_width>]
  [1 bucket]                 -> number of time values in range (<min_value_ms> + <bucket_width>, <min_value_ms> + <bucket_width> * 2]
....
  [last bucket]              -> number of time values in range (<min_value_ms> + <bucket_width> * (<bucket_count> - 1), <min_value_ms> + <bucket_width> * <bucket_count>]
  [positive_infinity bucket] -> number of time values in range (<max_value_ms>, +inf)
```

**Things to know about percentile caculation**

- when percentile calculation needs to take 'partial bucket' (i.e. not all values from the bucket) - it interpolates percentile value, assuming uniform distribution within the bucket
- percentile 0   - is always equal to min_value_ms
- percentile 100 - is always equal to max_value_ms

**Raw Histogram output format**

Generic format description

```
hv=<min_value_ms>:<min_value_ms>:<bucket_count>;values=[min:<negative_inf_value>,max:<positive_inf_value>,<bucket_id>:<value>, ...]
```

Example

```
// histogram configured with
//  min_value_ms = 0
//  min_value_ms = 2000  (aka 2 seconds)
//  bucket_count = 20000 (so histogram resolution is 2000ms/20000 = 100 microseconds)
//
// negative_inf bucket contains 3 values
// positive_inf bucket contains 3 values
// and bucket with id = 69, this bucket correspods to bucket (6ms, 7ms]
//    as buckets are numbered from 0, and (69 + 1)*100microseconds = 7000microseconds = 7milliseconds
hv=0:2000:20000;values=[min:3,max:3,69:3]
```

**Percentile caculation example**

Given the histogram above, say we need to calculate percentile 50 (aka median). Aka, the value that is larger than 50% of the values in the 'value set'.
Our 'value set' is as follows

```
[ -inf, -inf, -inf, 7ms, 7ms, 7ms, +inf, +inf, +inf ]

or, transforming 'infinities' into min_value_ms and max_value_ms

[ 0ms, 0ms, 0ms, 7ms, 7ms, 7ms, 2000ms, 2000ms, 2000ms ]
```

- calculate what '50% of all values' means, got 9 values, 50% is 4.5
- round 4.5 up, take the value of 5th elt -> 7ms is the answer
- but, taking into account a point from above (we interpolate within bucket, assuming uniform distribution)
  - actually the transformed value set will look like this
  ```
  [ 0ms, 0ms, 0ms, 6.33(3)ms, 6.66(6)ms, 7ms, 2000ms, 2000ms, 2000ms ]
  ```
  since we assume uniform distribution, virtually splitting the bucket into N=3 (the number of values in a bucket) sub-buckets
- so our answer will be 6.66(6) millseconds

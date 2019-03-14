Building
--------

**examples**

See [Dockerfile](../Dockerfile) and [build-from-source.sh](../docker/build-from-source.sh).<br/>
Also there is a rough guide for centos7 in [#17](https://github.com/badoo/pinba2/issues/17) - should be workable for multiple distros really.

**requirements**

- c++14 compatible compiler (tested with gcc7+, clang7+)
- meow: https://github.com/anton-povarov/meow
- boost: http://boost.org/ (or just install from packages for your distro, need headers only)
- nanomsg: http://nanomsg.org/ (or https://github.com/nanomsg/nanomsg/releases, or just pull master)
	- build it statically with `-DCMAKE_C_FLAGS="-fPIC -DPIC"` (see build-from-source.sh for an example)
	- make sure to adjust NN_MAX_SOCKETS cmake option as it limits the number of reports available, 4096 should be enough for ~700 reports.
- mysql (5.6+) or mariadb (10+)
	- IMPORTANT: just unpacking sources is not enough, as mysql generates required headers on configure and make
	- run `cmake . && make` (going to take a while)
	- cmake might require multiple `*-devel` packages, for stuff like ncurses, openssl, install them using your distro's package manager

**To build, run**

    $ ./buildconf.sh

    $ ./configure
        --prefix=<path>
        --with-mysql=<path to configured mysql sources (built as explained above)>
        --with-nanomsg=<nanomsg install dir>
        --with-meow=<path>
        --with-boost=<path (need headers only)>

    $ make -j4


Installation
------------
Unfinished, use containers or

- copy mysql_engine/.libs/libpinba_engine2.so (make sure to copy the real .so, not the symlink) to mysql plugin directory
- install plugin
- create database pinba
- create default tables + reports

Something like this

	$ rsync -av mysql_engine/.libs/libpinba_engine2.so `mysql_config --plugindir`
	$ echo "install plugin pinba soname 'libpinba_engine2.so';" | mysql
	$ (maybe change my.cnf here, restart server if you have made changes)
	$ echo "create database pinba;" | mysql
	$ cat scripts/default_tables/*.sql | mysql
	$ cat scripts/default_reports.sql | mysql

**Jemalloc**

highly recommended to run mysql/mariadb with jemalloc enabled

- mariadb does this automatically (i think) [and has been for quite a while](https://mariadb.org/mariadb-5-5-33-now-available/)
- mysql [has \-\-malloc-lib option](https://dev.mysql.com/doc/refman/5.7/en/mysqld-safe.html#option_mysqld_safe_malloc-lib)
- percona [has an ok guide](https://www.percona.com/blog/2017/01/03/enabling-and-disabling-jemalloc-on-percona-server/)


**Compatibility**

- if mysql/mariadb are built with debug enabled (or you're using debug packages) - build with \-\-enable-debug, or stuff will not work
- make sure that you're building pinba with the same mysql/mariadb version that you're going to install built plugin into, or mysterious crashes might happen
- MARIADB: you might need to change your `plugin_maturity` setting in my.cnf to `unknown` (should be possible to get rid of this requirement, please file an issue or send PR)

Configuration
=============

These are options that go in my.cnf.<br>
You don't need to change any of these, all options have workable defaults.

## pinba_address

Address to listen for UDP packets on.<br>
Default: 0.0.0.0 (aka - all interfaces present)

## pinba_port
Port to listen for UDP packets on.<br>
Default: 3002

## pinba_log_level
Logging level, one of: debug, info, notice, warn, error, crit, alert<br>
Use 'debug' for debugging :)<br>
Use 'warn' or 'error' for production, should be enough <br>
This value can be changed at runtime with query like `set global pinba_log_level='debug';`

## pinba_default_history_time_sec
Default aggregation time_window for all reports. This value is used if you use "default_history_time" as time window in report configuration.<br>
Default: 60 (seconds)

## pinba_udp_reader_threads
Number of UDP reader threads, default is usually enough here.<br>
Default: 2<br>
Max: 16

## pinba_repacker_threads
Number of internal packet-repack threads, default is usually enough here.<br>
Try tunning higher if stats udp_batches_lost is > 0.<br>
Default: 2<br>
Max: 16

## pinba_repacker_input_buffer
Queue buffer size for udp-reader -> packet-repack threads communication.<br>
Default: 512<br>
Max: 16K

## pinba_repacker_batch_messages
Batch size for packet-repack -> reports communication.<br>
Might want to tune higher if coordinator thread rusage is too high.<br>
Default: 1024<br>
Max: 16K

## pinba_repacker_batch_timeout_ms
Max delay between packet-repack thread batches generation (in milliseconds).<br>
This setting basically puts an upper bound on how long packet can be queued before begin transferred to report for processing. Don't set below 10ms unless you know what you're doing.<br>
Default: 100 (milliseconds)<br>
Max: 1000 (milliseconds)

## pinba_coordinator_input_buffer
Queue buffer size for packet-repack -> coordinator threads communication.<br>
Default: 128<br>
Max: 1024

## pinba_report_input_buffer
Queue buffer size for coordinator -> report threads communication. This setting is per report.<br>
Default: 128<br>
Max: 8192

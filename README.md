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
			) ENGINE=PINBA DEFAULT CHARSET=latin1 COMMENT='v2/stats'

	mysql> select uptime, udp_recv_total, udp_packets_received, udp_packets_received/uptime as udp_packets_per_sec from stats;
	+--------------+----------------+----------------------+---------------------+
	| uptime       | udp_recv_total | udp_packets_received | udp_packets_per_sec |
	+--------------+----------------+----------------------+---------------------+
	| 74.323456902 |          96987 |              2000002 |    26909.4318720552 |
	+--------------+----------------+----------------------+---------------------+
	1 row in set (0.00 sec)

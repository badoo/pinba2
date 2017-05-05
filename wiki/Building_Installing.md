Building
--------

**requirements**

- gcc 4.9.4+ (but will increase this requirement to something like gcc6+ soon)
- meow: https://github.com/anton-povarov/meow
- boost: http://boost.org/ (or just install from packages for your distro)
- nanomsg: http://nanomsg.org/ (or https://github.com/nanomsg/nanomsg/releases, or just pull master)
- mysql (5.6+) or mariadb (10+)
	- mysql: need source code, run ./configure, since mysql doesn't install all internal headers we need
	- mariadb: just install from with your favorite package manager, and point pinba to header files

**To build, run**

    $ ./buildconf.sh

    $ ./configure
        --prefix=<path>
        --with-mysql=<path to configured mysql source code, or mariadb installed headers>
        --with-nanomsg=<nanomsg install dir>
        --with-meow=<path>
        --with-boost=<path (need headers only)>

Installation
------------
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

**Jemalloc**

highly recommended to run mysql/mariadb with jemalloc enabled

- mariadb does this automatically (i think) [and has been for quite a while](https://mariadb.org/mariadb-5-5-33-now-available/)
- mysql [has \-\-malloc-lib option](https://dev.mysql.com/doc/refman/5.7/en/mysqld-safe.html#option_mysqld_safe_malloc-lib)
- percona [has an ok guide](https://www.percona.com/blog/2017/01/03/enabling-and-disabling-jemalloc-on-percona-server/)


**Compatibility**

- if mysql/mariadb are built with debug enabled (or you're using debug packages) - build with \-\-enable-debug, or stuff will not work
- make sure that you're building pinba with the same mysql/mariadb version that you're going to install built plugin into, or mysterious crashes might happen

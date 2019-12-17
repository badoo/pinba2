#!/bin/bash -xe

# this process is mildly copy-pasted from here
# https://github.com/docker-library/mariadb/tree/b558f64b736650b94df9a90e68ff9e3bc03d4bdb/10.1

if [ $1 = "mysqld" ]; then

	# fedora mysqld is in special location
	ln -snf /usr/libexec/mysqld /usr/local/bin

	# disable gss auth as it's not installed in this container
	rm -rf /etc/my.cnf.d/auth_gssapi.cnf

	# create default databases
	# too expensive to perform on container startup really
	mysql_install_db --rpm
	chmod -R 777 /var/lib/mysql

	# start mysql server in background for init process
	"$@" --skip-networking -umysql &
	pid="$!"

	# legen.... wait for it
	for i in {10..0}; do
		if echo 'SELECT 1' | mysql &> /dev/null; then
			break
		fi
		echo 'MySQL init process in progress...'
		sleep 1
	done
	if [ "$i" = 0 ]; then
		echo >&2 'MySQL init process failed.'
		exit 1
	fi

	# ..dary

	# create root user with no password
	# TODO: Use --init-file https://mariadb.com/kb/en/library/server-system-variables/#init_file
	mysql --protocol=socket -uroot <<-EOSQL
			SET @@SESSION.SQL_LOG_BIN=0;
			DELETE from mysql.user;
			FLUSH PRIVILEGES;
			CREATE USER 'root'@'%' IDENTIFIED BY '${MYSQL_ROOT_PASSWORD}' ;
			GRANT ALL ON *.* TO 'root'@'%' WITH GRANT OPTION ;
			FLUSH PRIVILEGES ;
	EOSQL

	# install plugin and create default db
	# TODO: Use --init-file https://mariadb.com/kb/en/library/server-system-variables/#init_file
	mysql --protocol=socket -uroot -e 'SHOW PLUGINS' | grep pinba \
		|| mysql --protocol=socket -uroot -e "INSTALL PLUGIN pinba SONAME 'libpinba_engine2.so'"
	mysql --protocol=socket -uroot <<-EOSQL
		CREATE DATABASE IF NOT EXISTS pinba;
	EOSQL

	# TODO: create default tables from scripts/default_tables.sql
	# TODO: Use --init-file https://mariadb.com/kb/en/library/server-system-variables/#init_file
	#       need to fix install process for that

	# terminate mysql server to start it in foreground
	if ! kill -s TERM "$pid" || ! wait "$pid"; then
		echo >&2 'MySQL init process failed.'
		exit 1
	fi
fi

exec "$@" -umysql

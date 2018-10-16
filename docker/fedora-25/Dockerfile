FROM        fedora:25 as builder

RUN         dnf install -y git-core gcc gcc-c++ boost-devel cmake autoconf automake libtool mariadb-devel
RUN         dnf install -y file hostname mariadb-server

ADD         . /_src/pinba2

RUN         mkdir -p /_src && cd /_src && \
			git clone --branch master --single-branch --depth 1  https://github.com/anton-povarov/meow && \
			git clone --branch master --single-branch --depth 1 https://github.com/nanomsg/nanomsg ;

RUN         /_src/pinba2/docker/build-from-source.sh

FROM        fedora:25
MAINTAINER  Anton Povarov "anton.povarov@gmail.com"

RUN         dnf install -y file hostname mariadb-server

COPY --from=builder /usr/lib64/mysql/plugin/libpinba_engine2.so /usr/lib64/mysql/plugin/libpinba_engine2.so

COPY        docker-entrypoint.sh /usr/local/bin/
RUN         ln -snf /usr/local/bin/docker-entrypoint.sh /
ENTRYPOINT  ["docker-entrypoint.sh"]

EXPOSE      30003
CMD         ["mysqld"]

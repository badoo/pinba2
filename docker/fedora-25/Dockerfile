FROM        fedora:25
MAINTAINER  Anton Povarov "anton.povarov@gmail.com"

RUN         dnf install -y git-core gcc gcc-c++ boost-devel cmake autoconf automake libtool mariadb-devel
RUN         dnf install -y file hostname mariadb-server

RUN         mkdir -p /_src && cd /_src && \
			git clone https://github.com/anton-povarov/pinba2 && \
			git clone https://github.com/anton-povarov/meow && \
			git clone https://github.com/nanomsg/nanomsg ;

RUN         /_src/pinba2/docker/build-from-source.sh

RUN         dnf remove -y git-core gcc gcc-c++ boost-devel cmake autoconf automake libtool mariadb-devel && \
			dnf autoremove -y && \
			dnf clean all

COPY        docker-entrypoint.sh /usr/local/bin/
RUN         ln -snf /usr/local/bin/docker-entrypoint.sh /
ENTRYPOINT  ["docker-entrypoint.sh"]

EXPOSE      30003
CMD         ["mysqld"]

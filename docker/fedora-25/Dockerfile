FROM        fedora:25
# FROM        fedora-pinba-dev:latest
MAINTAINER  anton.povarov@gmail.com

RUN         dnf install -y git-core gcc gcc-c++ boost-devel cmake autoconf automake libtool mariadb-devel
RUN         dnf install -y mariadb-server

RUN         mkdir -p /_src && cd /_src && \
			git clone https://github.com/anton-povarov/pinba2 && \
			git clone https://github.com/anton-povarov/meow && \
			git clone https://github.com/sparsehash/sparsehash && \
			git clone https://github.com/nanomsg/nanomsg


COPY        build-from-source.sh /opt
RUN         cd /opt/ && chmod a+x build-from-source.sh && ./build-from-source.sh

COPY        docker-entrypoint.sh /usr/local/bin/
RUN         ln -snf /usr/local/bin/docker-entrypoint.sh /
ENTRYPOINT  ["docker-entrypoint.sh"]

EXPOSE      30003
CMD         ["mysqld"]

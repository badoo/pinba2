FROM		mariadb:10.1

MAINTAINER	Anton Povarov "anton.povarov@gmail.com"

COPY		./docker/build-from-source.sh /opt/
COPY		10-pinba-init.sh /docker-entrypoint-initdb.d/

RUN			apt-get update && \
			apt-get install -y --force-yes automake make libtool libtool-bin g++

RUN			cd /opt && ./build-from-source.sh

EXPOSE		30003

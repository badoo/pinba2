FROM fedora:25 as builder

RUN dnf install -y \
    git-core \
    gcc \
    gcc-c++ \
    boost-devel \
    cmake \
    autoconf \
    automake \
    libtool \
    mariadb-devel \
    file \
    hostname \
    mariadb-server

RUN git clone --branch master --single-branch --depth 1 https://github.com/anton-povarov/meow /_src/meow
RUN curl -L https://github.com/nanomsg/nanomsg/archive/1.1.5.tar.gz | tar xvz -C /tmp && mv -v /tmp/nanomsg-1.1.5 /_src/nanomsg

COPY . /_src/pinba2
RUN /_src/pinba2/docker/build-from-source.sh

FROM fedora:25
MAINTAINER Anton Povarov "anton.povarov@gmail.com"

RUN dnf install -y file hostname mariadb-server

COPY --from=builder /_src/pinba2/mysql_engine/.libs/libpinba_engine2.so /usr/lib64/mysql/plugin/libpinba_engine2.so
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
EXPOSE 30003
CMD ["mysqld"]

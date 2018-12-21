FROM fedora:25 as builder

# https://docs.docker.com/develop/develop-images/dockerfile_best-practices/#sort-multi-line-arguments
RUN dnf install -y \
    autoconf \
    automake \
    boost-devel \
    cmake \
    dnf-plugins-core \
    file \
    gcc \
    gcc-c++ \
    git-core \
    hostname \
    libtool \
    mariadb-devel-3:10.1.26-2.fc25.x86_64 \
    mariadb-server-3:10.1.26-2.fc25.x86_64 \
    rpm-build

RUN	dnf builddep -y \
    mariadb

RUN         useradd builder -u 1000 -m -G users,wheel && \
            mkdir /home/builder/rpm && \
            chown -R builder /home/builder

COPY        --chown=builder:root ./docker/.rpmmacros /home/builder/.rpmmacros
COPY        --chown=root:root ./docker/sudoers /etc/sudoers

USER		builder
WORKDIR		/home/builder

RUN			dnf download --source mariadb-10.1.26-2.fc25.src
RUN			rpm -i mariadb*.rpm

WORKDIR		/home/builder/rpm
RUN			rpmbuild --nocheck -bi mariadb.spec

USER 		root
WORKDIR		/root

# TODO: Create v1.0.0 release and use it instead
RUN git clone --branch master --single-branch --depth 1 https://github.com/anton-povarov/meow /_src/meow
# https://docs.docker.com/develop/develop-images/dockerfile_best-practices/#using-pipes
# TODO: Set SHELL instead
RUN set -o pipefail && curl -L https://github.com/nanomsg/nanomsg/archive/1.1.5.tar.gz | tar xvz -C /tmp && mv -v /tmp/nanomsg-1.1.5 /_src/nanomsg
COPY . /_src/pinba2
RUN /_src/pinba2/docker/build-from-source.sh

FROM fedora:25
MAINTAINER Anton Povarov "anton.povarov@gmail.com"

RUN dnf install -y \
    file \
    hostname \
    jemalloc \
    mariadb-server-3:10.1.26-2.fc25.x86_64

COPY --from=builder /_src/pinba2/mysql_engine/.libs/libpinba_engine2.so /usr/lib64/mysql/plugin/libpinba_engine2.so
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
# TODO: Add bats based health check for exposed ports
EXPOSE 3002/udp
EXPOSE 3306/tcp
CMD ["mysqld"]

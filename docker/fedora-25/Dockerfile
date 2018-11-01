FROM        fedora:25
MAINTAINER  Anton Povarov "anton.povarov@gmail.com"

RUN         dnf install -y git-core gcc gcc-c++ boost-devel cmake autoconf automake libtool mariadb-devel
RUN         dnf install -y file hostname mariadb-server
RUN			dnf install -y dnf-plugins-core rpm-build
RUN			dnf builddep -y mariadb

RUN 		useradd builder -u 1000 -m -G users,wheel && \
    		echo "builder ALL=(ALL:ALL) NOPASSWD:ALL" >> /etc/sudoers && \
    		echo "# macros"                      >  /home/builder/.rpmmacros && \
    		echo "%_topdir    /home/builder/rpm" >> /home/builder/.rpmmacros && \
    		echo "%_sourcedir %{_topdir}"        >> /home/builder/.rpmmacros && \
    		echo "%_builddir  %{_topdir}"        >> /home/builder/.rpmmacros && \
    		echo "%_specdir   %{_topdir}"        >> /home/builder/.rpmmacros && \
    		echo "%_rpmdir    %{_topdir}"        >> /home/builder/.rpmmacros && \
    		echo "%_srcrpmdir %{_topdir}"        >> /home/builder/.rpmmacros && \
    		mkdir /home/builder/rpm && \
    		chown -R builder /home/builder

USER		builder
WORKDIR		/home/builder

RUN			dnf download --source mariadb
RUN			rpm -i mariadb*.rpm

WORKDIR		/home/builder/rpm
RUN			rpmbuild --nocheck -bi mariadb.spec

# RUN			echo 'test5' > test && rm test

USER 		root
WORKDIR		/root

RUN         mkdir -p /_src && cd /_src && \
			# git clone --branch=fix_lib https://github.com/mkevac/pinba2 && \
			git clone https://github.com/badoo/pinba2 && \
			git clone https://github.com/anton-povarov/meow && \
			git clone https://github.com/nanomsg/nanomsg ;

RUN         /_src/pinba2/docker/build-from-source.sh

RUN         dnf remove -y git-core gcc gcc-c++ boost-devel cmake autoconf automake libtool mariadb-devel && \
			dnf remove -y dnf-plugins-core rpm-build && \
			dnf autoremove -y && \
			dnf clean all && \
			rm -rf /home/builder/rpm/*

COPY        docker-entrypoint.sh /usr/local/bin/
RUN         ln -snf /usr/local/bin/docker-entrypoint.sh /
ENTRYPOINT  ["docker-entrypoint.sh"]

EXPOSE      30003
CMD         ["mysqld"]

#!/bin/bash -xe

# packages required for to build, can be safely removed when done
DEV_PACKAGES="git-core gcc gcc-c++ boost-devel cmake autoconf automake libtool mariadb-devel"

# packages required for operation
PACKAGES="hostname mariadb-server"

# install required packages
# dnf install -y $DEV_PACKAGES
# dnf install -y $PACKAGES

# # create directories
# mkdir -p /_src && cd /_src

# # pull sources
# git clone https://github.com/anton-povarov/pinba2
# git clone https://github.com/anton-povarov/meow
# git clone https://github.com/nanomsg/nanomsg

# build nanomsg and install (this one is a lil tricky to build statically)
cd /_src/nanomsg
cmake -DNN_STATIC_LIB=ON -DNN_ENABLE_DOC=OFF -DCMAKE_C_FLAGS="-fPIC -DPIC" -DCMAKE_INSTALL_PREFIX=/_install/nanomsg -DCMAKE_INSTALL_LIBDIR=lib .
make -j4
make install

# pinba
cd /_src/pinba2
./buildconf.sh
./configure --prefix=/_install/pinba2 \
	--with-mysql=/usr \
	--with-boost=/usr \
	--with-meow=/_src/meow \
	--with-nanomsg=/_install/nanomsg
make -j4

# FIXME: this needs to be easier
# for install, just copy stuff to mysql plugin dir
cp /_src/pinba2/mysql_engine/.libs/libpinba_engine2.so `mysql_config --plugindir`

# clean everything
rm -rf /_src
rm -rf /_install
dnf remove -y $DEV_PACKAGES
dnf autoremove -y
dnf clean all

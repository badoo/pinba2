#!/bin/bash -xe

# build nanomsg and install (this one is a lil tricky to build statically)
cd /_src/nanomsg
cmake \
	-DNN_STATIC_LIB=ON \
	-DNN_ENABLE_DOC=OFF \
	-DNN_MAX_SOCKETS=4096 \
	-DCMAKE_C_FLAGS="-fPIC -DPIC" \
	-DCMAKE_INSTALL_PREFIX=/_install/nanomsg \
	-DCMAKE_INSTALL_LIBDIR=lib \
	.

make -j4
make install

# build lz4 with PIC static lib
cd /_src/lz4
make CFLAGS="-fPIC -DPIC"
make install PREFIX=/_install/lz4

# pinba
cd /_src/pinba2
./buildconf.sh
./configure --prefix=/_install/pinba2 \
	--with-mysql=/home/builder/rpm/mariadb-10.1.26 \
	--with-boost=/usr \
	--with-meow=/_src/meow \
	--with-nanomsg=/_install/nanomsg \
	--with-lz4=/_install/lz4 \
	--enable-libmysqlservices
make -j4

# FIXME: this needs to be easier
# for install, just copy stuff to mysql plugin dir
cp /_src/pinba2/mysql_engine/.libs/libpinba_engine2.so `mysql_config --plugindir`

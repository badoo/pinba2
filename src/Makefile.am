
EXTRA_DIST = include/*.h

# FIXME: need to add this to configure and maybe move code to this repo completely
hardcoded_deps = \
	-I$(top_srcdir)/../misc \
	-I$(top_srcdir)/../meow \
	-I/local/_libs/boost/1.57.0/include \
	-I/local/_libs/sparsehash/2.0.3/include \
	#

cflags = \
	-O0 -g3 -mtune=native \
 	$(MYSQL_INC) \
	$(DEPS_CFLAGS) \
	-I$(top_srcdir) \
	-I$(top_srcdir)/include \
	$(hardcoded_deps) \
	#

AM_CXXFLAGS = \
	-std=c++11 \
	$(cflags) \
	#

AM_CPPFLAGS = \
	$(cflags) \
	#

# the base library
lib_LIBRARIES = libpinba2.a
libpinba2_a_SOURCES = \
	handler.cpp \
	collector.cpp \
	packet.cpp \
	$(top_srcdir)/proto/pinba.pb-c.c \
	#

libpinba2_a_CXXFLAGS = \
	$(AM_CXXFLAGS) \
	-fPIC -DPIC \
	#

libpinba2_a_LIBADD = \
	$(DEPS_LIBS) \
	#

# the standalone binary
bin_PROGRAMS = \
	pinba2 \
	#

pinba2_SOURCES = \
	main.cpp \
	#

pinba2_LDADD = \
	$(DEPS_LIBS) \
	libpinba2.a \
	#

# the mysql plugin
lib_LTLIBRARIES = libpinba_engine2.la

libpinba_engine2_la_SOURCES = \
	ha_pinba.cpp \
	#

libpinba_engine2_la_LDFLAGS = \
	-module

libpinba_engine2_la_LIBADD = \
	$(DEPS_LIBS) \
	libpinba2.a \
	#

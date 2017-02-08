
EXTRA_DIST = include/*.h

cflags = \
	$(CXXFLAGS) \
	-O0 -g3 -mtune=native \
 	$(MYSQL_INC) \
	$(DEPS_CFLAGS) \
	-I$(top_srcdir) \
	-I$(top_srcdir)/include \
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
	globals.cpp \
	handler.cpp \
	collector.cpp \
	repacker.cpp \
	coordinator.cpp \
	packet.cpp \
	report_snapshot.cpp \
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
	libpinba2.a \
	$(DEPS_LIBS) \
	#

# the mysql plugin
lib_LTLIBRARIES = libpinba_engine2.la

libpinba_engine2_la_SOURCES = \
	ha_pinba.cpp \
	#

libpinba_engine2_la_LDFLAGS = \
	-module

libpinba_engine2_la_LIBADD = \
	libpinba2.a \
	$(DEPS_LIBS) \
	#

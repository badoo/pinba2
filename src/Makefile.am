
EXTRA_DIST = include/*.h

cflags = \
	$(CXXFLAGS) \
	-D_GNU_SOURCE -fPIC -DPIC \
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
lib_LTLIBRARIES = libpinba2.la
libpinba2_la_SOURCES = \
	globals.cpp \
	collector.cpp \
	repacker.cpp \
	coordinator.cpp \
	packet.cpp \
	report_snapshot.cpp \
	../proto/pinba.pb-c.c \
	#

libpinba2_la_CXXFLAGS = \
	$(AM_CXXFLAGS) \
	-fPIC -DPIC \
	#

libpinba2_la_LIBADD = \
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
	libpinba2.la \
	$(DEPS_LIBS) \
	#

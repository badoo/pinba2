
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
lib_LIBRARIES = libpinba2.a
libpinba2_a_SOURCES = \
	globals.cpp \
	collector.cpp \
	repacker.cpp \
	coordinator.cpp \
	packet.cpp \
	report_snapshot.cpp \
	../t1ha/t1ha0.c \
	../t1ha/t1ha1.c \
	../proto/pinba.pb-c.c \
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

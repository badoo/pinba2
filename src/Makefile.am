
AM_CXXFLAGS = \
	$(AX_CXXFLAGS) \
	$(DEPS_CFLAGS) \
	-I$(top_srcdir) \
	-I$(top_srcdir)/include \
	-I$(top_srcdir)/third_party \
	#

AM_CFLAGS = \
	$(AX_CFLAGS) \
	$(DEPS_CFLAGS) \
	-I$(top_srcdir) \
	-I$(top_srcdir)/include \
	-I$(top_srcdir)/third_party \
	#

AM_LDFLAGS = \
	$(AX_LDFLAGS) \
	#


# the base library
lib_LIBRARIES = libpinba2.a
libpinba2_a_SOURCES = \
	globals.cpp \
	os_symbols.cpp \
	collector.cpp \
	repacker.cpp \
	coordinator.cpp \
	packet.cpp \
	report_snapshot.cpp \
	report_by_packet.cpp \
	report_by_request.cpp \
	report_by_timer.cpp \
	../proto/pinba.pb-c.c \
	#

libpinba2_a_LIBADD = \
	../third_party/t1ha/libt1ha.a \
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
	../third_party/t1ha/libt1ha.a \
	$(DEPS_LIBS) \
	#

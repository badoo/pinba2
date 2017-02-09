
cflags = \
	$(CXXFLAGS) \
 	-O0 -g3 -mtune=native \
 	$(MYSQL_INC) \
	$(DEPS_CFLAGS) \
	-I$(top_srcdir) \
	-I$(top_srcdir)/include \
	#

AM_CXXFLAGS = \
	-std=gnu++11 \
	$(cflags) \
	#

AM_CPPFLAGS = \
	$(cflags) \
	#

AM_LDFLAGS = \
	-flto \
	#

LIBS = \
	../src/libpinba2.a \
	$(DEPS_LIBS) \
	#

noinst_PROGRAMS = \
	test_collector \
	test_packet_repack \
	test_report \
	test_nanomsg_broadcast \
	test_nanomsg_ticker \
	test_variant \
	#

test_collector_SOURCES = \
	test_collector.cpp \
	#

test_packet_repack_SOURCES = \
	test_packet_repack.cpp \
	#

test_report_SOURCES = \
	test_report.cpp \
	#

test_nanomsg_broadcast_SOURCES = \
	test_nanomsg_broadcast.cpp \
	#

test_nanomsg_ticker_SOURCES = \
	test_nanomsg_ticker.cpp \
	#

test_variant_SOURCES = \
	test_variant.cpp \
	#

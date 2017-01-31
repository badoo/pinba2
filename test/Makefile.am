

# FIXME: need to add this to configure and maybe move code to this repo completely
hardcoded_deps = \
	-I$(top_srcdir)/../meow \
	-I$(top_srcdir)/../misc \
	-I/local/_libs/boost/1.57.0/include \
	-I/local/_libs/sparsehash/2.0.3/include \
	#

#	-I/local/_libs/sparsehash-c11/master/include/google

cflags = \
 	-O0 -g3 -mtune=native \
 	$(MYSQL_INC) \
	$(DEPS_CFLAGS) \
	-I$(top_srcdir) \
	-I$(top_srcdir)/include \
	$(hardcoded_deps) \
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
	$(DEPS_LIBS) \
	#

LDADD = \
	$(top_srcdir)/src/libpinba2.a \
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

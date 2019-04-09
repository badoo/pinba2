
AM_CXXFLAGS = \
	$(AX_CXXFLAGS) \
	$(DEPS_CFLAGS) \
	-I$(top_srcdir)/include \
	#

# disable optimization for experimental code
#AM_CXXFLAGS+= -O3 -ggdb3 -ffast-math
AM_CXXFLAGS+= -O0 -ggdb3

AM_CFLAGS = \
	$(AX_CFLAGS) \
	$(DEPS_CFLAGS) \
	-I$(top_srcdir)/include \
	#

AM_LDFLAGS = \
	$(AX_LDFLAGS) \
	#

LIBS = \
	../src/libpinba2.a \
	$(DEPS_LIBS) \
	#

noinst_PROGRAMS = \
	exp_collector \
	exp_packet_repack \
	exp_bloom_filter \
	exp_nanomsg_broadcast \
	exp_nanomsg_connect \
	exp_nanomsg_ticker \
	exp_nanomsg_poller \
	exp_protobuf_nmpa \
	exp_histogram_perf \
	exp_dictionary_perf \
	#

exp_collector_SOURCES = \
	exp_collector.cpp \
	#

exp_packet_repack_SOURCES = \
	exp_packet_repack.cpp \
	#

exp_bloom_filter_SOURCES = \
	exp_bloom_filter.cpp \
	#

exp_nanomsg_broadcast_SOURCES = \
	exp_nanomsg_broadcast.cpp \
	#

exp_nanomsg_connect_SOURCES = \
	exp_nanomsg_connect.cpp \
	#

exp_nanomsg_ticker_SOURCES = \
	exp_nanomsg_ticker.cpp \
	#

exp_nanomsg_poller_SOURCES = \
	exp_nanomsg_poller.cpp \
	#

exp_protobuf_nmpa_SOURCES = \
	exp_protobuf_nmpa.cpp \
	#

exp_histogram_perf_SOURCES = \
	exp_histogram_perf.cpp \
	#

exp_dictionary_perf_SOURCES = \
	exp_dictionary_perf.cpp \
	#

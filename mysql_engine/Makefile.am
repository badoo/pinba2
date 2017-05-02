
AM_CXXFLAGS = \
	$(AX_CXXFLAGS) \
	$(MYSQL_ENGINE_CFLAGS) \
	$(DEPS_CFLAGS) \
	-I$(top_srcdir) \
	-I$(top_srcdir)/include \
	#

AM_CFLAGS = \
	$(AX_CFLAGS) \
	$(DEPS_CFLAGS) \
	-I$(top_srcdir) \
	-I$(top_srcdir)/include \
	#

AM_LDFLAGS = \
	$(AX_LDFAGS) \
	-flto \
	#

# the mysql plugin
lib_LTLIBRARIES = libpinba_engine2.la

libpinba_engine2_la_SOURCES = \
	view_conf.cpp \
	plugin.cpp \
	handler.cpp \
	#

libpinba_engine2_la_LDFLAGS = \
	-module

libpinba_engine2_la_LIBADD = \
	../src/libpinba2.a \
	$(DEPS_LIBS) \
	#

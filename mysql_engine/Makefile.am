
AM_CXXFLAGS = \
	$(AX_CXXFLAGS) \
	$(DEPS_CFLAGS) \
	$(MYSQL_ENGINE_CFLAGS) \
	-I$(top_srcdir)/include \
	#

AM_CFLAGS = \
	$(AX_CFLAGS) \
	$(DEPS_CFLAGS) \
	-I$(top_srcdir)/include \
	#

AM_LDFLAGS = \
	$(AX_LDFLAGS) \
	#


# the mysql plugin
lib_LTLIBRARIES = libpinba_engine2.la

libpinba_engine2_la_SOURCES = \
	view_conf.cpp \
	plugin.cpp \
	handler.cpp \
	#

libpinba_engine2_la_LDFLAGS = \
	-flto -module

libpinba_engine2_la_LIBADD = \
	../src/libpinba2.a \
	$(DEPS_LIBS) \
	$(AX_LDFLAGS) \
	#

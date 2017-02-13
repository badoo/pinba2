
cflags = \
	$(CXXFLAGS) \
	-O0 -g3 \
	-D_GNU_SOURCE -DMYSQL_DYNAMIC_PLUGIN -DMYSQL_SERVER=1 \
	-fPIC -DPIC \
 	$(MYSQL_INC) \
	$(DEPS_CFLAGS) \
	-I.. \
	-I../include \
	#

AM_CXXFLAGS = \
	-std=c++11 \
	$(cflags) \
	#

AM_CPPFLAGS = \
	$(cflags) \
	#

# the mysql plugin
lib_LTLIBRARIES = libpinba_engine2.la

libpinba_engine2_la_SOURCES = \
	plugin.cpp \
	handler.cpp \
	#

libpinba_engine2_la_LDFLAGS = \
	-module

libpinba_engine2_la_LIBADD = \
	../src/libpinba2.la \
	$(DEPS_LIBS) \
	#

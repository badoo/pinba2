AC_INIT([pinba2], [2.0.0])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_SRCDIR([src/globals.cpp])
AM_INIT_AUTOMAKE([foreign])
AM_CONFIG_HEADER([src/auto_config.h])
#AX_PREFIX_CONFIG_H(src/pinba_config.h)
AM_INIT_AUTOMAKE

AC_PROG_CC
AC_PROG_CXX
AC_PROG_CXXCPP

AC_PROG_LIBTOOL
LIBTOOL="$LIBTOOL --preserve-dup-deps"
AC_SUBST(LIBTOOL)

AC_SUBST(MYSQL_INC)

AC_C_CONST
AC_TYPE_SIZE_T
AC_HEADER_STDC
AC_CHECK_FUNCS(memcpy memmove)
AC_CHECK_TYPES([uint16_t])     # defined in C99 systems
AC_CHECK_TYPES([u_int16_t])    # defined in BSD-derived systems, and gnu
AC_CHECK_TYPES([long long])    # probably defined everywhere, but...
AC_CHECK_HEADERS(limits.h syslimits.h string.h strings.h unistd.h stdint.h)
AC_PROG_SED
dnl AC_PROG_AWK

AC_CHECK_FUNCS([strndup sysconf recvmmsg])

changequote({,})
CXXFLAGS=`echo "$CXXFLAGS" | sed -e 's/ -DMYSQL_DYNAMIC_PLUGIN -DNDEBUG//g'`
CFLAGS=`echo "$CFLAGS" | sed -e 's/ -DNDEBUG//g'`
#CFLAGS="$CFLAGS -DNDEBUG"

changequote([,])
#CXXFLAGS="$CXXFLAGS -DMYSQL_DYNAMIC_PLUGIN -DNDEBUG"
CXXFLAGS="$CXXFLAGS -pthread -DMYSQL_DYNAMIC_PLUGIN"
CXXFLAGS="$CXXFLAGS -Wall -Wformat-security -Wno-unused -Wno-unused-parameter -Wno-unused-variable -Werror"

AX_CONFIG_NICE([config.nice])

dnl check for floor and libm
AC_CHECK_LIB([m], [floor], [LIBS="$LIBS -lm"], [AC_MSG_ERROR([can't continue without libm])])

AC_CHECK_LIB([pthread], [pthread_setname_np], [
	AC_DEFINE([HAVE_PTHREAD_SETNAME_NP], [1], [Whether pthread_setname_np() is available])
], [AC_MSG_NOTICE([can't find pthread_setname_np()])])

AC_CHECK_LIB([pthread], [pthread_setaffinity_np], [
	AC_DEFINE([HAVE_PTHREAD_SETAFFINITY_NP], [1], [Whether pthread_setaffinity_np() is available])
], [AC_MSG_NOTICE([can't find pthread_setaffinity_np()])])

STANDARD_PREFIXES="/usr /usr/local /opt /local"

dnl autorevision {{{
AUTOREVISION="$srcdir/autorevision.sh"

v=$($AUTOREVISION -t sh)
if test "$?" = "0"; then
	eval "$v"

	AC_DEFINE_UNQUOTED([VCS_DATE], "$VCS_DATE", [vcs date])
	AC_DEFINE_UNQUOTED([VCS_BRANCH], "$VCS_BRANCH", [vcs branch])
	AC_DEFINE_UNQUOTED([VCS_FULL_HASH], "$VCS_FULL_HASH", [vcs full hash])
	AC_DEFINE_UNQUOTED([VCS_SHORT_HASH], "$VCS_SHORT_HASH", [vcs short hash])
	AC_DEFINE_UNQUOTED([VCS_WC_MODIFIED], "$VCS_WC_MODIFIED", [vcs wc modified])
fi
dnl }}}

dir_resolve() dnl {{{
{
	pwd=`pwd`
	cd "$1" 2>/dev/null || cd "${pwd}/${1}" 2>/dev/null
	if test "$?" = "0"; then
		echo `pwd -P`
	else
		echo "$1"
	fi
}
dnl }}}

dnl {{{ --with-libdir
AC_ARG_WITH(libdir,
	[AS_HELP_STRING([--with-libdir],[look for libraries in .../NAME rather than .../lib])
	],
	[LIBDIR=$with_libdir],
	[LIBDIR=lib]
)
dnl }}}

dnl {{{ --disable-rpath
AC_ARG_ENABLE(rpath,
	[AS_HELP_STRING([--disable-rpath],[disable passing additional runtime library search paths])
	],
	[PINBA_RPATH=no],
	[PINBA_RPATH=yes]
)
dnl }}}

dnl {{{ check for rpath support
AC_MSG_CHECKING([if compiler supports -R])
AC_CACHE_VAL(pinba_cv_cc_dashr,[
	SAVE_LIBS=$LIBS
	LIBS="-R /usr/$LIBDIR $LIBS"
	AC_TRY_LINK([], [], pinba_cv_cc_dashr=yes, pinba_cv_cc_dashr=no)
	LIBS=$SAVE_LIBS])
AC_MSG_RESULT([$pinba_cv_cc_dashr])
if test $pinba_cv_cc_dashr = "yes"; then
	ld_runpath_switch=-R
else
	AC_MSG_CHECKING([if compiler supports -Wl,-rpath,])
	AC_CACHE_VAL(pinba_cv_cc_rpath,[
		SAVE_LIBS=$LIBS
		LIBS="-Wl,-rpath,/usr/$LIBDIR $LIBS"
		AC_TRY_LINK([], [], pinba_cv_cc_rpath=yes, pinba_cv_cc_rpath=no)
		LIBS=$SAVE_LIBS])
	AC_MSG_RESULT([$pinba_cv_cc_rpath])
	if test $pinba_cv_cc_rpath = "yes"; then
		ld_runpath_switch=-Wl,-rpath,
	else
		ld_runpath_switch=-L
	fi
fi
if test "$PINBA_RPATH" = "no"; then
	ld_runpath_switch=
fi
dnl }}}

dnl {{{ --with-mysql
AC_MSG_CHECKING(for MySQL source code)
AC_ARG_WITH(mysql, [AS_HELP_STRING([--with-mysql], [specify MySQL sources directory])], [],
[
	AC_MSG_ERROR([Please provide path to the MySQL sources directory])
])

if test "x$with_mysql" = "xno"; then
	AC_MSG_ERROR([can't continue without MySQL sources])
else

	if test "x$with_mysql" = "xyes"; then
		AC_MSG_ERROR([sorry, I'm not that smart to guess where the MySQL sources are, please specify the path])
	fi

	with_mysql=`dir_resolve "$with_mysql"`

	mysql_headers_found=0 # have we found usable headers?
	mysql_headers_type="" # which type are those? "source" = configured mysql source, "installed" = from package

	# check if we're building with mysql sources or installed headers
	AC_MSG_CHECKING([mysql configured sources in $with_mysql])
	HEADERS="sql/handler.h sql/plugin.h"
	for file in $HEADERS; do
		if test -r "$with_mysql/$file"; then
			mysql_headers_found=1
			mysql_headers_type="source"
		fi
	done

	if test "$mysql_headers_found" = "1"; then
		AC_MSG_RESULT([found])
	else
		AC_MSG_RESULT([not found])
	fi

	# now check installed headers
	if test "$mysql_headers_found" != "1"; then
		AC_MSG_CHECKING([mysql installed headers in $with_mysql/include/mysql])
		mysql_headers_found=0
		HEADERS="private/handler.h plugin.h"

		for file in $HEADERS; do
			if test -r "$with_mysql//include/mysql/$file"; then
				mysql_headers_found=1
				mysql_headers_type="installed"
			fi
		done

		if test "$mysql_headers_found" = "1"; then
			AC_MSG_RESULT([found])
		else
			AC_MSG_RESULT([not found])
		fi
	fi

	if test "$mysql_headers_found" != "1"; then
		AC_MSG_ERROR([couldn't find either configured sources or installed headers])
	else
		AC_MSG_NOTICE([using $with_mysql ($mysql_headers_type)])
	fi

	# now check for version
	case $mysql_headers_type in
		"source")
			MYSQL_INC="-DPINBA_USE_MYSQL_SOURCE -I$with_mysql -I$with_mysql/sql -I$with_mysql/regex -I$with_mysql/include"
			;;
		"installed")
			MYSQL_INC="-I$with_mysql/include -I$with_mysql/include/mysql -I$with_mysql/include/mysql/private"
			;;
		*)
			;;
	esac

	CFLAGS_old="$CFLAGS"
	CXXFLAGS_old="$CXXFLAGS"

	MYSQL_INC="$MYSQL_INC -fno-rtti"

	CFLAGS="$MYSQL_INC"
	CXXFLAGS="$MYSQL_INC"

	AC_MSG_CHECKING([for MySQL version >= 5.1])
	AC_TRY_RUN([
		#include <stdlib.h>
		#include <stdio.h>
		#include <mysql_version.h>
		int main() {
			printf("(MYSQL_VERSION_ID = %d, required >= 50100)... ", MYSQL_VERSION_ID);
		#if MYSQL_VERSION_ID >= 50100
			exit(0);
		#else
			exit(1);
		#endif
		}
	],
	[ AC_MSG_RESULT([ok]) ],
	[ AC_MSG_ERROR([MySQL 5.1+ is required])])

	CFLAGS="$CFLAGS_old"
	CXXFLAGS="$CXXFLAGS_old"
fi


dnl }}}

dnl {{{ --enable-static-build
AC_ARG_ENABLE(static-build,
	[AS_HELP_STRING([--enable-static-build], [link against non-system libs statically])
	],
	[
		AC_MSG_CHECKING([if static linking is forced])
		if test x"$enableval" = xyes ; then
			FORCE_STATIC_BUILD=yes
		else
			FORCE_STATIC_BUILD=no
		fi
		AC_MSG_RESULT([$FORCE_STATIC_BUILD])
	]
)
dnl }}}

dnl {{{ --enable-debug
AC_ARG_ENABLE(debug,
	[AS_HELP_STRING([--enable-debug], [enable debugging symbols and compile flags])
	],
	[
		if test x"$enableval" = xyes ; then
			debug="yes"
		else
			debug="no"
		fi
	]
)

if test x"$debug" = xyes ; then
	DEBUG_FLAGS="-DSAFE_MUTEX -DDBUG_ON -DEXTRA_DEBUG -DUNIV_MUST_NOT_INLINE -DFORCE_INIT_OF_VARS -DPINBA_DEBUG -DPINBA_ENGINE_DEBUG_ON"

	dnl AC_DEFINE([PINBA_ENGINE_DEBUG_ON], [1], [debug is on])

	if test x"$GCC" = xyes; then

		dnl Remove any optimization flags from CFLAGS
		changequote({,})
		CXXFLAGS=`echo "$CXXFLAGS" | sed -e 's/-O[0-9s]*//g'`
		CXXFLAGS=`echo "$CXXFLAGS" | sed -e 's/-g[0-2]\? //g'`
		CFLAGS=`echo "$CFLAGS" | sed -e 's/-O[0-9s]*//g'`
		CFLAGS=`echo "$CFLAGS" | sed -e 's/-g[0-2]\? //g'`
		changequote([,])
		CXXFLAGS="$CXXFLAGS -g3 -Wall -O0 $DEBUG_FLAGS"
		CFLAGS="$CFLAGS -g3 -Wall -O0 $DEBUG_FLAGS"
	fi

	ADD_FLAGS=""

	dnl Do not strip symbols from developer object files.
	INSTALL_STRIP_FLAG=""
else
	dnl AC_DEFINE([PINBA_ENGINE_DEBUG_OFF], [1], [debug is off])

	RELEASE_FLAGS="-O3 -ffast-math -g3 -mtune=native -DPINBA_ENGINE_DEBUG_OFF"

	CXXFLAGS="$CXXFLAGS $RELEASE_FLAGS"
	CFLAGS="$CFLAGS $RELEASE_FLAGS"

	dnl Make sure to strip symbols from non-developer object files.
	dnl INSTALL_STRIP_FLAG="-s"
fi
dnl }}}

dnl {{{ --with-build-string
AC_ARG_WITH(build-string,
	[AS_HELP_STRING([--with-build-string],[set custom build information here])
	],
	[
	 dq="\""
	 ws=" "
	 if test "${with_build_string#*$ws}" != "$with_build_string" || test "${with_build_string#*$dq}" != "$with_build_string"; then
		 AC_MSG_ERROR([Build string cannot contain whitespaces or double quotes])
	 fi
	 AC_DEFINE_UNQUOTED([PINBA_BUILD_STRING], "$with_build_string", [custom build string])
	],
	[ ]
)
dnl }}}

AC_ARG_WITH(nanomsg, [AS_HELP_STRING([--with-nanomsg], [path to nanomsg library (build it statically)])],
[
	CXXFLAGS="$CXXFLAGS -pthread -I$with_nanomsg/include"
	LDFLAGS="$LDFLAGS -lanl"
	DEPS_LIBS="$DEPS_LIBS $with_nanomsg/lib/libnanomsg.a"
	#DEPS_LIBS="$DEPS_LIBS -L$with_nanomsg/lib -lnanomsg"
])

AC_ARG_WITH(sparsehash, [AS_HELP_STRING([--with-sparsehash], [path to sparsehash library])],
[
	CXXFLAGS="$CXXFLAGS -I$with_sparsehash/include" # this one is for sparsehash
	CXXFLAGS="$CXXFLAGS -I$with_sparsehash/include/google" # this one is for sparsehash-c11
])

AC_ARG_WITH(meow, [AS_HELP_STRING([--with-meow], [path to meow library])],
[
	CXXFLAGS="$CXXFLAGS -I$with_meow"
])

AC_ARG_WITH(boost, [AS_HELP_STRING([--with-boost], [path to boost library])],
[
	CXXFLAGS="$CXXFLAGS -I$with_boost/include" # installed version
	CXXFLAGS="$CXXFLAGS -I$with_boost"         # just downloaded source code version
])

AC_SUBST(INSTALL_STRIP_FLAG)

AC_SUBST(DEPS_LIBS)
AC_SUBST(DEPS_CFLAGS)

AC_OUTPUT([Makefile src/Makefile test/Makefile mysql_engine/Makefile])

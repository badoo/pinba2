AC_INIT([pinba], [2.3.1])
AC_CONFIG_MACRO_DIR([m4])
AC_CONFIG_SRCDIR([src/globals.cpp])
AM_INIT_AUTOMAKE([foreign subdir-objects])
AM_CONFIG_HEADER([auto_config.h])
AX_PREFIX_CONFIG_H([pinba_config.h])
AX_CONFIG_NICE([config.nice])
AM_INIT_AUTOMAKE

${CFLAGS=""}
AC_PROG_CC
${CXXFLAGS=""}
AC_PROG_CXX

AC_PROG_LIBTOOL
LIBTOOL="$LIBTOOL --preserve-dup-deps"
AC_SUBST(LIBTOOL)

AC_C_CONST
AC_TYPE_SIZE_T
AC_HEADER_STDC
AC_CHECK_FUNCS(memcpy memmove)
AC_CHECK_TYPES([uint16_t])     # defined in C99 systems
AC_CHECK_TYPES([u_int16_t])    # defined in BSD-derived systems, and gnu
AC_CHECK_TYPES([long long])    # probably defined everywhere, but...
AC_CHECK_HEADERS(inttypes.h stdint.h unistd.h)
AC_PROG_SED
dnl AC_PROG_AWK

AC_CHECK_FUNCS([sysconf recvmmsg])

# compiler flags
common_flags=" -pthread"
# common_flags="$common_flags -Wall -Wextra"
# common_flags="$common_flags -Wno-unused -Wno-unused-parameter -Wno-unused-variable -Wno-missing-field-initializers"
common_flags="$common_flags -Wformat -Wformat-security -Werror"
common_flags="$common_flags -fno-omit-frame-pointer"
common_flags="$common_flags -fPIC -DPIC"
common_flags="$common_flags -D_GNU_SOURCE -D_POSIX_SOURCE"
common_flags="$common_flags -maes -msse4 -msse4.2"

AX_CFLAGS="-std=gnu11 $common_flags"
AX_CXXFLAGS="-std=gnu++11 -fno-rtti $common_flags"
AX_LDFLAGS="-flto -lrt -ldl"

dnl nonportable pthread expensions
AC_CHECK_LIB([pthread], [pthread_setname_np], [
	AC_DEFINE([HAVE_PTHREAD_SETNAME_NP], [1], [Whether pthread_setname_np() is available])
], [AC_MSG_NOTICE([can't find pthread_setname_np()])])

AC_CHECK_LIB([pthread], [pthread_setaffinity_np], [
	AC_DEFINE([HAVE_PTHREAD_SETAFFINITY_NP], [1], [Whether pthread_setaffinity_np() is available])
], [AC_MSG_NOTICE([can't find pthread_setaffinity_np()])])

STANDARD_PREFIXES="/usr /usr/local /opt /local"

# autorevision part
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


dir_resolve()
{
	pwd=`pwd`
	cd "$1" 2>/dev/null || cd "${pwd}/${1}" 2>/dev/null
	if test "$?" = "0"; then
		echo `pwd -P`
	else
		echo "$1"
	fi
}


AC_ARG_WITH(libdir,
	[AS_HELP_STRING([--with-libdir],[look for libraries in .../NAME rather than .../lib])
	],
	[LIBDIR=$with_libdir],
	[LIBDIR=lib]
)


AC_ARG_ENABLE(rpath,
	[AS_HELP_STRING([--disable-rpath],[disable passing additional runtime library search paths])
	],
	[PINBA_RPATH=no],
	[PINBA_RPATH=yes]
)


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
		AC_MSG_ERROR([not found])
	fi

	# now check for version
	case $mysql_headers_type in
		"source")
			MYSQL_INC="-DPINBA_USE_MYSQL_SOURCE -I$with_mysql -I$with_mysql/sql -I$with_mysql/regex -I$with_mysql/include"
			# these are for mysql 5.7
			MYSQL_INC="$MYSQL_INC -I$with_mysql/libbinlogevents/export -I$with_mysql/libbinlogevents/include"
			;;
		*)
			;;
	esac

	CFLAGS_old="$CFLAGS"
	CXXFLAGS_old="$CXXFLAGS"

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

	MYSQL_ENGINE_CFLAGS="-DMYSQL_DYNAMIC_PLUGIN -DMYSQL_SERVER=1 $MYSQL_INC"
fi


AC_ARG_ENABLE(libmysqlservices,
	[AS_HELP_STRING([--enable-libmysqlservices], [link against libmysqlservices from mariadb (required)])
	],
	[
		DEPS_LIBS="$DEPS_LIBS $with_mysql/libservices/libmysqlservices.a"
	]
)


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


AC_ARG_ENABLE(debug, [AS_HELP_STRING([--enable-debug], [enable debugging symbols and compile flags])],
[
	#enabled
	MYSQL_ENGINE_CFLAGS="$MYSQL_ENGINE_CFLAGS -DSAFE_MUTEX -DDBUG_ON -DEXTRA_DEBUG"
	MYSQL_ENGINE_CFLAGS="$MYSQL_ENGINE_CFLAGS -DUNIV_MUST_NOT_INLINE -DFORCE_INIT_OF_VARS"
	MYSQL_ENGINE_CFLAGS="$MYSQL_ENGINE_CFLAGS -DPINBA_DEBUG -DPINBA_ENGINE_DEBUG_ON"

	DEBUG_FLAGS="-O0 -ggdb3"
	AX_CFLAGS="$AX_CFLAGS $DEBUG_FLAGS"
	AX_CXXFLAGS="$AX_CXXFLAGS $DEBUG_FLAGS"
],
[
	#disabled (default if option not given or != yes)
	MYSQL_ENGINE_CFLAGS="$MYSQL_ENGINE_CFLAGS -DPINBA_ENGINE_DEBUG_OFF"

	RELEASE_FLAGS="-O3 -ffast-math -ggdb3 -mtune=native"
	AX_CFLAGS="$AX_CFLAGS $RELEASE_FLAGS"
	AX_CXXFLAGS="$AX_CXXFLAGS $RELEASE_FLAGS"

	dnl strip symbols from non-developer object files
	INSTALL_STRIP_FLAG="-s"
])

AC_ARG_WITH(build-string,  [AS_HELP_STRING([--with-build-string],[set custom build information here])],
[
	dq="\""
	ws=" "
	if test "${with_build_string#*$ws}" != "$with_build_string" || test "${with_build_string#*$dq}" != "$with_build_string"; then
		AC_MSG_ERROR([Build string cannot contain whitespaces or double quotes])
	fi
	AC_DEFINE_UNQUOTED([BUILD_STRING], "$with_build_string", [custom build string])
],
[
	AC_DEFINE_UNQUOTED([BUILD_STRING], "$with_build_string", [custom build string])
])


AC_ARG_WITH(nanomsg, [AS_HELP_STRING([--with-nanomsg], [path to nanomsg library (build it statically)])],
[
	DEPS_CFLAGS="$DEPS_CFLAGS -pthread -I$with_nanomsg/include"
	LDFLAGS="$LDFLAGS -lanl"
	DEPS_LIBS="$DEPS_LIBS $with_nanomsg/lib/libnanomsg.a"
])

AC_ARG_WITH(sparsehash, [AS_HELP_STRING([--with-sparsehash], [path to sparsehash library])],
[
	DEPS_CFLAGS="$DEPS_CFLAGS -I$with_sparsehash/include" # this one is for sparsehash
	DEPS_CFLAGS="$DEPS_CFLAGS -I$with_sparsehash/include/google" # this one is for sparsehash-c11
])

AC_ARG_WITH(meow, [AS_HELP_STRING([--with-meow], [path to meow library])],
[
	DEPS_CFLAGS="$DEPS_CFLAGS -I$with_meow"
])

AC_ARG_WITH(boost, [AS_HELP_STRING([--with-boost], [path to boost library])],
[
	DEPS_CFLAGS="$DEPS_CFLAGS -I$with_boost/include" # installed version
	DEPS_CFLAGS="$DEPS_CFLAGS -I$with_boost"         # just downloaded source code version
])

AC_ARG_ENABLE(experiments, [AS_HELP_STRING([--enable-experiments], [enable building experiments code])],
[
	EXPERIMENT_DIR="experiments"
],
[])



AC_SUBST(AX_CFLAGS)
AC_SUBST(AX_CXXFLAGS)
AC_SUBST(AX_LDFLAGS)

AC_SUBST(DEPS_CFLAGS)
AC_SUBST(DEPS_LIBS)

AC_SUBST(MYSQL_ENGINE_CFLAGS)
AC_SUBST(INSTALL_STRIP_FLAG)

AC_SUBST(EXPERIMENT_DIR)

AC_OUTPUT([Makefile src/Makefile include/Makefile mysql_engine/Makefile experiments/Makefile])

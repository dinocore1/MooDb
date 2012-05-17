AC_PREREQ(2.59)
AC_INIT(Moo db, 0.1, paul@dev-smart.com)

AC_CANONICAL_SYSTEM # create a cross platform build system
AM_INIT_AUTOMAKE # init automake

AC_CONFIG_MACRO_DIR([m4])

LT_INIT

AC_PROG_CC #check for C compiler
AC_PROG_CXX #check for C++ compiler

AC_ARG_ENABLE([debug],
  AS_HELP_STRING([--enable-debug], [compile with -g -O0]),
  [echo "compiling with -g -O0"
  CFLAGS="-g -O0"
  CXXFLAGS="-g -O0"
  ]
)

PKG_CHECK_MODULES( JS, mozjs185)
PKG_CHECK_MODULES( SQLITE, sqlite3)
PKG_CHECK_MODULES( POCO, poco)

AC_CHECK_TOOL([XXD], [xxd], [])
AC_CHECK_TOOL([OBJCOPY], [objcopy], [])
AC_PATH_PROG( JISON, cli-wrapper.js)  #check for jison (http://zaach.github.com/jison)
JISON="node $JISON"

AC_CONFIG_HEADERS([config.h]) #setup config header

#output Makefiles
AC_CONFIG_FILES([
	Makefile
	moodb/Makefile
	])
AC_OUTPUT


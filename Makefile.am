ACLOCAL_AMFLAGS = -I m4

SUBDIRS = moodb




check_PROGRAMS = test1 inserttest
test1_SOURCES = tests/test1.c
test1_LDADD = moodb/libmoodb.la $(JS_LIBS)
test1_LDFLAGS = -no-install

inserttest_SOURCES = tests/inserttest.c
inserttest_CFLAGS = -pg
inserttest_LDADD = moodb/libmoodb.la $(JS_LIBS)
inserttest_LDFLAGS = -no-install -pg
lib_LTLIBRARIES = libmoodb.la

libmoodb_la_CFLAGS = $(JS_CFLAGS) $(SQLITE_CFLAGS) $(POCO_CFLAGS)
libmoodb_la_CXXFLAGS = $(JS_CFLAGS) $(SQLITE_CFLAGS) $(POCO_CFLAGS)
libmoodb_la_LIBADD = $(JS_LIBS) $(SQLITE_LIBS) $(POCO_LIBS)
libmoodb_la_includedir = $(includedir)/moodb
libmoodb_la_include_HEADERS = \
	moodb.h

libmoodb_la_SOURCES = \
	common.h jsfunctions.h \
	moodb.h moodb.cpp \
	sqlitewrapper.h sqlitewrapper.cpp \
	js.h js.cpp \
	utils.h utils.cpp \
	queryexpression.c

.jison.js:
	$(JISON) $< -o $@ -m js
	echo "$*;" >> $@

.js.c:
	cat $< | (echo "const unsigned char $*[] = {"; $(XXD) -i ; echo "};") > $@
	

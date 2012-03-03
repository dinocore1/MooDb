lib_LTLIBRARIES = libmoodb.la

libmoodb_la_CFLAGS = $(JS_CFLAGS) $(SQLITE_CFLAGS) $(UUID_CFLAGS)
libmoodb_la_CXXFLAGS = $(JS_CFLAGS) $(SQLITE_CFLAGS) $(UUID_CFLAGS)
libmoodb_la_LIBADD = queryexpression.o $(JS_LIBS) $(SQLITE_LIBS) $(UUID_LIBS)
libmoodb_la_includedir = $(includedir)/moodb
libmoodb_la_include_HEADERS = \
	moodb.h

libmoodb_la_SOURCES = \
	moodb.h moodb.cpp \
	sqlitewrapper.h sqlitewrapper.cpp \
	js.h js.cpp \
	utils.h utils.cpp

.jison.js:
	$(JISON) $<	
	
.js.o:
	$(OBJCOPY) --input binary --output elf32-i386 --binary-architecture i386 $< $@
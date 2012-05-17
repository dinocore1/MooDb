ifeq ($(shell uname),Darwin)
ABSPATH := greadlink -m
else
ABSPATH := readlink -m
endif

JS_SRC_TAR := venders/js185-1.0.0.tar.gz

$(JS_SRC_TAR): 
	wget -O $(JS_SRC_TAR) http://ftp.mozilla.org/pub/mozilla.org/js/js185-1.0.0.tar.gz

SQLITE_SRC_TAR := venders/sqlite-autoconf-3071100.tar.gz

$(SQLITE_SRC_TAR):
	wget -O $(SQLITE_SRC_TAR) http://www.sqlite.org/sqlite-autoconf-3071100.tar.gz

POCO_SRC_TAR := venders/poco-1.4.3p1.tar.gz

$(POCO_SRC_TAR):
	wget -O $(POCO_SRC_TAR) http://downloads.sourceforge.net/project/poco/sources/poco-1.4.3/poco-1.4.3p1.tar.gz


	


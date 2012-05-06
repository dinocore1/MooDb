ifeq ($(shell uname),Darwin)
ABSPATH := greadlink -m
else
ABSPATH := readlink -m
endif

JS_SRC_TAR := venders/js185-1.0.0.tar.gz

$(JS_SRC_TAR): 
	wget -O $(JS_SRC_TAR) http://ftp.mozilla.org/pub/mozilla.org/js/js185-1.0.0.tar.gz


	


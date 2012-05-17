include utils.mk

NDK_VERSION := android-ndk-r8

ifeq ($(shell uname),Darwin)
ANDROID_NDK_ZIP := $(NDK_VERSION)-darwin-x86.tar.bz2
else
ANDROID_NDK_ZIP := $(NDK_VERSION)-linux-x86.tar.bz2
endif

ANDROID_NDK_ZIPFILE := venders/$(ANDROID_NDK_ZIP)


ANDROID_X86SYSROOT := venders/androidtoolchain-x86
ANDROID_ARMSYSROOT := venders/androidtoolchain-arm
ANDROID_CXX_X86 := $(ANDROID_X86SYSROOT)/bin/i686-android-linux-g++
ANDROID_CXX_ARM := $(ANDROID_ARMSYSROOT)/bin/arm-linux-androideabi-g++

$(ANDROID_NDK_ZIPFILE):
	wget -O $(ANDROID_NDK_ZIPFILE) http://dl.google.com/android/ndk/$(ANDROID_NDK_ZIP)
	cd venders && tar -xjf $(ANDROID_NDK_ZIP)
	touch $(ANDROID_NDK_ZIPFILE)

$(ANDROID_CXX_X86): $(ANDROID_NDK_ZIPFILE)
	venders/$(NDK_VERSION)/build/tools/make-standalone-toolchain.sh --platform=android-9 --install-dir=$(ANDROID_X86SYSROOT) --arch=x86
	touch $(ANDROID_CXX_X86)

$(ANDROID_CXX_ARM): $(ANDROID_NDK_ZIPFILE)
	venders/$(NDK_VERSION)/build/tools/make-standalone-toolchain.sh --platform=android-9 --install-dir=$(ANDROID_ARMSYSROOT) --arch=arm
	touch $(ANDROID_CXX_ARM)
	TOOLCHAIN="$(ABSPATH) $(ANDROID_ARMSYSROOT)" ./android/fixtoolchain.sh

ANDROID_ARM_SQLITE_LIB := build/android/lib/libsqlite3.a
ANDROID_ARM_JS185_LIB := build/android/lib/libmozjs185-1.0.a
ANDROID_ARM_MOODB_LIB := build/android/lib/moodb.a
ANDROID_ARM_POCO_LIB := build/android/lib/libPocoFoundation.a

ANDROID_ALL_LIBS := $(ANDROID_ARM_JS185_LIB) $(ANDROID_ARM_MOODB_LIB) $(ANDROID_ARM_SQLITE_LIB) $(ANDROID_ARM_POCO_LIB)

$(ANDROID_ARM_SQLITE_LIB): $(ANDROID_CXX_ARM) $(SQLITE_SRC_TAR)
	tar -xzf $(SQLITE_SRC_TAR) -C android
	export PATH="`$(ABSPATH) $(ANDROID_ARMSYSROOT)/bin`:${PATH}" && \
	cd android/sqlite-autoconf-3071100 && \
	./configure --prefix="`$(ABSPATH) ../../build/android/`" --host=arm-linux-eabi && \
	make install
	

$(ANDROID_ARM_JS185_LIB): $(ANDROID_CXX_ARM) $(JS_SRC_TAR)
	mkdir -p build/android/
	tar -xzf $(JS_SRC_TAR) -C android
	patch android/js-1.8.5/js/src/assembler/wtf/Platform.h < android/js185-android-build.patch
	PREFIX="`$(ABSPATH) build/android/`" TOOLCHAIN="`$(ABSPATH) $(ANDROID_ARMSYSROOT)`" NDK="`$(ABSPATH) venders/$(NDK_VERSION)`" ./android/build-android.sh 

$(ANDROID_ARM_POCO_LIB): $(ANDROID_CXX_ARM) $(POCO_SRC_TAR)
	tar -xzf $(POCO_SRC_TAR) -C android
	export PATH="`$(ABSPATH) $(ANDROID_ARMSYSROOT)/bin`:${PATH}" && \
	cd android/poco-1.4.3p1 && \
	./configure --prefix="`$(ABSPATH) ../../build/android/`" --config=Android --no-samples --no-tests && \
	make install -s -j4
	

$(ANDROID_ARM_MOODB_LIB): $(ANDROID_CXX_ARM) $(ANDROID_ARM_JS185_LIB) $(ANDROID_ARM_SQLITE_LIB) $(ANDROID_ARM_POCO_LIB)
	PREFIX="`$(ABSPATH) build/android/`" \
	TOOLCHAIN="`$(ABSPATH) $(ANDROID_ARMSYSROOT)`" \
	PKG_CONFIG_PATH="`$(ABSPATH) build/android/lib/pkgconfig`" \
	POCO_CFLAGS="-I`$(ABSPATH) build/android/include`" \
	POCO_LIBS="-L`$(ABSPATH) build/android/lib` -l PocoFoundation" \
	./android/build-moodb.sh


	


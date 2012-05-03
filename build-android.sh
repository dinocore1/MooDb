#!/bin/bash

set -e
set -o nounset


SDK=$HOME/Apps/android-sdk-mac_x86
NDK="`pwd`/android-ndk-r7"
TOOLCHAIN="`pwd`/android-arm-toolchain"

build_toolchain=false
if [ ! -d $TOOLCHAIN ]; then
  build_toolchain=true
fi

build_js185=true

JOBS=${JOBS:="-j4"}

SYSROOT=$TOOLCHAIN/sysroot
export PATH=$TOOLCHAIN/bin:$PATH
CFLAGS="-mandroid -mthumb -mcpu=cortex-a9 -mfpu=neon -mfloat-abi=softfp"

mkdir -p files
pushd files

if [ ! -e js185-1.0.0.tar.gz ]; then
  wget http://ftp.mozilla.org/pub/mozilla.org/js/js185-1.0.0.tar.gz
fi
popd


if [ "$build_toolchain" = "true" ]; then

  rm -r $TOOLCHAIN || true
  $NDK/build/tools/make-standalone-toolchain.sh --platform=android-9 --toolchain=arm-linux-androideabi-4.4.3 --install-dir=$TOOLCHAIN

  mkdir -p $SYSROOT/usr/local
  
  # Set up some symlinks to make the SpiderMonkey build system happy
  ln -sf $NDK/platforms $NDK/build/platforms
  for f in $TOOLCHAIN/bin/arm-linux-androideabi-*; do
    ln -sf $f ${f/arm-linux-androideabi-/arm-eabi-}
  done

  # Set up some symlinks for the typical autoconf-based build systems
  for f in $TOOLCHAIN/bin/arm-linux-androideabi-*; do
    ln -sf $f ${f/arm-linux-androideabi-/arm-linux-eabi-}
  done

fi

mkdir -p temp


if [ "$build_js185" = "true" ]; then
  rm -rf temp/js-1.8.5
  tar xvf files/js185-1.0.0.tar.gz -C temp/
  patch temp/js-1.8.5/js/src/assembler/wtf/Platform.h < js185-android-build.patch
  pushd temp/js-1.8.5/js/src
  CXXFLAGS="-I $TOOLCHAIN/arm-linux-androideabi/include/c++/4.4.3/arm-linux-androideabi" \
  HOST_CXXFLAGS="-DFORCE_LITTLE_ENDIAN" \
  ./configure \
    --prefix=$SYSROOT/usr/local \
    --target=arm-android-eabi \
    --with-android-ndk=$NDK \
    --with-android-sdk=$SDK \
    --with-android-toolchain=$TOOLCHAIN \
    --with-android-version=9 \
    --disable-tests \
    --disable-shared-js \
    --with-arm-kuser \
    CFLAGS="$CFLAGS"
  make $JOBS JS_DISABLE_SHELL=1
  make install JS_DISABLE_SHELL=1
  popd
fi


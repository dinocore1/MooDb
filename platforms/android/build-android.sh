#!/bin/bash

set -e
set -o nounset

JOBS=${JOBS:="-j4"}

export PATH="$TOOLCHAIN/bin:$PATH"
echo $PATH

# Set up some symlinks to make the SpiderMonkey build system happy
ln -sf $NDK/platforms $NDK/build/platforms
for f in $TOOLCHAIN/bin/arm-linux-androideabi-*; do
  ln -sf $f ${f/arm-linux-androideabi-/arm-eabi-}
done

# Set up some symlinks for the typical autoconf-based build systems
for f in $TOOLCHAIN/bin/arm-linux-androideabi-*; do
  ln -sf $f ${f/arm-linux-androideabi-/arm-linux-eabi-}
done

pushd android/js-1.8.5/js/src
  CXXFLAGS="-DANDROID -I $TOOLCHAIN/arm-linux-androideabi/include/c++/4.4.3/arm-linux-androideabi" \
  HOST_CXXFLAGS="-DFORCE_LITTLE_ENDIAN" \
  ./configure \
    --prefix="$PREFIX" \
    --target=arm-linux-eabi \
    --with-android-ndk="$NDK" \
    --with-android-sdk="$NDK/build/platforms/android-9" \
    --with-android-toolchain=$TOOLCHAIN \
    --with-android-version=9 \
    --disable-tests \
    --disable-shared-js \
    --with-arm-kuser
  make $JOBS JS_DISABLE_SHELL=1
  make install JS_DISABLE_SHELL=1
popd

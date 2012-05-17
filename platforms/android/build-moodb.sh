#!/bin/bash

set -e
set -o nounset

JOBS=${JOBS:="-j4"}

export PATH="$TOOLCHAIN/bin:$PATH"
echo $PKG_CONFIG_PATH

mkdir -p android/moodb

pushd android/moodb
  CXXFLAGS="-DANDROID -I $TOOLCHAIN/arm-linux-androideabi/include/c++/4.4.3/arm-linux-androideabi" \
  ../../../configure \
    --prefix="$PREFIX" \
    --host=arm-linux-eabi
  make $JOBS
  make install
popd

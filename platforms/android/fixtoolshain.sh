#!/bin/bash

set -e
set -o nounset

# Set up some symlinks for the typical autoconf-based build systems
for f in $TOOLCHAIN/bin/arm-linux-androideabi-*; do
  ln -sf $f ${f/arm-linux-androideabi-/arm-linux-eabi-}
done


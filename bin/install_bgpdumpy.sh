#!/usr/bin/env bash

set -e
set -x

sudo apt install libbz2-dev zlib1g-dev
if [ ! -d bgpdump ]; then
    git clone https://github.com/RIPE-NCC/bgpdump.git
    pushd bgpdump
else
    pushd bgpdump
    hg pull
fi
./bootstrap.sh
make
./bgpdump -T
popd


git submodule init
git submodule update

cp bgpdump/libbgpdump.so bgpdumpy/bgpdumpy/clib/
pip install -e bgpdumpy/

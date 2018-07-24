#!/bin/bash

set -e
set -x

../../redis/src/redis-cli -s ./storage.sock shutdown save

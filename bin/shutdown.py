#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from ipasnhistory.libs.helpers import is_running, get_socket_path
import time
from redis import StrictRedis

if __name__ == '__main__':
    r = StrictRedis(unix_socket_path=get_socket_path('cache'), db=1, decode_responses=True)
    r.set('shutdown', 1)
    while True:
        running = is_running()
        print(running)
        if not running:
            break
        time.sleep(10)

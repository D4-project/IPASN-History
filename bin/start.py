#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from subprocess import Popen
from ipasnhistory.libs.helpers import get_homedir
import time

if __name__ == '__main__':
    # Just fail if the env isn't set.
    get_homedir()
    p = Popen(['run_backend.py', '--start'])
    p.wait()
    Popen(['lookup_manager.py', '--days_in_memory', '180', '--floating_window_days', '30'])
    # Just wait a few seconds to make sure the lookup manager puts the days in memory key in redis.
    time.sleep(5)
    Popen(['caida_dl.py'])
    Popen(['caida_loader.py'])

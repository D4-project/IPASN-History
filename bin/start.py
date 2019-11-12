#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from subprocess import Popen
from ipasnhistory.libs.helpers import get_homedir
import time
from dateutil.relativedelta import relativedelta
from datetime import date
import sys

months_to_download = 1
days_in_memory = 10
floating_window_days = 3

oldest_day = date.today() - relativedelta(days=days_in_memory)
oldest_download = date.today() - relativedelta(months=months_to_download)

quit = False

if oldest_day < oldest_download:
    print(f'Need to download more historical data. Oldest day to keep in memory: {oldest_day} / Oldest download: {oldest_download}')
    quit = True

if floating_window_days > days_in_memory:
    print(f'Inconsistant amount of days to keep in memory ({days_in_memory}) vs. days floating window ({floating_window_days})')
    quit = True

if quit:
    sys.exit(1)

if __name__ == '__main__':
    # Just fail if the env isn't set.
    get_homedir()
    p = Popen(['run_backend.py', '--start'])
    p.wait()
    if p.returncode == 1:
        sys.exit(1)
    Popen(['lookup_manager.py', '--days_in_memory', str(days_in_memory), '--floating_window_days', str(floating_window_days)])
    # Just wait a few seconds to make sure the lookup manager puts the days in memory key in redis.
    time.sleep(5)
    Popen(['caida_dl.py', '--months_to_download', str(months_to_download)])
    Popen(['ripe_dl.py', '--months_to_download', str(months_to_download)])
    Popen(['caida_loader.py'])
    Popen(['ripe_loader.py'])

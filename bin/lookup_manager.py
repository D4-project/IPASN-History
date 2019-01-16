#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import logging
from datetime import timedelta, date
from subprocess import Popen
from typing import List

from redis import StrictRedis

from ipasnhistory.abstractmanager import AbstractManager
from ipasnhistory.libs.helpers import set_running, unset_running, get_socket_path

logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s:%(message)s',
                    level=logging.DEBUG, datefmt='%I:%M:%S')


'''
Each lookup module has 30 consecurive days, starting every 15 days so we have dedundancy

Process 1: 2018-08-01 -> 2018-08-30
Process 2: 2018-08-15 -> 2018-09-14
Process 3: 2018-08-30 -> 2018-09-29

The last 30 days are covered by 2 lookup processses to avoid downtime

Metadata
* currently loaded days
* sources
'''


class LookupManager(AbstractManager):

    def __init__(self,
                 days_in_memory: int=10,
                 floating_window_days: int=5,
                 sources: List[str]=['caida'],
                 loglevel: int=logging.WARNING):
        super().__init__(loglevel)
        self.floating_window_days = floating_window_days
        self.days_in_memory = days_in_memory
        self.sources = sources

        self.cache = StrictRedis(unix_socket_path=get_socket_path('cache'), decode_responses=True)
        # Cleanup pytricia cache information as it has to be reloaded
        for source in self.sources:
            self.cache.delete(f'{source}|v4|cached_dates')
            self.cache.delete(f'{source}|v6|cached_dates')

        init_date = date.today()
        self.running_processes = []
        # Start process today -> today + self.floating_window_days
        last = init_date + timedelta(days=self.floating_window_days)
        for source in self.sources:
            p = Popen(['lookup.py', source, init_date.isoformat(), last.isoformat()])
            self.running_processes.append((p, init_date, last))
            # Start process today - self.floating_window_days/2 -> today + self.floating_window_days/2
            first = init_date - timedelta(days=self.floating_window_days / 2)
            last = init_date + timedelta(days=self.floating_window_days / 2)
            p = Popen(['lookup.py', source, first.isoformat(), last.isoformat()])
            self.running_processes.append((p, first, last))

            current = init_date - timedelta(days=1)
            # Start all processes with complete datasets
            while current > (init_date - timedelta(days=self.days_in_memory)):
                begin_interval = current - timedelta(self.floating_window_days)
                p = Popen(['lookup.py', source, begin_interval.isoformat(), current.isoformat()])
                self.running_processes.append((p, begin_interval, current))
                current = current - timedelta(self.floating_window_days / 2)

        self.cache.sadd('META:sources', *self.sources)
        self.cache.hmset('META:expected_interval', {'first': (init_date - timedelta(days=self.days_in_memory)).isoformat(),
                                                    'last': init_date.isoformat()})

    def _cleanup_cached_dates(self):
        """Remove from '{source}|v4|cached_dates' and {source}|v6|cached_dates the dates that aren't cached anymore"""
        oldest_date = (date.today() - timedelta(days=self.days_in_memory)).isoformat()
        for source in self.sources:
            for address_family in ['v4', 'v6']:
                key = f'{source}|{address_family}|cached_dates'
                cached_dates = self.cache.smembers(key)
                to_remove = [date for date in cached_dates if date < oldest_date]
                if to_remove:
                    self.cache.srem(key, *to_remove)

    def _to_run_forever(self):
        # Check the processes are running, respawn if needed
        # Kill the processes with old data to clear memory
        set_running(self.__class__.__name__)
        for source in self.sources:
            first_loop = True
            for p, first, last in sorted(self.running_processes, key=lambda tup: tup[2], reverse=True):
                if first_loop:
                    first_loop = False
                    if last < (date.today() + timedelta(self.floating_window_days / 2)):
                        new_first = date.today()
                        new_last = date.today() + timedelta(days=self.floating_window_days)
                        new_p = Popen(['lookup.py', source, new_first.isoformat(), new_last.isoformat()])
                        self.running_processes.append((new_p, new_first, new_last))
                if last < (date.today() - timedelta(days=self.days_in_memory)):
                    p.kill()
                elif p.poll():
                    logging.warning(f'Lookup process died: {first} {last}')
                    # FIXME - maybe: respawn a dead process?
            # Cleanup the process list
            self.running_processes = [process for process in self.running_processes if process[0].poll() is None]

        self.cache.hmset('META:expected_interval', {'first': (date.today() - timedelta(days=self.days_in_memory)).isoformat(),
                                                    'last': date.today().isoformat()})
        self._cleanup_cached_dates()
        unset_running(self.__class__.__name__)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Manage the cached prefix announcements.')
    parser.add_argument('--days_in_memory', default=10, type=int, help='Total amount of days to keep in memory.')
    parser.add_argument('--floating_window_days', default=5, type=int, help='Amount of days per process.')
    parser.add_argument('--sources', metavar='source', type=str, nargs='+', default=['caida'], help='Sources to load.')
    args = parser.parse_args()

    lookup = LookupManager(args.days_in_memory, args.floating_window_days, args.sources)
    lookup.run(sleep_in_sec=1)

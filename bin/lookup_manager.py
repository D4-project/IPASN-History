#!/usr/bin/env python3

import argparse
import logging

from collections import defaultdict
from datetime import timedelta, date
from subprocess import Popen

from redis import Redis

from ipasnhistory.default import AbstractManager, get_socket_path, get_config

logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s:%(message)s',
                    level=logging.INFO)


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

    def __init__(self, loglevel: int=logging.WARNING):
        super().__init__(loglevel)
        self.script_name = "lookup_manager"
        self.floating_window_days = get_config('generic', 'floating_window_days')
        self.days_in_memory = get_config('generic', 'days_in_memory')
        self.sources = get_config('generic', 'sources')

        self.cache = Redis(unix_socket_path=get_socket_path('cache'), decode_responses=True)
        # Cleanup pytricia cache information as it has to be reloaded
        for source in self.sources:
            self.cache.delete(f'{source}|v4|cached_dates')
            self.cache.delete(f'{source}|v6|cached_dates')

        init_date = date.today()
        self.running_processes = defaultdict(list)
        # Start process today -> today + self.floating_window_days
        last = init_date + timedelta(days=self.floating_window_days)
        for source in self.sources:
            p = Popen(['lookup', source, init_date.isoformat(), last.isoformat()])
            self.running_processes[source].append((p, init_date, last))
            # Start process today - self.floating_window_days/2 -> today + self.floating_window_days/2
            first = init_date - timedelta(days=self.floating_window_days / 2)
            last = init_date + timedelta(days=self.floating_window_days / 2)
            p = Popen(['lookup', source, first.isoformat(), last.isoformat()])
            self.running_processes[source].append((p, first, last))

            current = init_date - timedelta(days=1)
            # Start all processes with complete datasets
            while current > (init_date - timedelta(days=self.days_in_memory)):
                begin_interval = current - timedelta(self.floating_window_days)
                p = Popen(['lookup', source, begin_interval.isoformat(), current.isoformat()])
                self.running_processes[source].append((p, begin_interval, current))
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
        for source in self.sources:
            first_loop = True
            for p, first, last in sorted(self.running_processes[source], key=lambda tup: tup[2], reverse=True):
                if first_loop:
                    first_loop = False
                    if last < (date.today() + timedelta(self.floating_window_days / 2)):
                        new_first = date.today()
                        new_last = date.today() + timedelta(days=self.floating_window_days)
                        new_p = Popen(['lookup', source, new_first.isoformat(), new_last.isoformat()])
                        self.running_processes[source].append((new_p, new_first, new_last))
                if last < (date.today() - timedelta(days=self.days_in_memory)):
                    p.kill()
                elif p.poll():
                    logging.warning(f'Lookup process died: {first} {last}')
                    # FIXME - maybe: respawn a dead process?
            # Cleanup the process list
            self.running_processes[source] = [process for process in self.running_processes[source] if process[0].poll() is None]

        self.cache.hmset('META:expected_interval', {'first': (date.today() - timedelta(days=self.days_in_memory)).isoformat(),
                                                    'last': date.today().isoformat()})
        self._cleanup_cached_dates()


def main():
    parser = argparse.ArgumentParser(description='Manage the cached prefix announcements.')
    parser.parse_args()

    lookup = LookupManager()
    lookup.run(sleep_in_sec=3600)


if __name__ == '__main__':
    main()

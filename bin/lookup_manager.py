#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
from datetime import timedelta, date
from subprocess import Popen

from ipasnhistory.abstractmanager import AbstractManager

logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s:%(message)s',
                    level=logging.DEBUG, datefmt='%I:%M:%S')


'''
Each lookup module has 30 consecurive days, starting every 15 days so we have dedundancy

Process 1: 2018-08-01 -> 2018-08-30
Process 2: 2018-08-15 -> 2018-09-14
Process 3: 2018-08-30 -> 2018-09-29

The last 30 days are covered by 2 lookup processses to avoid downtime
'''


class LookupManager(AbstractManager):

    def __init__(self,
                 days_in_memory: int=30,  # FIXME make it 180
                 floating_window_days: int=8,  # FIXME make it 30
                 source: str='caida',
                 loglevel: int=logging.WARNING):
        super().__init__(loglevel)
        self.floating_window_days = floating_window_days
        self.days_in_memory = days_in_memory
        self.source = source

        init_date = date.today()
        self.running_processes = []
        # Start process today -> today + self.floating_window_days
        last = init_date + timedelta(days=self.floating_window_days)
        p = Popen(['lookup.py', self.source, init_date.isoformat(), last.isoformat()])
        self.running_processes.append((p, init_date, last))
        # Start process today - self.floating_window_days/2 -> today + self.floating_window_days/2
        first = init_date - timedelta(days=self.floating_window_days / 2)
        last = init_date + timedelta(days=self.floating_window_days / 2)
        p = Popen(['lookup.py', self.source, first.isoformat(), last.isoformat()])
        self.running_processes.append((p, first, last))

        current = init_date - timedelta(days=1)
        # Start all processes with complete datasets
        while current > (init_date - timedelta(days=self.days_in_memory)):
            begin_interval = current - timedelta(self.floating_window_days)
            p = Popen(['lookup.py', self.source, begin_interval.isoformat(), current.isoformat()])
            self.running_processes.append((p, begin_interval, current))
            current = current - timedelta(self.floating_window_days / 2)

    def _to_run_forever(self):
        # Check the processes are running, respawn if needed
        # Kill the processes with old data to clear memory
        first_loop = True
        for p, first, last in sorted(self.running_processes, key=lambda tup: tup[2], reverse=True):
            if first_loop:
                first_loop = False
                if last < (date.today() + timedelta(self.floating_window_days / 2)):
                    new_first = date.today()
                    new_last = date.today() + timedelta(days=self.floating_window_days)
                    new_p = Popen(['lookup.py', self.source, new_first.isoformat(), new_last.isoformat()])
                    self.running_processes.append((new_p, new_first, new_last))
            if last < (date.today() - timedelta(days=self.days_in_memory)):
                p.kill()
            elif p.poll():
                logging.warning(f'Lookup process died: {first} {last}')
                # FIXME - maybe: respawn a dead process?
        # Cleanup the process list
        self.running_processes = [process for process in self.running_processes if process[0].poll() is None]


if __name__ == '__main__':
    lookup = LookupManager()
    lookup.run(sleep_in_sec=1)

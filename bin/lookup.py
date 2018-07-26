#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging

from ipasnhistory.abstractmanager import AbstractManager
from ipasnhistory.lookup import Lookup

logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s:%(message)s',
                    level=logging.DEBUG, datefmt='%I:%M:%S')


class LookupManager(AbstractManager):

    def __init__(self, loglevel: int=logging.WARNING):
        super().__init__(loglevel)
        self.lookup = Lookup(loglevel)
        self.lookup.load_all()

    def _to_run_forever(self):
        self.lookup.lookup()


if __name__ == '__main__':
    lookup = LookupManager()
    lookup.run(sleep_in_sec=1)

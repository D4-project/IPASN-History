#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import logging

from ipasnhistory.abstractmanager import AbstractManager
from ipasnhistory.caida_loader import CaidaLoader
from ipasnhistory.libs.helpers import get_homedir

logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s:%(message)s',
                    level=logging.DEBUG, datefmt='%I:%M:%S')


class CaidaManager(AbstractManager):

    def __init__(self, storage_directory: Path=None, loglevel: int=logging.WARNING):
        super().__init__(loglevel)
        if not storage_directory:
            self.storage_directory = get_homedir() / 'rawdata'
        self.loader = CaidaLoader(self.storage_directory, loglevel)

    def _to_run_forever(self):
        self.loader.load_all()


if __name__ == '__main__':
    m = CaidaManager()
    m.run(sleep_in_sec=3600)

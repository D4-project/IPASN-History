#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import logging
from dateutil.relativedelta import relativedelta
import asyncio
from datetime import date
import aiohttp

from ipasnhistory.abstractmanager import AbstractManager
from ipasnhistory.caida_downloader import CaidaDownloader
from ipasnhistory.libs.helpers import get_homedir

logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s:%(message)s',
                    level=logging.WARNING, datefmt='%I:%M:%S')


class CaidaManager(AbstractManager):

    def __init__(self, storage_directory: Path=None, loglevel: int=logging.WARNING):
        super().__init__(loglevel)
        if not storage_directory:
            self.storage_directory = get_homedir() / 'rawdata'
        self.downloader = CaidaDownloader(self.storage_directory, loglevel)
        # Download last year data.
        loop = asyncio.get_event_loop()
        last_year = date.today() - relativedelta(years=1)
        v4 = asyncio.ensure_future(self.downloader.find_routes('v4', first_date=last_year))
        v6 = asyncio.ensure_future(self.downloader.find_routes('v6', first_date=last_year))
        loop.run_until_complete(asyncio.gather(v4, v6, return_exceptions=True))

    def _to_run_forever(self):
        try:
            loop = asyncio.get_event_loop()
            tasks = []
            for address_family in ['v4', 'v6']:
                task = self.downloader.download_latest(address_family)
                tasks.append(task)
            loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
        except aiohttp.client_exceptions.ClientConnectorError as e:
            self.logger.critical(f'Error while fetching a routeview file: {e}')


if __name__ == '__main__':
    m = CaidaManager()
    m.run(sleep_in_sec=3600)

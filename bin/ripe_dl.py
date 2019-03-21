#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import logging
from dateutil.relativedelta import relativedelta
import asyncio
from datetime import date
import aiohttp

from ipasnhistory.abstractmanager import AbstractManager
from ipasnhistory.ripe_downloader import RipeDownloader
from ipasnhistory.libs.helpers import get_homedir

logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s:%(message)s',
                    level=logging.DEBUG, datefmt='%I:%M:%S')


class RipeManager(AbstractManager):

    def __init__(self, collector: str, hours: list, storage_directory: Path=None, loglevel: int=logging.DEBUG):
        super().__init__(loglevel)
        if not storage_directory:
            self.storage_directory = get_homedir() / 'rawdata'
        self.downloader = RipeDownloader(self.storage_directory, collector, hours, loglevel)
        # Download last 6 month data.
        last_months = date.today() - relativedelta(months=6)
        first_date = last_months

        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.downloader.find_routes(first_date=first_date))

    async def _to_run_forever_async(self):
        try:
            await self.downloader.download_latest()
        except aiohttp.client_exceptions.ClientConnectorError as e:
            self.logger.critical(f'Error while fetching a routeview file: {e}')


if __name__ == '__main__':
    m = RipeManager('rrc00', ['0000'])
    loop = asyncio.get_event_loop()
    loop.run_until_complete(m.run_async(sleep_in_sec=3600))
    loop.close()

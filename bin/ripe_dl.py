#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import logging
from dateutil.relativedelta import relativedelta
import asyncio
from datetime import date, timedelta
import aiohttp

from ipasnhistory.default import AbstractManager, safe_create_dir, get_config
from ipasnhistory.helpers import get_data_dir

logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s:%(message)s',
                    level=logging.INFO)


class RipeDownloader(AbstractManager):

    def __init__(self, loglevel: int=logging.INFO):
        super().__init__(loglevel)
        self.script_name = "ripe_downloader"
        self.collector = 'rrc00'
        self.hours = ['0000']
        self.months_to_download = get_config('generic', 'months_to_download')
        self.url = 'http://data.ris.ripe.net/{}'
        self.storage_root = get_data_dir()
        self.sema = asyncio.BoundedSemaphore(10)

        oldest_month_to_download = date.today() - relativedelta(months=self.months_to_download)
        asyncio.run(self.find_routes(first_date=oldest_month_to_download))

    async def _to_run_forever_async(self):
        try:
            await self.download_latest()
        except aiohttp.client_exceptions.ClientConnectorError as e:
            self.logger.critical(f'Error while fetching a routeview file: {e}')

    async def download_routes(self, session: aiohttp.ClientSession, path: str) -> bool:
        store_path = self.storage_root / 'ripe' / path
        if store_path.exists():
            # Already downloaded
            return False
        self.logger.info(f'New file to download: {path}')
        safe_create_dir(store_path.parent)
        async with self.sema, session.get(self.url.format(path)) as r:
            self.logger.debug('Starting {}'.format(self.url.format(path)))
            if r.status != 200:
                self.logger.debug('Unreachable: {}'.format(self.url.format(path)))
                return False
            content = await r.read()
            if not content.startswith(b'\x1f\x8b'):
                # Not a gzip file, skip.
                print(content[:2])
                return False
            with open(store_path, 'wb') as f:
                f.write(content)
            self.logger.debug('Done {}'.format(self.url.format(path)))
            return True

    async def find_routes(self, first_date: date, last_date: date=date.today()) -> None:
        cur_date = last_date
        tasks = []
        async with aiohttp.ClientSession() as session:
            while cur_date >= first_date:
                for hour in self.hours:
                    path = f'{self.collector}/{cur_date:%Y.%m}/bview.{cur_date:%Y%m%d}.{hour}.gz'
                    tasks.append(self.download_routes(session, path))
                cur_date -= timedelta(days=1)
            await asyncio.gather(*tasks, return_exceptions=True)

    async def download_latest(self) -> None:
        self.logger.debug('Search for new routes.')
        cur_date = date.today()
        async with aiohttp.ClientSession() as session:
            for hour in self.hours:
                path = f'{self.collector}/{cur_date:%Y.%m}/bview.{cur_date:%Y%m%d}.{hour}.gz'
                downloaded = await self.download_routes(session, path)
                if downloaded:
                    self.logger.debug('New routes found.')
                    break
            else:
                self.logger.debug('No new routes.')


def main():
    parser = argparse.ArgumentParser(description='Download raw routes from RIPE.')
    parser.parse_args()

    m = RipeDownloader()
    asyncio.run(m.run_async(sleep_in_sec=3600))


if __name__ == '__main__':
    main()

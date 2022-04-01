#!/usr/bin/env python3

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
        self.url = 'http://data.ris.ripe.net/{}'
        self.storage_root = get_data_dir()

    async def _to_run_forever_async(self):
        try:
            await self.download_latest()
        except aiohttp.client_exceptions.ClientConnectorError as e:
            self.logger.critical(f'Error while fetching a routeview file: {e}')

    async def download_routes(self, session: aiohttp.ClientSession, path: str) -> None:
        store_path = self.storage_root / 'ripe' / path
        if store_path.exists():
            # Already downloaded
            return
        self.logger.info(f'New file to download: {path}')
        safe_create_dir(store_path.parent)
        async with session.get(self.url.format(path)) as r:
            self.logger.debug(f'Starting {self.url.format(path)}')
            if r.status != 200:
                self.logger.info(f'Unreachable: {self.url.format(path)}')
                return
            content = await r.read()
            if not content.startswith(b'\x1f\x8b'):
                # Not a gzip file, skip.
                print(content[:2])
            with open(store_path, 'wb') as f:
                f.write(content)
            self.logger.info(f'File downloaded: {path}')

    async def find_routes(self, first_date: date, last_date: date=date.today()) -> None:
        cur_date = last_date
        sem = asyncio.Semaphore(2)
        async with aiohttp.ClientSession() as session:
            while cur_date >= first_date:
                for hour in self.hours:
                    path = f'{self.collector}/{cur_date:%Y.%m}/bview.{cur_date:%Y%m%d}.{hour}.gz'
                    async with sem:
                        await self.download_routes(session, path)
                cur_date -= timedelta(days=1)

    async def download_latest(self) -> None:
        self.logger.debug('Search for new routes.')
        cur_date = date.today()
        async with aiohttp.ClientSession() as session:
            for hour in self.hours:
                path = f'{self.collector}/{cur_date:%Y.%m}/bview.{cur_date:%Y%m%d}.{hour}.gz'
                await self.download_routes(session, path)
            else:
                self.logger.debug('No new routes.')


def main():
    parser = argparse.ArgumentParser(description='Download raw routes from RIPE.')
    parser.parse_args()

    m = RipeDownloader()
    months_to_download = get_config('generic', 'months_to_download')
    oldest_month_to_download = date.today() - relativedelta(months=months_to_download)
    asyncio.run(m.find_routes(first_date=oldest_month_to_download))
    asyncio.run(m.run_async(sleep_in_sec=3600))


if __name__ == '__main__':
    main()

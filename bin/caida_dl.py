#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import asyncio
import logging

from datetime import date
from typing import Tuple

import aiohttp
from bs4 import BeautifulSoup  # type: ignore
from dateutil.relativedelta import relativedelta

from ipasnhistory.default import AbstractManager, safe_create_dir, get_config
from ipasnhistory.helpers import get_data_dir

logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s:%(message)s',
                    level=logging.INFO)


class CaidaDownloader(AbstractManager):

    def __init__(self, loglevel: int=logging.INFO):
        super().__init__(loglevel)
        self.script_name = "caida_downloader"
        self.months_to_download = get_config('generic', 'months_to_download')
        last_months = date.today() - relativedelta(months=self.months_to_download)
        self.ipv6_url = 'http://data.caida.org/datasets/routing/routeviews6-prefix2as/{}'
        self.ipv4_url = 'http://data.caida.org/datasets/routing/routeviews-prefix2as/{}'
        self.storage_root = get_data_dir()
        self.sema = asyncio.BoundedSemaphore(2)

        asyncio.run(self._fetch_existing_routes(last_months))

    async def _fetch_existing_routes(self, cutoff_date: date):
        v4 = asyncio.ensure_future(self.find_routes('v4', first_date=cutoff_date))
        v6 = asyncio.ensure_future(self.find_routes('v6', first_date=cutoff_date))

        await asyncio.gather(v4, v6, return_exceptions=True)

    async def _to_run_forever_async(self):
        try:
            for address_family in ['v4', 'v6']:
                await self.download_latest(address_family)
        except aiohttp.client_exceptions.ClientConnectorError as e:
            self.logger.critical(f'Error while fetching a routeview file: {e}')

    def _get_root_url(self, address_family: str) -> str:
        if address_family == 'v4':
            return self.ipv4_url
        return self.ipv6_url

    async def _has_new(self, address_family: str) -> Tuple[bool, str]:
        root_url = self._get_root_url(address_family)
        async with aiohttp.ClientSession() as session:
            async with session.get(root_url.format('pfx2as-creation.log')) as r:
                text = await r.text()
                last_entry = text.split('\n')[-2]
                path = last_entry.split('\t')[-1]
                if (self.storage_root / 'caida' / address_family / path).exists():
                    self.logger.debug(f'Same file already loaded: {path}')
                    return False, path
                self.logger.info(f'New route found: {path}')
                return True, path

    async def download_routes(self, session: aiohttp.ClientSession, address_family: str, path: str) -> None:
        store_path = self.storage_root / 'caida' / address_family / path
        if store_path.exists():
            # Already downloaded
            return
        self.logger.info(f'New file to download: {path}')
        safe_create_dir(store_path.parent)
        root_url = self._get_root_url(address_family)
        async with self.sema, session.get(root_url.format(path)) as r:
            self.logger.debug(root_url.format(path))
            content = await r.read()
            if not content.startswith(b'\x1f\x8b'):
                # Not a gzip file, skip.
                print(content[:2])
                return
            with open(store_path, 'wb') as f:
                f.write(content)

    async def find_routes(self, address_family: str, first_date: date, last_date: date=date.today()) -> None:
        root_url = self._get_root_url(address_family)
        cur_date = last_date
        while cur_date >= first_date:
            list_url = f'{cur_date:%Y/%m}'  # Makes a string like that: YYYY/MM
            async with aiohttp.ClientSession() as session:
                tasks = []
                self.logger.debug(root_url.format(list_url))
                async with session.get(root_url.format(list_url)) as r:
                    soup = BeautifulSoup(await r.text(), 'html.parser')
                    for a in soup.find_all('a'):
                        href = a.get('href')
                        if href.startswith('routeviews'):
                            dl_path = f'{cur_date:%Y/%m}/{href}'
                            self.logger.debug(dl_path)
                            task = asyncio.ensure_future(self.download_routes(session, address_family, dl_path))
                            if task:
                                tasks.append(task)
                await asyncio.gather(*tasks, return_exceptions=True)
            cur_date = cur_date - relativedelta(months=1)

    async def download_latest(self, address_family: str) -> None:
        self.logger.debug(f'Search for new routes ({address_family}).')
        has_new, path = await self._has_new(address_family)
        if not has_new:
            self.logger.debug(f'None found ({address_family}.')
            return
        async with aiohttp.ClientSession() as session:
            self.logger.debug(f'Has new ({address_family}.')
            await self.download_routes(session, address_family, path)


def main():
    parser = argparse.ArgumentParser(description='Download raw routes from RIPE.')
    parser.parse_args()

    m = CaidaDownloader()
    asyncio.run(m.run_async(sleep_in_sec=3600))


if __name__ == '__main__':
    main()

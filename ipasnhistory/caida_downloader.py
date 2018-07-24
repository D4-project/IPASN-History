#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from bs4 import BeautifulSoup
import aiohttp
from datetime import date
from dateutil.relativedelta import relativedelta
import asyncio

from .libs.helpers import safe_create_dir


class CaidaDownloader():

    def __init__(self, storage_directory: Path, loglevel: int=logging.DEBUG) -> None:
        self.__init_logger(loglevel)
        self.ipv6_url = 'http://data.caida.org/datasets/routing/routeviews6-prefix2as/{}'
        self.ipv4_url = 'http://data.caida.org/datasets/routing/routeviews-prefix2as/{}'
        self.storage_root = storage_directory

    def __init_logger(self, loglevel):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.logger.setLevel(loglevel)

    def _get_root_url(self, address_family: str) -> str:
        if address_family == 'v4':
            return self.ipv4_url
        return self.ipv6_url

    async def _has_new(self, address_family: str):
        root_url = self._get_root_url(address_family)
        async with aiohttp.ClientSession() as session:
            async with session.get(root_url.format('pfx2as-creation.log')) as r:
                last_entry = await r.text.split('\n')[-2]
                path = last_entry.split('\t')[-1]
                # date = parse(re.findall('(?:.*)/(?:.*)/routeviews-rv[2,6]-(.*).pfx2as.gz', path)[0])
                if (self.storage_root / 'caida' / address_family / path).exists():
                    self.logger.debug(f'Same file already loaded: {path}')
                    return False, path
                return True, path

    async def download_routes(self, session, address_family: str, path: str) -> None:
        store_path = self.storage_root / 'caida' / address_family / path
        if store_path.exists():
            # Already downloaded
            return
        safe_create_dir(store_path.parent)
        root_url = self._get_root_url(address_family)
        async with session.get(root_url.format(path)) as r:
            logging.debug(root_url.format(path))
            content = await r.read()
            with open(store_path, 'wb') as f:
                f.write(content)

    async def find_routes(self, address_family: str, first_date: date, last_date: date=date.today()):
        root_url = self._get_root_url(address_family)
        cur_date = last_date
        while cur_date >= first_date:
            list_url = f'{cur_date:%Y/%m}'  # Makes a string like that: YYYY/MM
            async with aiohttp.ClientSession() as session:
                tasks = []
                logging.debug(root_url.format(list_url))
                async with session.get(root_url.format(list_url)) as r:
                    soup = BeautifulSoup(await r.text(), 'html.parser')
                    for a in soup.find_all('a'):
                        href = a.get('href')
                        if href.startswith('routeviews'):
                            dl_path = f'{cur_date:%Y/%m}/{href}'
                            logging.debug(dl_path)
                            task = asyncio.ensure_future(self.download_routes(session, address_family, dl_path))
                            tasks.append(task)
                r = asyncio.gather(*tasks, return_exceptions=True)
                await r
            cur_date = cur_date - relativedelta(months=1)

    async def download_latest(self, address_family: str):
        has_new, path = await self._has_new(address_family)
        if has_new:
            async with aiohttp.ClientSession() as session:
                await self.download_routes(session, address_family, path)

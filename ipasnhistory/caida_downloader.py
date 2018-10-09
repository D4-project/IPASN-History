#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from bs4 import BeautifulSoup
import aiohttp
from datetime import date
from dateutil.relativedelta import relativedelta
import asyncio
from typing import Tuple

from .libs.helpers import safe_create_dir, set_running, unset_running


class CaidaDownloader():

    def __init__(self, storage_directory: Path, loglevel: int=logging.DEBUG) -> None:
        self.__init_logger(loglevel)
        self.ipv6_url = 'http://data.caida.org/datasets/routing/routeviews6-prefix2as/{}'
        self.ipv4_url = 'http://data.caida.org/datasets/routing/routeviews-prefix2as/{}'
        self.storage_root = storage_directory
        self.sema = asyncio.BoundedSemaphore(2)

    def __init_logger(self, loglevel):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.logger.setLevel(loglevel)

    def _get_root_url(self, address_family: str) -> str:
        if address_family == 'v4':
            return self.ipv4_url
        return self.ipv6_url

    async def _has_new(self, address_family: str) -> Tuple[bool, str]:
        root_url = self._get_root_url(address_family)
        async with aiohttp.ClientSession() as session:
            async with session.get(root_url.format('pfx2as-creation.log')) as r:
                last_entry = await r.text.split('\n')[-2]
                path = last_entry.split('\t')[-1]
                if (self.storage_root / 'caida' / address_family / path).exists():
                    self.logger.debug(f'Same file already loaded: {path}')
                    return False, path
                self.logger.info(f'New route found: {path}')
                return True, path

    async def download_routes(self, session: aiohttp.ClientSession, address_family: str, path: str) -> None:
        self.logger.info(f'New file to download: {path}')
        store_path = self.storage_root / 'caida' / address_family / path
        if store_path.exists():
            # Already downloaded
            return
        safe_create_dir(store_path.parent)
        root_url = self._get_root_url(address_family)
        async with self.sema, session.get(root_url.format(path)) as r:
            logging.debug(root_url.format(path))
            content = await r.read()
            if not content.startswith(b'\x1f\x8b'):
                # Not a gzip file, skip.
                print(content[:2])
                return
            with open(store_path, 'wb') as f:
                f.write(content)

    async def find_routes(self, address_family: str, first_date: date, last_date: date=date.today()) -> None:
        set_running(self.__class__.__name__)
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
        unset_running(self.__class__.__name__)

    async def download_latest(self, address_family: str) -> None:
        set_running(self.__class__.__name__)
        self.logger.debug(f'Search for new routes ({address_family}).')
        has_new, path = await self._has_new(address_family)
        if not has_new:
            self.logger.debug(f'None found ({address_family}.')
            unset_running(self.__class__.__name__)
            return
        async with aiohttp.ClientSession() as session:
            self.logger.debug(f'Has new ({address_family}.')
            await self.download_routes(session, address_family, path)
        unset_running(self.__class__.__name__)

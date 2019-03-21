#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
import aiohttp
from datetime import date, timedelta
import asyncio

from .libs.helpers import safe_create_dir, set_running, unset_running


class RipeDownloader():

    def __init__(self, storage_directory: Path, collector: str='rrc00', hours: list=['0000'], loglevel: int=logging.DEBUG) -> None:
        self.__init_logger(loglevel)
        self.collector = collector
        self.hours = hours
        self.url = 'http://data.ris.ripe.net/{}'
        self.storage_root = storage_directory
        self.sema = asyncio.BoundedSemaphore(5)

    def __init_logger(self, loglevel: int):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.logger.setLevel(loglevel)

    async def download_routes(self, session: aiohttp.ClientSession, path: str) -> None:
        store_path = self.storage_root / 'ripe' / path
        if store_path.exists():
            # Already downloaded
            return
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
        set_running(self.__class__.__name__)
        cur_date = last_date
        tasks = []
        async with aiohttp.ClientSession() as session:
            while cur_date >= first_date:
                for hour in self.hours:
                    path = f'{self.collector}/{cur_date:%Y.%m}/bview.{cur_date:%Y%m%d}.{hour}.gz'
                    tasks.append(self.download_routes(session, path))
                cur_date -= timedelta(days=1)
            await asyncio.gather(*tasks, return_exceptions=True)
        unset_running(self.__class__.__name__)

    async def download_latest(self) -> None:
        set_running(self.__class__.__name__)
        self.logger.debug(f'Search for new routes.')
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
        unset_running(self.__class__.__name__)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from abc import ABC
import logging

from .libs.helpers import long_sleep, shutdown_requested, long_sleep_async


class AbstractManager(ABC):

    def __init__(self, loglevel: int=logging.DEBUG):
        self.loglevel = loglevel
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.logger.setLevel(loglevel)
        self.logger.info(f'Initializing {self.__class__.__name__}')

    async def _to_run_forever_async(self):
        pass

    def _to_run_forever(self):
        pass

    async def run_async(self, sleep_in_sec: int):
        self.logger.info(f'Launching {self.__class__.__name__} (async)')
        while True:
            if shutdown_requested():
                break
            try:
                await self._to_run_forever_async()
            except Exception:
                self.logger.exception(f'Something went terribly wrong in {self.__class__.__name__}.')
            if not await long_sleep_async(sleep_in_sec):
                break
        self.logger.info(f'Shutting down {self.__class__.__name__} (async)')

    def run(self, sleep_in_sec: int):
        self.logger.info(f'Launching {self.__class__.__name__}')
        while True:
            if shutdown_requested():
                break
            try:
                self._to_run_forever()
            except Exception:
                self.logger.exception(f'Something went terribly wrong in {self.__class__.__name__}.')
            if not long_sleep(sleep_in_sec):
                break
        self.logger.info(f'Shutting down {self.__class__.__name__}')

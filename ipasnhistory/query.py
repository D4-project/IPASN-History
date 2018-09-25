#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
from redis import StrictRedis
from .libs.helpers import get_socket_path
from datetime import datetime
from dateutil.parser import parse
import time

from collections import OrderedDict


class Query():

    def __init__(self, loglevel: int=logging.DEBUG):
        self.__init_logger(loglevel)
        self.cache = StrictRedis(unix_socket_path=get_socket_path('cache'), decode_responses=True)

    def __init_logger(self, loglevel) -> None:
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.logger.setLevel(loglevel)

    def nearest_date(self, source: str, address_family: str, date: str):
        dates = [parse(d) for d in self.cache.smembers(f'{source}|{address_family}|cached_dates')]
        return min(dates, key=lambda x: abs(x - parse(date))).isoformat()

    def find_interval(self, source: str, address_family: str, first: str, last: str=None):
        first = self.nearest_date(source, address_family, first)
        if not last:
            last = datetime.now().isoformat()
        last = self.nearest_date(source, address_family, last)
        return [d for d in self.cache.smembers(f'{source}|{address_family}|cached_dates')
                if d >= first and d <= last]

    def query(self, ip, source: str='caida', address_family: str='v4', date=None, first=None, last=None):
        if date:
            to_check = [self.nearest_date(source, address_family, date)]
        elif first:
            to_check = self.find_interval(source, address_family, first, last)
        else:
            # Assuming we want the latest possible date.
            to_check = [self.nearest_date(source, address_family, datetime.now().isoformat())]
        keys = [f'{source}|{address_family}|{d}|{ip}' for d in to_check]
        p = self.cache.pipeline()
        [p.sadd('query', k) for k in keys]
        p.execute()
        waiting = True
        to_return = {'meta': {'source': source, 'ip_version': address_family, 'ip': ip},
                     'response': {}}
        while waiting:
            waiting = False
            for k in keys:
                _, _, date, _ = k.split('|')
                if to_return['response'].get(date):
                    continue
                data = self.cache.hgetall(k)
                if not data:
                    waiting = True
                    continue
                to_return['response'][date] = data
            if waiting:
                time.sleep(.1)
        to_return['response'] = OrderedDict(sorted(to_return['response'].items(), key=lambda t: t[0]))
        return to_return

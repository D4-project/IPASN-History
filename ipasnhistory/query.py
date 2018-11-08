#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
from redis import StrictRedis
from .libs.helpers import get_socket_path
from datetime import datetime, timedelta
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

    def nearest_date(self, source: str, address_family: str, date: str, precision_delta: dict={}):
        dates = [parse(d) for d in self.cache.smembers(f'{source}|{address_family}|cached_dates')]
        if not dates:
            raise Exception(f'No route views have been loaded for {source} / {address_family} yet.')
        date = parse(date)
        nearest = min(dates, key=lambda x: abs(x - date))
        if precision_delta:
            min_date = date - timedelta(**precision_delta)
            max_date = date + timedelta(**precision_delta)
            if nearest < min_date or nearest > max_date:
                raise Exception(f'Unable to find a date in the expected interval: {min_date.isoformat()} -> {max_date.isoformat()}.')
        return nearest.isoformat()

    def find_interval(self, source: str, address_family: str, first: str, last: str=None):
        near_first = self.nearest_date(source, address_family, first)
        if not last:
            last = datetime.now().isoformat()
        near_last = self.nearest_date(source, address_family, last)
        if near_first <= last and near_last >= first:
            # We have something in the given interval
            return [d for d in self.cache.smembers(f'{source}|{address_family}|cached_dates') if d >= first and d <= last]
        raise Exception(f'No data available in the given interval: {first} -> {last}. Nearest data to first: {near_first.isoformat()}, nearest data to last: {near_last.isoformat()} ')

    def perdelta(self, start, end):
        curr = start
        while curr < end:
            yield curr
            curr += timedelta(days=1)

    def meta(self):
        sources = self.cache.smembers('META:sources')
        expected_interval = self.cache.hgetall('META:expected_interval')
        expected_dates = [date for date in self.perdelta(parse(expected_interval['first'].date(),
                                                               expected_interval['last']).date())]
        cached_dates_by_sources = {}
        for source in sources:
            cached_v4 = self.cache.smembers(f'{source}|v4|cached_dates')
            missing_v4 = [c for c in cached_v4 if parse(c).date() not in expected_dates]
            percent_v4 = float(len(expected_dates) - len(missing_v4)) * 100 / len(expected_dates)

            cached_v6 = self.cache.smembers(f'{source}|v6|cached_dates')
            missing_v6 = [c for c in cached_v6 if parse(c).date() not in expected_dates]
            percent_v6 = float(len(expected_dates) - len(missing_v6)) * 100 / len(expected_dates)

            cached_dates_by_sources[source] = {'v4': {'cached': cached_v4, 'missing': missing_v4, 'percent': percent_v4},
                                               'v6': {'cached': cached_v6, 'missing': missing_v6, 'percent': percent_v6}}

        return {'sources': sources, 'expected_interval': expected_interval,
                'cached_dates': cached_dates_by_sources}

    def query(self, ip, source: str='caida', address_family: str='v4', date: str=None, first: str=None, last: str=None, cache_only: bool=False, precision_delta: dict={}):
        '''Launch a query.
        :param ip: IP to lookup
        :param source: Source to query (currently, only caida is supported)
        :param address_family: v4 or v6
        :param date: Exact date to lookup. Fallback to most recent available.
        :param first: First date in the interval
        :param last: Last date in the interval
        :param cache_only: Do not wait for the response. Useful when an other process is expected to request the IP later on.
        :param precision_delta: Max delta allowed between the date queried and the one we have in the database. Expects a dictionary to pass to timedelta.
                                Example: {days=1, seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=0}
        '''
        to_return = {'meta': {'source': source, 'ip_version': address_family, 'ip': ip},
                     'response': {}}
        try:
            if date:
                to_check = [self.nearest_date(source, address_family, date, precision_delta)]
            elif first:
                to_check = self.find_interval(source, address_family, first, last)
            else:
                # Assuming we want the latest possible date.
                to_check = [self.nearest_date(source, address_family, datetime.now())]
        except Exception as e:
            to_return['error'] = str(e)
            return to_return

        keys = [f'{source}|{address_family}|{d}|{ip}' for d in to_check]
        p = self.cache.pipeline()
        [p.sadd('query', k) for k in keys]
        p.execute()
        if cache_only:
            to_return['info'] = 'Query for cache purposes only, not waiting for the lookup.'
            return to_return

        p_update_expire = self.cache.pipeline()
        waiting = True
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
                p_update_expire.expire(k, 43200)  # 12h
            if waiting:
                time.sleep(.1)
        to_return['response'] = OrderedDict(sorted(to_return['response'].items(), key=lambda t: t[0]))
        p_update_expire.execute()
        return to_return

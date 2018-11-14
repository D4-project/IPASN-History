#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
from redis import StrictRedis
from .libs.helpers import get_socket_path
from datetime import datetime, timedelta, timezone
from dateutil.parser import parse
import time
import copy

from collections import OrderedDict


class Query():

    def __init__(self, loglevel: int=logging.DEBUG):
        self.__init_logger(loglevel)
        self.cache = StrictRedis(unix_socket_path=get_socket_path('cache'), decode_responses=True)

    def __init_logger(self, loglevel) -> None:
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.logger.setLevel(loglevel)

    def nearest_date(self, cached_dates: set, source: str, address_family: str, date: str, precision_delta: dict={}):
        dates = [parse(d) for d in cached_dates]
        date = parse(date)
        if date.tzinfo:
            # Make sure the datetime isn't TZ aware, and UTC.
            date = date.astimezone(timezone.utc).replace(tzinfo=None)
        nearest = min(dates, key=lambda x: abs(x - date))
        if precision_delta:
            min_date = date - timedelta(**precision_delta)
            max_date = date + timedelta(**precision_delta)
            if nearest < min_date or nearest > max_date:
                raise Exception(f'Unable to find a date in the expected interval: {min_date.isoformat()} -> {max_date.isoformat()}.')
        return nearest.isoformat()

    def find_interval(self, cached_dates: set, source: str, address_family: str, first: str, last: str=None):
        if last and first > last:
            raise Exception(f'The first date of the interval ({first}) has to be before the last one ({last})...')
        near_first = self.nearest_date(cached_dates, source, address_family, first)
        if not last:
            last = datetime.now().isoformat()
        near_last = self.nearest_date(cached_dates, source, address_family, last)
        if near_first <= last and near_last >= first:
            # We have something in the given interval
            return [d for d in cached_dates if d >= first and d <= last]
        raise Exception(f'No data available in the given interval: {first} -> {last}. Nearest data to first: {near_first}, nearest data to last: {near_last} ')

    def perdelta(self, start, end):
        curr = start
        while curr < end:
            yield curr
            curr += timedelta(days=1)

    def meta(self):
        '''Get meta information from the current instance'''
        sources = self.cache.smembers('META:sources')
        expected_interval = self.cache.hgetall('META:expected_interval')
        expected_dates = set([date.isoformat() for date in self.perdelta(parse(expected_interval['first']).date(),
                                                                         parse(expected_interval['last']).date())])
        cached_dates_by_sources = {}
        for source in sources:
            cached_v4 = self.cache.smembers(f'{source}|v4|cached_dates')
            temp_cached_as_date = set([parse(c).date().isoformat() for c in cached_v4])
            missing_v4 = sorted(list(expected_dates - temp_cached_as_date))
            percent_v4 = float(len(expected_dates) - len(missing_v4)) * 100 / len(expected_dates)

            cached_v6 = self.cache.smembers(f'{source}|v6|cached_dates')
            temp_cached_as_date = set([parse(c).date().isoformat() for c in cached_v6])
            missing_v6 = sorted(list(expected_dates - temp_cached_as_date))
            percent_v6 = float(len(expected_dates) - len(missing_v6)) * 100 / len(expected_dates)

            cached_dates_by_sources[source] = {'v4': {'cached': sorted(list(cached_v4)), 'missing': missing_v4, 'percent': percent_v4},
                                               'v6': {'cached': sorted(list(cached_v6)), 'missing': missing_v6, 'percent': percent_v6}}

        return {'sources': list(sources), 'expected_interval': expected_interval,
                'cached_dates': cached_dates_by_sources}

    def mass_cache(self, list_to_cache):
        to_return = {}
        to_return['not_cached'] = []
        p = self.cache.pipeline()
        for to_cache in list_to_cache:
            try:
                cached_dates = self.cache.smembers(f'{to_cache["source"]}|{to_cache["address_family"]}|cached_dates')
                if not cached_dates:
                    raise Exception(f'No route views have been loaded for {to_cache["source"]} / {to_cache["address_family"]} yet.')

                date_search = copy.copy(to_cache)
                date_search.pop('ip')
                if 'date' in date_search or 'first' in date_search:
                    to_check = self.nearest_date(cached_dates, **date_search)
                else:
                    # Assuming we want the latest possible date.
                    to_check = self.nearest_date(cached_dates, date=datetime.now().isoformat(), **date_search)

                if isinstance(to_check, list):
                    keys = [f'{to_cache["source"]}|{to_cache["address_family"]}|{d}|{to_cache["ip"]}' for d in to_check]
                    [p.sadd('query', k) for k in keys]
                else:
                    self.cache.sadd('query', f'{to_cache["source"]}|{to_cache["address_family"]}|{to_check}|{to_cache["ip"]}')
            except Exception as e:
                to_return['not_cached'].append((to_cache, str(e)))
                logging.exception('woops')
        p.execute()
        return to_return

    def query(self, ip, source: str='caida', address_family: str='v4', date: str=None, first: str=None, last: str=None, precision_delta: dict={}):
        '''Launch a query.
        :param ip: IP to lookup
        :param source: Source to query (currently, only caida is supported)
        :param address_family: v4 or v6
        :param date: Exact date to lookup. Fallback to most recent available.
        :param first: First date in the interval
        :param last: Last date in the interval
        :param precision_delta: Max delta allowed between the date queried and the one we have in the database. Expects a dictionary to pass to timedelta.
                                Example: {days=1, seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=0}
        '''
        to_return = {'meta': {'source': source, 'ip_version': address_family, 'ip': ip},
                     'response': {}}
        try:
            cached_dates = self.cache.smembers(f'{source}|{address_family}|cached_dates')
            if not cached_dates:
                raise Exception(f'No route views have been loaded for {source} / {address_family} yet.')
            if date:
                to_check = self.nearest_date(cached_dates, source, address_family, date, precision_delta)
            elif first:
                to_check = self.find_interval(cached_dates, source, address_family, first, last)
            else:
                # Assuming we want the latest possible date.
                to_check = self.nearest_date(cached_dates, source, address_family, datetime.now().isoformat())
        except Exception as e:
            # self.logger.exception(e)
            to_return['error'] = str(e)
            return to_return

        if isinstance(to_check, list):
            keys = [f'{source}|{address_family}|{d}|{ip}' for d in to_check]
            p = self.cache.pipeline()
            [p.sadd('query', k) for k in keys]
            p.execute()
        else:
            self.cache.sadd('query', f'{source}|{address_family}|{to_check}|{ip}')
            keys = [f'{source}|{address_family}|{to_check}|{ip}']

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

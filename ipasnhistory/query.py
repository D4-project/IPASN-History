#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import time
import copy

from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional

from redis import Redis
from dateutil.parser import parse

from .default import get_socket_path, get_config


class Query():

    def __init__(self):
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.logger.setLevel(get_config('generic', 'loglevel'))
        self.cache = Redis(unix_socket_path=get_socket_path('cache'), decode_responses=True)
        self.storagedb = Redis(get_config('generic', 'storage_db_hostname'), get_config('generic', 'storage_db_port'), decode_responses=True)
        self.temp_cached_dates: Dict[str, Dict[str, Any]] = {}

    def nearest_date(self, cached_dates: set, source: str, address_family: str, date: str, precision_delta: Optional[Dict[str, int]]=None):
        dates = []
        for d in cached_dates:
            if isinstance(d, datetime):
                dates.append(d)
            else:
                dates.append(parse(d))

        parsed_date = parse(date)
        if parsed_date.tzinfo:
            # Make sure the datetime isn't TZ aware, and UTC.
            parsed_date = parsed_date.astimezone(timezone.utc).replace(tzinfo=None)
        nearest = min(dates, key=lambda x: abs(x - parsed_date))
        if precision_delta:
            min_date = parsed_date - timedelta(**precision_delta)
            max_date = parsed_date + timedelta(**precision_delta)
            if nearest < min_date or nearest > max_date:
                raise Exception(f'Unable to find a date in the expected interval: {min_date.isoformat()} -> {max_date.isoformat()}.')
        return nearest.isoformat()

    def find_interval(self, cached_dates: set, source: str, address_family: str, first: str, last: Optional[str]=None):
        if last and first > last:
            raise Exception(f'The first date of the interval ({first}) has to be before the last one ({last})...')
        near_first = self.nearest_date(cached_dates, source, address_family, first)
        if not last:
            last = datetime.now().isoformat()
        near_last = self.nearest_date(cached_dates, source, address_family, last)
        if near_first <= last and near_last >= first:
            # We have something in the given interval
            return [d.isoformat() for d in cached_dates if first <= d.isoformat() <= last]
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

    def _find_dates(self, **query_params):
        cached_key = f'{query_params["source"]}|{query_params["address_family"]}|cached_dates'
        if cached_key in self.temp_cached_dates and self.temp_cached_dates[cached_key]['cache_time'] >= (datetime.now() - timedelta(minutes=10)):
            cached_dates = self.temp_cached_dates[cached_key]['dates']
        else:
            cached_dates = self.cache.smembers(f'{query_params["source"]}|{query_params["address_family"]}|cached_dates')
            cached_dates = [parse(d) for d in cached_dates]
            self.temp_cached_dates[cached_key] = {'cache_time': datetime.now(), 'dates': cached_dates}

        if not cached_dates:
            raise Exception(f'No route views have been loaded for {query_params["source"]} / {query_params["address_family"]} yet.')

        date_search = copy.copy(query_params)
        # Pop all the keys not expected by either nearest_date or find_interval
        authorized_keys = ['source', 'address_family', 'date', 'first', 'last', 'precision_delta']
        [date_search.pop(k) for k in list(date_search.keys()) if k not in authorized_keys or not date_search[k]]
        if 'date' in date_search:
            dates = [self.nearest_date(cached_dates, **date_search)]
        elif 'first' in date_search:
            dates = self.find_interval(cached_dates, **date_search)
        else:
            # Assuming we want the latest possible date.
            dates = [self.nearest_date(cached_dates, date=datetime.now().isoformat(), **date_search)]
        return dates

    def mass_cache(self, list_to_cache: list):
        to_return = {'meta': {'number_queries': len(list_to_cache)}, 'not_cached': [], 'cached': []}
        p = self.cache.pipeline()
        for to_cache in list_to_cache:
            try:
                if 'source' not in to_cache:
                    to_cache['source'] = 'caida'
                if 'address_family' not in to_cache:
                    to_cache['address_family'] = 'v4'
                dates = self._find_dates(**to_cache)
                if len(dates) > 1:
                    keys = [f'{to_cache["source"]}|{to_cache["address_family"]}|{d}|{to_cache["ip"]}' for d in dates]
                    [p.sadd('query', k) for k in keys]
                    to_return['cached'] += keys  # type: ignore
                else:
                    key = f'{to_cache["source"]}|{to_cache["address_family"]}|{dates[0]}|{to_cache["ip"]}'
                    p.sadd('query', key)
                    to_return['cached'].append(key)  # type: ignore
            except Exception as e:
                to_return['not_cached'].append((to_cache, str(e)))  # type: ignore
        p.execute()
        return to_return

    def mass_query(self, list_to_query: list):
        to_return = {'meta': {'number_queries': len(list_to_query)}, 'responses': []}
        p = self.cache.pipeline()
        for to_query in list_to_query:
            to_append = {'meta': to_query, 'response': {}}
            try:
                if 'source' not in to_query:
                    to_query['source'] = 'caida'
                if 'address_family' not in to_query:
                    to_query['address_family'] = 'v4'
                dates = self._find_dates(**to_query)
                if len(dates) > 1:
                    keys = [f'{to_query["source"]}|{to_query["address_family"]}|{d}|{to_query["ip"]}' for d in dates]
                else:
                    keys = [f'{to_query["source"]}|{to_query["address_family"]}|{dates[0]}|{to_query["ip"]}']
                for k in keys:
                    _, _, date, _ = k.split('|')
                    data = self.cache.hgetall(k)
                    to_append['response'][date] = data
                    if data:
                        p.expire(k, 43200)  # 12h
                    else:
                        p.sadd('query', k)
            except Exception as e:
                self.logger.warning(f'Unable to run {to_query}. - {e}')
                # If something fails, it *has* to be in the list
                to_append['response']['error'] = str(e)
            finally:
                to_append['response'] = OrderedDict(sorted(to_append['response'].items(), key=lambda t: t[0]))
                to_return['responses'].append(to_append)  # type: ignore
        p.execute()
        return to_return

    def query(self, ip, source: str='caida', address_family: str='v4', date: Optional[str]=None,
              first: Optional[str]=None, last: Optional[str]=None, precision_delta: Optional[Dict[str, int]]=None):
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
        to_return: Dict = {'meta': {'source': source, 'address_family': address_family, 'ip': ip},
                           'response': {}}
        try:
            dates = self._find_dates(source=source, address_family=address_family, date=date,
                                     first=first, last=last, precision_delta=precision_delta)
        except Exception as e:
            to_return['error'] = str(e)
            return to_return

        if len(dates) > 1:
            keys = [f'{source}|{address_family}|{d}|{ip}' for d in dates]
            p = self.cache.pipeline()
            [p.sadd('query', k) for k in keys]
            p.execute()
        else:
            self.cache.sadd('query', f'{source}|{address_family}|{dates[0]}|{ip}')
            keys = [f'{source}|{address_family}|{dates[0]}|{ip}']

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

    def asn_meta(self, asn: Optional[int]=None, source: str='caida', address_family: str='v4',
                 date: Optional[str]=None, first: Optional[str]=None, last: Optional[str]=None,
                 precision_delta: Optional[Dict[str, int]]=None):
        to_return: Dict = {'meta': {'source': source, 'address_family': address_family},
                           'response': {}}
        if asn is not None:
            to_return['meta']['asn'] = asn
        try:
            dates = self._find_dates(source=source, address_family=address_family, date=date,
                                     first=first, last=last, precision_delta=precision_delta)
        except Exception as e:
            to_return['error'] = str(e)
            return to_return

        for date in dates:
            data = {}
            if asn is None:
                asns = self.storagedb.smembers(f'{source}|{address_family}|{date}|asns')
            else:
                asns = {asn, }  # type: ignore
            for _a in asns:
                prefixes = self.storagedb.smembers(f'{source}|{address_family}|{date}|{_a}')
                ipcount = self.storagedb.get(f'{source}|{address_family}|{date}|{_a}|ipcount')
                data[_a] = {'prefixes': list(prefixes), 'ipcount': ipcount}
            to_return['response'][date] = data
        return to_return

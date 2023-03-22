#!/usr/bin/env python3
import ipaddress
import logging
import time

from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional, List, Tuple

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
        self.sources = get_config('generic', 'sources')

    def nearest_date(self, cached_dates: set, source: str, address_family: str,
                     date: str, precision_delta: Optional[Dict[str, int]]=None):
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
                raise Exception(f'Unable to find a date in the expected interval: {min_date.isoformat()} -> {max_date.isoformat()} for {source}.')
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
        expected_interval = self.cache.hgetall('META:expected_interval')
        expected_dates = {date.isoformat() for date in self.perdelta(parse(expected_interval['first']).date(),
                                                                     parse(expected_interval['last']).date())}
        cached_dates_by_sources = {}
        for source in self.sources:
            cached_v4 = self.cache.smembers(f'{source}|v4|cached_dates')
            temp_cached_as_date = {parse(c).date().isoformat() for c in cached_v4}
            missing_v4 = sorted(list(expected_dates - temp_cached_as_date))
            percent_v4 = float(len(expected_dates) - len(missing_v4)) * 100 / len(expected_dates)

            cached_v6 = self.cache.smembers(f'{source}|v6|cached_dates')
            temp_cached_as_date = {parse(c).date().isoformat() for c in cached_v6}
            missing_v6 = sorted(list(expected_dates - temp_cached_as_date))
            percent_v6 = float(len(expected_dates) - len(missing_v6)) * 100 / len(expected_dates)

            cached_dates_by_sources[source] = {'v4': {'cached': sorted(list(cached_v4)), 'missing': missing_v4, 'percent': percent_v4},
                                               'v6': {'cached': sorted(list(cached_v6)), 'missing': missing_v6, 'percent': percent_v6}}

        return {'sources': self.sources, 'expected_interval': expected_interval,
                'cached_dates': cached_dates_by_sources}

    def _find_dates(self, source: str, address_family: str, *, date: Optional[str]=None,
                    first: Optional[str]=None, last: Optional[str]=None, precision_delta: Optional[Dict[str, int]]=None):
        cached_key = f'{source}|{address_family}|cached_dates'
        if cached_key in self.temp_cached_dates and self.temp_cached_dates[cached_key]['cache_time'] >= (datetime.now() - timedelta(minutes=10)):
            cached_dates = self.temp_cached_dates[cached_key]['dates']
        else:
            cached_dates = self.cache.smembers(f'{source}|{address_family}|cached_dates')
            cached_dates = [parse(d) for d in cached_dates]
            self.temp_cached_dates[cached_key] = {'cache_time': datetime.now(), 'dates': cached_dates}

        if not cached_dates:
            raise Exception(f'No route views have been loaded for {source} / {address_family} yet.')

        if date:
            dates = [self.nearest_date(cached_dates, source, address_family, date, precision_delta)]
        elif first:
            dates = self.find_interval(cached_dates, source, address_family, first, last)
        else:
            # Assuming we want the latest possible date.
            dates = [self.nearest_date(cached_dates, source, address_family, datetime.now().isoformat(), precision_delta)]
        return dates

    def _keys_for_query(self, query: Dict) -> List[str]:
        if 'source' in query:
            sources = [query['source']]
        else:
            sources = self.sources
        to_return = []
        for source in sources:
            if 'address_family' not in query:
                if ':' in query['ip']:
                    address_family = 'v6'
                else:
                    address_family = 'v4'
            else:
                address_family = query['address_family']
            dates = self._find_dates(source, address_family, date=query.get('date'),
                                     first=query.get('first'), last=query.get('last'),
                                     precision_delta=query.get('precision_delta'))
            if len(dates) > 1:
                to_return += [f'{source}|{address_family}|{d}|{query["ip"]}' for d in dates]
            else:
                to_return.append(f'{source}|{address_family}|{dates[0]}|{query["ip"]}')
        return to_return

    def _prepare_all_keys(self, queries: List[Dict]) -> Tuple[List[str], List[Tuple[Dict, str]]]:
        keys: List[str] = []
        invalid_queries: List[Tuple[Dict, str]] = []
        for query in queries:
            try:
                keys += self._keys_for_query(query)
            except Exception as e:
                invalid_queries.append((query, str(e)))

        return keys, invalid_queries

    def mass_cache(self, list_to_cache: list):
        to_return: Dict[str, Any] = {'meta': {'number_queries': len(list_to_cache)}, 'not_cached': [], 'cached': []}
        keys, invalid_queries = self._prepare_all_keys(list_to_cache)
        self.cache.sadd('query', *keys)
        to_return['cached'] = keys
        to_return['not_cached'] = invalid_queries
        return to_return

    def mass_query(self, list_to_query: list):
        to_return = {'meta': {'number_queries': len(list_to_query)}, 'responses': []}
        keys, invalid_queries = self._prepare_all_keys(list_to_query)

        p = self.cache.pipeline()
        for to_query in list_to_query:
            to_append = {'meta': to_query, 'response': {}}
            responses: Dict = {}
            try:
                for k in self._keys_for_query(to_query):
                    _, _, date, _ = k.split('|')
                    data = self.cache.hgetall(k)
                    if (data and date in responses
                            and ('asn' in data and data['asn'] not in [None, 0, '0'])
                            and ('prefix' in data and data['prefix'] not in [None, '0.0.0.0/0', '::/0'])):
                        # we have more than one query for the same date, find the best one
                        if ipaddress.ip_network(data['prefix']).num_addresses < ipaddress.ip_network(responses[date]['prefix']).num_addresses:
                            responses[date] = data
                    else:
                        responses[date] = data
                    if data:
                        p.expire(k, 43200)  # 12h
                    else:
                        p.sadd('query', k)
            except Exception as e:
                self.logger.warning(f'Unable to run {to_query}. - {e}')
                # If something fails, it *has* to be in the list
                to_append['error'] = str(e)
            finally:
                if 'error' not in to_append:
                    sorted_responses = OrderedDict(sorted(responses.items(), key=lambda t: t[0], reverse=True))
                    if 'first' in to_query:
                        # working on an interval, return everything
                        to_append['response'] = sorted_responses
                    else:
                        # specific date, return most recent valid answer (if any)
                        if (tmp := {date: entry for date, entry in sorted_responses.items() if entry and entry['asn'] not in ['0', 0]}):
                            to_append['response'] = tmp
                        else:
                            to_append['response'] = sorted_responses
                to_return['responses'].append(to_append)  # type: ignore
        p.execute()
        return to_return

    def query(self, ip, source: Optional[str]=None, address_family: Optional[str]=None, date: Optional[str]=None,
              first: Optional[str]=None, last: Optional[str]=None, precision_delta: Optional[Dict[str, int]]=None):
        '''Launch a query.
        :param ip: IP to lookup
        :param source: Source to query
        :param address_family: v4 or v6
        :param date: Exact date to lookup. Fallback to most recent available.
        :param first: First date in the interval
        :param last: Last date in the interval
        :param precision_delta: Max delta allowed between the date queried and the one we have in the database. Expects a dictionary to pass to timedelta.
                                Example: {days=1, seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=0}
        '''

        query = {'ip': ip}
        if source:
            query['source'] = source
        if address_family:
            query['address_family'] = address_family

        if date:
            query['date'] = date
        elif first:
            query['first'] = first
            if last:
                query['last'] = last

        if precision_delta:
            query['precision_delta'] = precision_delta

        to_return: Dict = {'meta': query, 'response': {}}
        try:
            keys = self._keys_for_query(query)
        except Exception as e:
            to_return['error'] = str(e)
            return to_return

        self.cache.sadd('query', *keys)

        waiting = True
        responses: Dict = {}
        p_update_expire = self.cache.pipeline()
        while waiting:
            waiting = False
            for k in keys:
                _source, _address_family, _date, _ip = k.split('|')
                if _date in responses and responses[_date]['source'] == _source:
                    # same source
                    continue

                data = self.cache.hgetall(k)
                if not data:
                    waiting = True
                    continue
                data['source'] = _source
                if (_date in responses
                        and ('asn' in data and data['asn'] not in [None, 0, '0'])
                        and ('prefix' in data and data['prefix'] not in [None, '0.0.0.0/0', '::/0'])):
                    # we have more than one query for the same date, find the best one
                    if ipaddress.ip_network(data['prefix']).num_addresses < ipaddress.ip_network(responses[_date]['prefix']).num_addresses:
                        responses[date] = data
                else:
                    responses[_date] = data
                p_update_expire.expire(k, 43200)  # 12h
            if waiting:
                time.sleep(.1)
        p_update_expire.execute()
        sorted_responses = OrderedDict(sorted(responses.items(), key=lambda t: t[0], reverse=True))
        if first:
            # working on an interval, return everything
            to_return['response'] = sorted_responses
        else:
            # specific date, return most recent valid answer (if any)
            tmp = {date: entry for date, entry in sorted_responses.items() if entry and entry['asn'] != '0'}
            if tmp:
                to_return['response'] = tmp
            else:
                to_return['response'] = sorted_responses
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

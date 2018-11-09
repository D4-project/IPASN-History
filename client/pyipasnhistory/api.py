#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import requests


class IPASNHistory():

    def __init__(self, root_url: str):
        self.root_url = root_url.rstrip('/')

    def meta(self):
        '''Get meta information from the remote instance'''
        r = requests.get(f'{self.root_url}/meta')
        return r.json()

    def query(self, ip: str, source: str='caida', address_family: str='v4',
              date: str=None, first: str=None, last: str=None,
              cache_only: bool=False, precision_delta: dict={}):
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
        to_query = {'ip': ip, 'source': source, 'address_family': address_family}
        if date:
            to_query['date'] = date
        elif first:
            to_query['first'] = first
            if last:
                to_query['last'] = last
        if cache_only:
            to_query['cache_only'] = cache_only
        if precision_delta:
            to_query['precision_delta'] = json.dumps(precision_delta)

        r = requests.post(self.root_url, data=to_query)
        return r.json()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
from redis import StrictRedis
from .libs.helpers import set_running, unset_running, get_socket_path
import pytricia
from datetime import date
from typing import List
from .abstractmanager import AbstractManager


class Lookup(AbstractManager):

    def __init__(self, source: str, dates: List[date], loglevel: int=logging.DEBUG):
        self.__init_logger(loglevel)
        self.storagedb = StrictRedis(unix_socket_path=get_socket_path('storage'), decode_responses=True)
        self.cache = StrictRedis(unix_socket_path=get_socket_path('cache'), decode_responses=True)
        self.source = source
        self.dates = [d.isoformat() for d in dates]
        self.loaded_dates = []
        self.trees_v4 = {source: {d: pytricia.PyTricia() for d in self.dates}}
        self.trees_v6 = {source: {d: pytricia.PyTricia(128) for d in self.dates}}
        self.static = False

    def __init_logger(self, loglevel) -> None:
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.logger.setLevel(loglevel)

    def locked(self):
        # Avoid to see scripts providing data for the same time frame to be locked at the same time
        locked_dates = self.cache.smembers(f'lock|{self.source}')
        if set(self.dates).intersection(locked_dates):
            return True
        return False

    def load_all(self):
        if self.static or self.locked():
            return
        self.cache.sadd(f'lock|{self.source}', *self.dates)
        for d in self.dates:
            if not self.trees_v4[self.source][d] and self.storagedb.sismember(f'{self.source}|v4|dates', d):
                self.load_tree(d, 'v4')
            if not self.trees_v6[self.source][d] and self.storagedb.sismember(f'{self.source}|v6|dates', d):
                self.load_tree(d, 'v6')
            if self.trees_v4[self.source][d] and self.trees_v6[self.source][d]:
                self.loaded_dates.append(d)
        if sorted(self.dates) == sorted(self.loaded_dates):
            # All the expected dates are loaded, don't touch that dataset anymore
            self.static = True
        self.cache.srem(f'lock|{self.source}', *self.dates)

    def load_tree(self, announces_date: str, address_family: str):
        logging.debug(f'Loading {self.source} {address_family} {announces_date}')
        asns = self.storagedb.smembers(f'{self.source}|{address_family}|{announces_date}|asns')

        p = self.storagedb.pipeline()
        [p.smembers(f'{self.source}|{address_family}|{announces_date}|{asn}') for asn in asns]
        to_load = p.execute()

        for asn, ip_prefixes in zip(asns, to_load):
            for ip_prefix in ip_prefixes:
                if address_family == 'v4':
                    self.trees_v4[self.source][announces_date][ip_prefix] = asn
                elif address_family == 'v6':
                    self.trees_v6[self.source][announces_date][ip_prefix] = asn
                else:
                    raise Exception(f'address_family has to be v4 or v6, not {address_family}')
        self.cache.sadd(f'{self.source}|{address_family}|cached_dates', announces_date)
        logging.debug(f'Done with Loading {self.source} {address_family}')

    def _to_run_forever(self):
        set_running(self.__class__.__name__)
        while True:
            self.load_all()
            queries = self.cache.srandmember('query', 20)
            if not queries:
                break
            p = self.cache.pipeline()
            for q in queries:
                prefix, address_family, date, ip = q.split('|')
                if prefix != self.source:
                    # query for an other data source, ignore
                    continue
                if date not in self.loaded_dates:
                    # Date not loaded in this process, ignore
                    continue
                if address_family == 'v4':
                    trees = self.trees_v4
                else:
                    trees = self.trees_v6
                p.hmset(q, {'asn': trees[prefix][date].get(ip),
                            'prefix': trees[prefix][date].get_key(ip)})
                p.expire(q, 43200)  # 12h
                p.srem('query', q)
            p.execute()
        unset_running(self.__class__.__name__)

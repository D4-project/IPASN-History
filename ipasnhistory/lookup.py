#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
from redis import StrictRedis
from .libs.helpers import set_running, unset_running, get_socket_path
import pytricia


class Lookup():

    def __init__(self, loglevel: int=logging.DEBUG):
        self.__init_logger(loglevel)
        self.trees_v4 = {}
        self.trees_v6 = {}
        self.storagedb = StrictRedis(unix_socket_path=get_socket_path('storage'), decode_responses=True)
        self.cache = StrictRedis(unix_socket_path=get_socket_path('cache'), decode_responses=True)

    def __init_logger(self, loglevel) -> None:
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.logger.setLevel(loglevel)

    def load_all(self):
        for prefix in self.storagedb.smembers('prefixes'):
            self.load_prefix(prefix, 'v4')
            self.load_prefix(prefix, 'v6')

    def load_prefix(self, prefix: str, address_family: str):
        set_running(self.__class__.__name__)
        if address_family == 'v4':
            trees = self.trees_v4
        else:
            trees = self.trees_v6
        if not trees.get(prefix):
            trees[prefix] = {}
        for date in self.storagedb.smembers(f'{prefix}|{address_family}|dates'):
            logging.debug(f'Loading {prefix} {address_family} {date}')
            if trees[prefix].get(date):
                continue

            if address_family == 'v4':
                trees[prefix][date] = pytricia.PyTricia()
            else:
                trees[prefix][date] = pytricia.PyTricia(128)

            asns = self.storagedb.smembers(f'{prefix}|{address_family}|{date}|asns')
            p = self.storagedb.pipeline()
            for asn in asns:
                p.smembers(f'{prefix}|{address_family}|{date}|{asn}')
            to_load = p.execute()
            for asn, ip_prefixes in zip(asns, to_load):
                for ip_prefix in ip_prefixes:
                    trees[prefix][date][ip_prefix] = asn
            self.cache.sadd(f'{prefix}|{address_family}|cached_dates', date)
        logging.debug(f'Done with Loading {prefix} {address_family}')
        unset_running(self.__class__.__name__)

    def lookup(self):
        set_running(self.__class__.__name__)
        while True:
            queries = self.cache.spop('query', 20)
            if not queries:
                break
            p = self.cache.pipeline()
            for q in queries:
                prefix, address_family, date, ip = q.split('|')
                if address_family == 'v4':
                    trees = self.trees_v4
                else:
                    trees = self.trees_v6
                if not trees.get(prefix):
                    # When we will have multiple lookup modules in on different prefixes/sources
                    p.sadd('query', q)
                    continue
                if not trees[prefix].get(date):
                    # When we will have multiple lookup modules in on different time intervals
                    p.sadd('query', q)
                    continue
                p.hmset(q, {'asn': trees[prefix][date].get(ip),
                            'prefix': trees[prefix][date].get_key(ip)})
                p.expire(q, 43200)  # 12h
            p.execute()
        unset_running(self.__class__.__name__)

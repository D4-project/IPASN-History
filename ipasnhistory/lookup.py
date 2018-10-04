#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
from redis import StrictRedis
from .libs.helpers import set_running, unset_running, get_socket_path
import pytricia
from .abstractmanager import AbstractManager


class Lookup(AbstractManager):

    def __init__(self, source: str, first: str, last: str, loglevel: int=logging.DEBUG):
        self.__init_logger(loglevel)
        self.storagedb = StrictRedis(unix_socket_path=get_socket_path('storage'), decode_responses=True)
        self.cache = StrictRedis(unix_socket_path=get_socket_path('cache'), decode_responses=True)

        self.source = source
        self.first_date = first
        self.last_date = last

        self.trees = {'v4': {source: {}}, 'v6': {source: {}}}
        self.loaded_dates = {'v4': [], 'v6': []}

        # For the initial load, we don't care about the locks and want to load everything as fast as possible.
        self.load_all(ignore_lock=True)

    def __init_logger(self, loglevel) -> None:
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.logger.setLevel(loglevel)

    def locked(self, address_family: str):
        # Avoid to see scripts providing data for the same time frame to be locked at the same time
        for locked_interval in self.cache.smembers(f'lock|{self.source}|{address_family}'):
            locked_first, locked_last = locked_interval.split('_')
            if (locked_first <= self.first_date <= locked_last) or (locked_first <= self.last_date <= locked_last):
                logging.debug(f'Locked: {self.first_date} {self.last_date} because of {locked_first} {locked_last}')
                return True
        return False

    def load_all(self, ignore_lock: bool=False):
        for address_family in ['v4', 'v6']:
            if not ignore_lock and self.locked(address_family):
                continue
            self.cache.sadd(f'lock|{self.source}|{address_family}', f'{self.first_date}_{self.last_date}')
            available_dates = self.storagedb.smembers(f'{self.source}|{address_family}|dates')
            to_load = [available_date for available_date in available_dates if available_date >= self.first_date and available_date <= self.last_date]
            for d in to_load:
                if self.trees[address_family][self.source].get(d) is None:
                    self.trees[address_family][self.source][d] = pytricia.PyTricia()
                if not self.trees[address_family][self.source][d]:
                    self.load_tree(d, address_family)
                    self.loaded_dates[address_family].append(d)
            self.cache.srem(f'lock|{self.source}|{address_family}', f'{self.first_date}_{self.last_date}')

    def load_tree(self, announces_date: str, address_family: str):
        logging.debug(f'Loading {self.source} {address_family} {announces_date}')
        asns = self.storagedb.smembers(f'{self.source}|{address_family}|{announces_date}|asns')

        p = self.storagedb.pipeline()
        [p.smembers(f'{self.source}|{address_family}|{announces_date}|{asn}') for asn in asns]
        to_load = p.execute()

        for asn, ip_prefixes in zip(asns, to_load):
            for ip_prefix in ip_prefixes:
                self.trees[address_family][self.source][announces_date][ip_prefix] = asn
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
                logging.debug(f'Searching {q}')
                prefix, address_family, date, ip = q.split('|')
                if prefix != self.source:
                    # query for an other data source, ignore
                    continue
                if date not in self.loaded_dates[address_family]:
                    # Date not loaded in this process, ignore
                    continue
                p.hmset(q, {'asn': self.trees[address_family][prefix][date].get(ip),
                            'prefix': self.trees[address_family][prefix][date].get_key(ip)})
                p.expire(q, 43200)  # 12h
                p.srem('query', q)
            p.execute()
        unset_running(self.__class__.__name__)

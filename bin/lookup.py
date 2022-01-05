#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
import logging

from typing import Dict, List

from redis import Redis
import pytricia  # type: ignore

from ipasnhistory.default import AbstractManager, get_socket_path, get_config


class Lookup(AbstractManager):

    def __init__(self, source: str, first: str, last: str, loglevel: int=logging.DEBUG):
        super().__init__(loglevel)
        self.script_name = "lookup"

        self.storagedb = Redis(get_config('generic', 'storage_db_hostname'), get_config('generic', 'storage_db_port'), decode_responses=True)
        self.cache = Redis(unix_socket_path=get_socket_path('cache'), decode_responses=True)

        self.source = source
        self.first_date = first
        self.last_date = last

        self.trees: Dict[str, Dict[str, Dict]] = {'v4': {source: {}}, 'v6': {source: {}}}
        self.loaded_dates: Dict[str, List] = {'v4': [], 'v6': []}

        # For the initial load, we don't care about the locks and want to load everything as fast as possible.
        self.load_all(ignore_lock=True)

    def locked(self, address_family: str):
        # Avoid to see scripts providing data for the same time frame to be locked at the same time
        for locked_interval in self.cache.smembers(f'lock|{self.source}|{address_family}'):
            locked_first, locked_last = locked_interval.split('_')
            if (locked_first <= self.first_date <= locked_last) or (locked_first <= self.last_date <= locked_last):
                self.logger.debug(f'Locked: {self.first_date} {self.last_date} because of {locked_first} {locked_last}')
                return True
        return False

    def load_all(self, ignore_lock: bool=False):
        for address_family in ['v4', 'v6']:
            if not ignore_lock and self.locked(address_family):
                continue
            available_dates = self.storagedb.smembers(f'{self.source}|{address_family}|dates')
            to_load = [available_date for available_date in available_dates if available_date >= self.first_date and available_date <= self.last_date]
            if not set(to_load).difference(set(self.trees[address_family][self.source].keys())):
                # Everything available has already been loaded
                continue
            if not ignore_lock:
                self.cache.sadd(f'lock|{self.source}|{address_family}', f'{self.first_date}_{self.last_date}')
            for d in to_load:
                if self.trees[address_family][self.source].get(d) is None:
                    if address_family == 'v4':
                        self.trees[address_family][self.source][d] = pytricia.PyTricia()
                    else:
                        self.trees[address_family][self.source][d] = pytricia.PyTricia(128)
                if not self.trees[address_family][self.source][d]:
                    self.load_tree(d, address_family)
                    self.loaded_dates[address_family].append(d)
            if not ignore_lock:
                self.cache.srem(f'lock|{self.source}|{address_family}', f'{self.first_date}_{self.last_date}')

    def load_tree(self, announces_date: str, address_family: str):
        self.logger.debug(f'Loading {self.source} {address_family} {announces_date}')
        asns = self.storagedb.smembers(f'{self.source}|{address_family}|{announces_date}|asns')

        p = self.storagedb.pipeline()
        [p.smembers(f'{self.source}|{address_family}|{announces_date}|{asn}') for asn in asns]
        to_load = p.execute()

        for asn, ip_prefixes in zip(asns, to_load):
            for ip_prefix in ip_prefixes:
                self.trees[address_family][self.source][announces_date][ip_prefix] = asn
        self.cache.sadd(f'{self.source}|{address_family}|cached_dates', announces_date)
        self.logger.debug(f'Done with Loading {self.source} {address_family}')

    def _to_run_forever(self):
        while True:
            self.load_all()
            queries: List[str] = self.cache.srandmember('query', 20)  # type: ignore
            if not queries:
                break
            p = self.cache.pipeline()
            for q in queries:
                if self.cache.exists(q):
                    # The query is already cached, cleanup.
                    self.cache.srem('query', q)
                    continue
                self.logger.debug(f'Searching {q}')
                prefix, address_family, date, ip = q.split('|', 3)
                if prefix != self.source:
                    # query for an other data source, ignore
                    continue
                if date not in self.loaded_dates[address_family]:
                    # Date not loaded in this process, ignore
                    continue
                try:
                    asn = self.trees[address_family][prefix][date].get(ip)
                    ip_prefix = self.trees[address_family][prefix][date].get_key(ip)
                    if asn is None or ip_prefix is None:
                        self.logger.warning(f'Unable to find ASN ({asn}) and/or IP Prefix ({ip_prefix}): "{address_family}" "{prefix}" "{date}" "{ip}"')
                        asn = 0
                        if address_family == 'v4':
                            ip_prefix = '0.0.0.0/0'
                        else:
                            ip_prefix = '::/0'
                    if ip_prefix in ['0.0.0.0/0', '::/0']:
                        # Make sure not to return an ASN if we have no prefix.
                        asn = 0
                    p.hmset(q, {'asn': asn, 'prefix': ip_prefix})
                except ValueError:
                    p.hmset(q, {'error': f'Query invalid: "{address_family}" "{prefix}" "{date}" "{ip}"'})
                    self.logger.warning(f'Query invalid: "{address_family}" "{prefix}" "{date}" "{ip}"')
                finally:
                    p.expire(q, 43200)  # 12h
                    p.srem('query', q)
            p.execute()


def main():
    parser = argparse.ArgumentParser(description='Cache prefix announcements on a specific timeframe.')
    parser.add_argument('source', help='Dataset source name (must be caida for now).')
    parser.add_argument('first_date', help='First date in the interval.')
    parser.add_argument('last_date', help='Last date in the interval.')
    args = parser.parse_args()
    lookup = Lookup(args.source, args.first_date, args.last_date)
    lookup.run(sleep_in_sec=1)


if __name__ == '__main__':
    main()

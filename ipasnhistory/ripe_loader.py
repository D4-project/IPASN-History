#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from redis import StrictRedis
from .libs.helpers import set_running, unset_running, get_socket_path, shutdown_requested, get_homedir
import re
from collections import defaultdict
from ipaddress import ip_network
from datetime import datetime

from bgpdumpy import TableDumpV2, BGPDump
from socket import AF_INET


def routeview(bview_file: Path, libbgpdump_path: Path=None):

    def find_best_non_AS_set(originatingASs):
        pass

    if not libbgpdump_path:
        libbgpdump_path = get_homedir() / 'bgpdump' / 'libbgpdump.so'
        if not libbgpdump_path.exists():
            raise Exception(f'The path to the library is invalid: {libbgpdump_path}')

    routes = {'v4': [], 'v6': []}

    with BGPDump(bview_file, libbgpdump_path) as bgp:

        for entry in bgp:

            # entry.body can be either be TableDumpV1 or TableDumpV2

            if not isinstance(entry.body, TableDumpV2):
                continue  # I expect an MRT v2 table dump file

            # get a string representation of this prefix
            prefix = f'{entry.body.prefix}/{entry.body.prefixLength}'

            # get a list of each unique originating ASN for this prefix
            all_paths = [[asn for asn in route.attr.asPath.split()] for route in entry.body.routeEntries]

            # Cleanup the AS Sets
            for asn in reversed(all_paths[-1]):
                if asn.isnumeric():
                    best_as = asn
                    break
                elif asn[1:-1].isnumeric():
                    best_as = asn[1:-1]
                    break

            if entry.body.afi == AF_INET:
                routes['v4'].append((prefix, best_as))
            else:
                routes['v6'].append((prefix, best_as))

        return routes


class RipeLoader():

    def __init__(self, collector: str, storage_directory: Path, loglevel: int=logging.DEBUG) -> None:
        self.__init_logger(loglevel)
        self.collector = collector
        self.key_prefix = f'ripe_{self.collector}'
        self.storage_root = storage_directory / 'ripe' / self.collector
        self.storagedb = StrictRedis(unix_socket_path=get_socket_path('storage'), decode_responses=True)
        self.storagedb.sadd('prefixes', self.key_prefix)
        self.cache = StrictRedis(unix_socket_path=get_socket_path('cache'), decode_responses=True)

    def __init_logger(self, loglevel) -> None:
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.logger.setLevel(loglevel)

    def already_loaded(self, date: str) -> bool:
        return (self.storagedb.sismember(f'{self.key_prefix}|v4|dates', date)
                and self.storagedb.sismember(f'{self.key_prefix}|v6|dates', date))

    def update_last(self, address_family: str, date: str) -> None:
        cur_last = self.storagedb.get(f'{self.key_prefix}|{address_family}|last')
        if not cur_last or date > cur_last:
            self.storagedb.set(f'{self.key_prefix}|{address_family}|last', date)

    def load_all(self):
        set_running(self.__class__.__name__)
        for path in sorted(self.storage_root.glob('**/*.gz'), reverse=True):
            if shutdown_requested():
                break
            date_str = re.findall('.*/bview.(.*).gz', str(path))[0]
            date = datetime.strptime(date_str, '%Y%m%d.%H%M').isoformat()

            oldest_to_load = self.cache.hget('META:expected_interval', 'first')
            if oldest_to_load and oldest_to_load > date:
                # The RIPE dump we're trying to load is older than the oldest date we want to cache, skipping.
                continue

            if self.already_loaded(date):
                self.logger.debug(f'Already loaded {path}')
                continue
            self.logger.info(f'Loading {path}')
            routes = routeview(path)
            self.logger.debug('Content loaded')
            for address_family, entries in routes.items():
                to_import = defaultdict(lambda: {address_family: set(), 'ipcount': 0})
                for prefix, asn in entries:
                    network = ip_network(prefix)
                    to_import[asn][address_family].add(str(network))
                    to_import[asn]['ipcount'] += network.num_addresses
                p = self.storagedb.pipeline()
                p.sadd(f'{self.key_prefix}|{address_family}|dates', date)
                p.sadd(f'{self.key_prefix}|{address_family}|{date}|asns', *to_import.keys())  # Store all ASNs
                for asn, data in to_import.items():
                    p.sadd(f'{self.key_prefix}|{address_family}|{date}|{asn}', *data[address_family])  # Store all prefixes
                    p.set(f'{self.key_prefix}|{address_family}|{date}|{asn}|ipcount', data['ipcount'])  # Total IPs for the AS
                self.logger.debug('All keys ready')
                p.execute()
                self.update_last(address_family, date)
            self.logger.info(f'Done with {path}')

            self.logger.debug('Done.')
        unset_running(self.__class__.__name__)

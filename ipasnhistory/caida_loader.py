#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from redis import StrictRedis
from .libs.helpers import set_running, unset_running, get_socket_path, shutdown_requested
import re
from dateutil.parser import parse
from collections import defaultdict
import gzip
from ipaddress import ip_network


class CaidaLoader():

    def __init__(self, storage_directory: Path, loglevel: int=logging.DEBUG) -> None:
        self.__init_logger(loglevel)
        self.key_prefix = 'caida'
        self.storage_root = storage_directory / 'caida'
        self.storagedb = StrictRedis(unix_socket_path=get_socket_path('storage'), decode_responses=True)
        self.storagedb.sadd('prefixes', self.key_prefix)
        self.cache = StrictRedis(unix_socket_path=get_socket_path('cache'), decode_responses=True)

    def __init_logger(self, loglevel) -> None:
        self.logger = logging.getLogger(f'{self.__class__.__name__}')
        self.logger.setLevel(loglevel)

    def already_loaded(self, address_family: str, date: str) -> bool:
        return self.storagedb.sismember(f'{self.key_prefix}|{address_family}|dates', date)

    def update_last(self, address_family: str, date: str) -> None:
        cur_last = self.storagedb.get(f'{self.key_prefix}|{address_family}|last')
        if not cur_last or date > cur_last:
            self.storagedb.set(f'{self.key_prefix}|{address_family}|last', date)

    def load_all(self):
        set_running(self.__class__.__name__)
        for path in sorted(self.storage_root.glob('**/*.gz'), reverse=True):
            if shutdown_requested():
                break
            address_family, year, month, date_str = re.findall('.*/(.*)/(.*)/(.*)/routeviews-rv[2,6]-(.*).pfx2as.gz', str(path))[0]
            date = parse(date_str).isoformat()

            oldest_to_load = self.cache.hget('META:expected_interval', 'first')
            if oldest_to_load and oldest_to_load > date:
                # The CAIDA dump we're trying to load is older than the oldest date we want to cache, skipping.
                continue

            if self.already_loaded(address_family, date):
                self.logger.debug(f'Already loaded {path}')
                continue
            self.logger.info(f'Loading {path}')
            to_import = defaultdict(lambda: {address_family: set(), 'ipcount': 0})
            with gzip.open(path) as f:
                for line in f:
                    prefix, length, asns = line.decode().strip().split('\t')
                    # The meaning of AS set and multi-origin AS in unclear. Taking the first ASN in the list only.
                    asn = re.split('[,_]', asns)[0]
                    network = ip_network(f'{prefix}/{length}')
                    to_import[asn][address_family].add(str(network))
                    to_import[asn]['ipcount'] += network.num_addresses

            self.logger.debug('Content loaded')
            p = self.storagedb.pipeline()
            p.sadd(f'{self.key_prefix}|{address_family}|dates', date)
            p.sadd(f'{self.key_prefix}|{address_family}|{date}|asns', *to_import.keys())  # Store all ASNs
            for asn, data in to_import.items():
                p.sadd(f'{self.key_prefix}|{address_family}|{date}|{asn}', *data[address_family])  # Store all prefixes
                p.set(f'{self.key_prefix}|{address_family}|{date}|{asn}|ipcount', data['ipcount'])  # Total IPs for the AS
            self.logger.debug('All keys ready')
            p.execute()
            self.update_last(address_family, date)
            self.logger.debug('Done.')
        unset_running(self.__class__.__name__)

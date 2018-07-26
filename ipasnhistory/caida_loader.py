#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
from pathlib import Path
from redis import StrictRedis
from .libs.helpers import set_running, unset_running, get_socket_path
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
        for path in self.storage_root.glob('**/*.gz'):
            address_family, year, month, date_str = re.findall('.*/(.*)/(.*)/(.*)/routeviews-rv[2,6]-(.*).pfx2as.gz', str(path))[0]
            date = parse(date_str).isoformat()
            if self.already_loaded(address_family, date):
                logging.debug(f'Already loaded {path}')
                continue
            logging.info(f'Loading {path}')
            to_import = defaultdict(lambda: {address_family: set(), 'ipcount': 0})
            with gzip.open(path) as f:
                for line in f:
                    prefix, length, asns = line.decode().strip().split('\t')
                    # The meaning of AS set and multi-origin AS in unclear. Taking the first ASN in the list only.
                    asn = re.split('[,_]', asns)[0]
                    network = ip_network(f'{prefix}/{length}')
                    to_import[asn][address_family].add(str(network))
                    to_import[asn]['ipcount'] += network.num_addresses

            logging.debug('Content loaded')
            p = self.storagedb.pipeline()
            p.sadd(f'{self.key_prefix}|{address_family}|dates', date)
            p.sadd(f'{self.key_prefix}|{address_family}|{date}|asns', *to_import.keys())
            for asn, data in to_import.items():
                p.sadd(f'{self.key_prefix}|{address_family}|{date}|{asn}', *data[address_family])
                p.set(f'{self.key_prefix}|{address_family}|{date}|{asn}|ipcount', data['ipcount'])
            logging.debug('All keys ready')
            p.execute()
            self.update_last(address_family, date)
            logging.debug('Done.')
        unset_running(self.__class__.__name__)

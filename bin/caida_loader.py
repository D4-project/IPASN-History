#!/usr/bin/env python3

import gzip
import logging
import re

from dateutil.parser import parse
from collections import defaultdict
from ipaddress import ip_network
from typing import Dict, Any

from redis import Redis

from ipasnhistory.default import get_socket_path, AbstractManager, get_config
from ipasnhistory.helpers import get_data_dir

logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s:%(message)s',
                    level=logging.INFO)


class CaidaLoader(AbstractManager):

    def __init__(self, loglevel: int=logging.INFO):
        super().__init__(loglevel)
        self.script_name = "caida_loader"
        self.key_prefix = 'caida'
        self.storage_root = get_data_dir() / 'caida'
        self.storagedb = Redis(get_config('generic', 'storage_db_hostname'), get_config('generic', 'storage_db_port'), decode_responses=True)
        self.storagedb.sadd('prefixes', self.key_prefix)
        self.cache = Redis(unix_socket_path=get_socket_path('cache'), decode_responses=True)

    def _to_run_forever(self):
        self.load_all()

    def already_loaded(self, address_family: str, date: str) -> bool:
        return self.storagedb.sismember(f'{self.key_prefix}|{address_family}|dates', date)

    def update_last(self, address_family: str, date: str) -> None:
        cur_last = self.storagedb.get(f'{self.key_prefix}|{address_family}|last')
        if not cur_last or date > cur_last:
            self.storagedb.set(f'{self.key_prefix}|{address_family}|last', date)

    def load_all(self):
        for path in sorted(self.storage_root.glob('**/*.gz'), reverse=True):
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
            to_import: Dict[str, Any] = defaultdict(lambda: {address_family: set(), 'ipcount': 0})
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


def main():
    m = CaidaLoader()
    m.run(sleep_in_sec=30)


if __name__ == '__main__':
    main()

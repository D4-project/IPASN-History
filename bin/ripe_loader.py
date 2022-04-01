#!/usr/bin/env python3

import logging
import re
from collections import defaultdict
from datetime import datetime
from ipaddress import ip_network
from pathlib import Path
from typing import Dict, List, Any

from redis import Redis

from bgpdumpy import TableDumpV2, BGPDump  # type: ignore
from socket import AF_INET


from ipasnhistory.default import AbstractManager, get_socket_path, get_config
from ipasnhistory.helpers import get_data_dir

logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s:%(message)s',
                    level=logging.INFO)


def routeview(bview_file: Path):

    routes: Dict[str, List] = {'v4': [], 'v6': []}

    with BGPDump(bview_file) as bgp:

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


class RipeLoader(AbstractManager):

    def __init__(self, loglevel: int=logging.INFO):
        super().__init__(loglevel)
        self.script_name = "ripe_loader"
        self.collector = 'rrc00'
        self.key_prefix = f'ripe_{self.collector}'
        self.storage_root = get_data_dir() / 'ripe' / self.collector
        self.storagedb = Redis(get_config('generic', 'storage_db_hostname'),
                               get_config('generic', 'storage_db_port'), decode_responses=True)
        self.storagedb.sadd('prefixes', self.key_prefix)
        self.cache = Redis(unix_socket_path=get_socket_path('cache'), decode_responses=True)

    def _to_run_forever(self):
        self.load_all()

    def already_loaded(self, date: str) -> bool:
        return (self.storagedb.sismember(f'{self.key_prefix}|v4|dates', date)
                and self.storagedb.sismember(f'{self.key_prefix}|v6|dates', date))

    def update_last(self, address_family: str, date: str) -> None:
        cur_last = self.storagedb.get(f'{self.key_prefix}|{address_family}|last')
        if not cur_last or date > cur_last:
            self.storagedb.set(f'{self.key_prefix}|{address_family}|last', date)

    def load_all(self):
        for path in sorted(self.storage_root.glob('**/*.gz'), reverse=True):
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
            self.logger.info('Content loaded')
            for address_family, entries in routes.items():
                to_import: Dict[str, Any] = defaultdict(lambda: {address_family: set(), 'ipcount': 0})
                for prefix, asn in entries:
                    network = ip_network(prefix)
                    to_import[asn][address_family].add(str(network))
                    to_import[asn]['ipcount'] += network.num_addresses
                p = self.storagedb.pipeline()
                self.storagedb.sadd(f'{self.key_prefix}|{address_family}|dates', date)
                self.storagedb.sadd(f'{self.key_prefix}|{address_family}|{date}|asns', *to_import.keys())  # Store all ASNs
                for asn, data in to_import.items():
                    p = self.storagedb.pipeline()
                    p.sadd(f'{self.key_prefix}|{address_family}|{date}|{asn}', *data[address_family])  # Store all prefixes
                    p.set(f'{self.key_prefix}|{address_family}|{date}|{asn}|ipcount', data['ipcount'])  # Total IPs for the AS
                    p.execute()
            self.logger.debug('All keys ready')
            self.update_last(address_family, date)
            self.logger.info(f'Done with {path}')


def main():
    m = RipeLoader()
    m.run(sleep_in_sec=30)


if __name__ == '__main__':
    main()

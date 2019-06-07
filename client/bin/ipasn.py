#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import time
try:
    import simplejson as json
except ImportError:
    import json


from pyipasnhistory import IPASNHistory

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a query against IP ASN History')
    parser.add_argument('--url', type=str, help='URL of the instance.')

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--meta', action='store_true', help='Get meta information.')

    group.add_argument('--file', help='Mass process queries from a file.')

    group.add_argument('--ip', help='IP to lookup')
    parser.add_argument('--source', default='caida', help='Source to query (currently, only caida is supported)')
    parser.add_argument('--address_family', help='Can be either v4 or v6')
    parser.add_argument('--date', help='Exact date to lookup. Fallback to most recent available.')
    parser.add_argument('--first', help='First date in the interval')
    parser.add_argument('--last', help='Last date in the interval')
    args = parser.parse_args()

    if args.url:
        ipasn = IPASNHistory(args.url)
    else:
        ipasn = IPASNHistory()
    if args.meta:
        response = ipasn.meta()
        print(json.dumps(response))
    elif args.file:
        with open(args.file) as f:
            queries = [json.loads(q) for q in f]
            response = ipasn.mass_cache(queries)
            print(json.dumps(response, indent=2))
            time.sleep(1)
            response = ipasn.mass_query(queries)
            print(json.dumps(response, indent=2))
    else:
        response = ipasn.query(args.ip, args.source, args.address_family, args.date,
                               args.first, args.last)
        print(json.dumps(response, indent=2))

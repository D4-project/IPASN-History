#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse

from ipasnhistory.lookup import Lookup

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Cache prefix announcements on a specific timeframe.')
    parser.add_argument('source', help='Dataset source name (must be caida for now).')
    parser.add_argument('first_date', help='First date in the interval.')
    parser.add_argument('last_date', help='Last date in the interval.')
    args = parser.parse_args()

    lookup = Lookup(args.source, args.first_date, args.last_date)
    lookup.run(sleep_in_sec=1)

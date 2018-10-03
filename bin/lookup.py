#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from dateutil.parser import parse
from datetime import timedelta
import argparse

from ipasnhistory.lookup import Lookup

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Cache prefix announcements on a specific timeframe.')
    parser.add_argument('source', help='Dataset source name (must be caida for now).')
    parser.add_argument('first_date', help='First date in the interval.')
    parser.add_argument('last_date', help='Last date in the interval.')
    args = parser.parse_args()

    def date_range(start, end, delta):
        current = start
        while current < end:
            yield current
            current += delta

    dates = [day for day in date_range(parse(args.first_date), parse(args.last_date), timedelta(days=1))]

    lookup = Lookup(args.source, dates)
    lookup.run(sleep_in_sec=1)

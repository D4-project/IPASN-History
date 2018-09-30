#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from ipasnhistory.query import Query
import requests

q = Query()

response = q.query('146.185.222.49', first='2018-09-01', last='2018-09-25')
print(json.dumps(response))

r = requests.get('http://127.0.0.1:5006/?ip=8.8.8.8&first=2018-09-01')
print(r.json())

query = {'ip': '8.8.8.8', 'first': '2018-09-01'}
r = requests.post('http://127.0.0.1:5006', data=query)
print(r.json())


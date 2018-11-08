#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from ipasnhistory.query import Query
import requests

q = Query()

response = q.meta()
print(response)
print(json.dumps(response, indent=2))

response = q.query('146.185.222.49')
print(json.dumps(response, indent=2))

response = q.query('146.185.222.49', first='2018-11-01', last='2018-11-25')
print(json.dumps(response, indent=2))

# --------- web

print('Meta info')
r = requests.get('http://127.0.0.1:5006/meta')
print(r.json())

print('Interval with first / last')
r = requests.get('http://127.0.0.1:5006/?ip=8.8.8.8&first=2018-09-01')
print(r.json())

print('Interval with first only')
query = {'ip': '8.8.7.7', 'first': '2018-09-01'}
r = requests.post('http://127.0.0.1:5006', data=query)
print(r.json())

print('One day only')
query = {'ip': '8.8.7.7', 'first': '2018-11-05'}
r = requests.post('http://127.0.0.1:5006', data=query)
print(r.json())

print('Cache only')
query = {'ip': '8.8.7.7', 'first': '2018-11-05', 'cache_only': True}
r = requests.post('http://127.0.0.1:5006', data=query)
print(r.json())

print('Precision delta')
query = {'ip': '8.8.7.7', 'date': '2018-11-08', 'precision_delta': json.dumps({'days': 2})}
r = requests.post('http://127.0.0.1:5006', data=query)
print(r.json())

print('Latest')
query = {'ip': '8.8.7.7'}
r = requests.post('http://127.0.0.1:5006', data=query)
print(r.json())

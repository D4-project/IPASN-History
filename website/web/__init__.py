#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from flask import Flask, request
from ipasnhistory.query import Query
from flask import jsonify
import json

app = Flask(__name__)

q = Query()


@app.route('/', methods=['GET', 'POST'])
def index():
    # The values in request.args and request.form are lists, convert it to unique values
    if request.method == 'POST':
        d = {k: v for k, v in request.form.items()}
    elif request.method == 'GET':
        d = {k: v for k, v in request.args.items()}

    if 'precision_delta' in d:
        d['precision_delta'] = json.loads(d['precision_delta'])
    # Expected keys in d: ip, source, address_family, date, first, last, cache_only, precision_delta
    return jsonify(q.query(**dict(d)))


@app.route('/meta', methods=['GET'])
def meta():
    '''Returns meta information regarding the data contained in the system'''
    return jsonify(q.meta())

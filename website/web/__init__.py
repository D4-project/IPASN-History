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
    if request.method == 'HEAD':
        # Just returns ack if the webserver is running
        return 'Ack'
    # The values in request.args and request.form are lists, convert it to unique values
    if request.method == 'POST':
        d = {k: v for k, v in request.form.items()}
    elif request.method == 'GET':
        d = {k: v for k, v in request.args.items()}

    if 'precision_delta' in d:
        d['precision_delta'] = json.loads(d['precision_delta'])
    # Expected keys in d: ip, source, address_family, date, first, last, cache_only, precision_delta

    try:
        response = q.query(**dict(d))
        return jsonify(response)
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/mass_cache', methods=['POST'])
def mass_cache():
    '''Cache a all the queries'''
    try:
        to_cache = request.get_json(force=True)
        for c in to_cache:
            if 'precision_delta' in c:
                c['precision_delta'] = json.loads(c.pop('precision_delta'))
        response = q.mass_cache(to_cache)
        return jsonify(response)
    except Exception as e:
        return jsonify({'error': str(e)})


@app.route('/meta', methods=['GET'])
def meta():
    '''Returns meta information regarding the data contained in the system'''
    try:
        response = q.meta()
        return jsonify(response)
    except Exception as e:
        return jsonify({'error': str(e)})

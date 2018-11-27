#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from flask import Flask, request
from ipasnhistory.query import Query
from flask import jsonify

try:
    import simplejson as json
except ImportError:
    import json

app = Flask(__name__)

app.config["DEBUG"] = False
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False

if app.config["DEBUG"]:
    import flask_profiler

    # You need to declare necessary configuration to initialize
    # flask-profiler as follows:
    app.config["flask_profiler"] = {
        "enabled": app.config["DEBUG"],
        "storage": {
            "engine": "sqlite"
        },
        "basicAuth": {
            "enabled": True,
            "username": "admin",
            "password": "admin"
        },
        "ignore": [
            "^/static/.*"
        ]
    }

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


@app.route('/mass_query', methods=['POST'])
def mass_query():
    '''Query all the things'''
    try:
        to_query = request.get_json(force=True)
        for c in to_query:
            if 'precision_delta' in c:
                c['precision_delta'] = json.loads(c.pop('precision_delta'))
        response = q.mass_query(to_query)
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


@app.route('/asn_meta', methods=['POST'])
def asn_meta():
    '''Get the ASN meta information'''
    try:
        query = request.get_json(force=True)
        if 'precision_delta' in query:
            query['precision_delta'] = json.loads(query.pop('precision_delta'))
        response = q.asn_meta(**query)
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


if app.config["DEBUG"]:
    # In order to active flask-profiler, you have to pass flask
    # app as an argument to flask-profiler.
    # All the endpoints declared so far will be tracked by flask-profiler.
    flask_profiler.init_app(app)

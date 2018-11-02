#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from flask import Flask, request
from ipasnhistory.query import Query
from flask import jsonify

app = Flask(__name__)

q = Query()


@app.route('/', methods=['GET', 'POST'])
def index():
    # The values in request.args and request.form are lists, convert it to unique values
    if request.method == 'POST':
        d = {k: v for k, v in request.form.items()}
    elif request.method == 'GET':
        d = {k: v for k, v in request.args.items()}

    # Expected keys in d: ip, source, address_family, date, first, last, cache_only, precision_delta
    return jsonify(q.query(**dict(d)))

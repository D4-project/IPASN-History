#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from flask import Flask, request
from ipasnhistory.query import Query
from flask import jsonify

app = Flask(__name__)

q = Query()


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        d = {k: v for k, v in request.form.items()}
    elif request.method == 'GET':
        # The values in request.args are lists, convert it to unique values
        d = {k: v for k, v in request.args.items()}

    print('-----', d)
    # Expected keys in d: ip, source, address_family, date, first, last
    return jsonify(q.query(**dict(d)))

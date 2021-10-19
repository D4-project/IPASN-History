#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import pkg_resources
from typing import Dict, List

from flask import Flask, request
from flask_restx import Api, Resource, fields  # type: ignore

from ipasnhistory.query import Query

from .helpers import get_secret_key
from .proxied import ReverseProxied

app: Flask = Flask(__name__)

app.wsgi_app = ReverseProxied(app.wsgi_app)  # type: ignore

app.config['SECRET_KEY'] = get_secret_key()

api = Api(app, title='IP ASN History API',
          description='API to query IPASN History.',
          version=pkg_resources.get_distribution('ipasnhistory').version)

query: Query = Query()


def _unpack_query(query: Dict) -> Dict:
    if 'precision_delta' in query:
        query['precision_delta'] = json.loads(query['precision_delta'])
    return query


ipquery_fields = api.model('IPQueryFields', {
    'ip': fields.String(description="The IP to lookup", default="8.8.8.8", required=True),
    'source': fields.String(description="The source of the data to use (currently, only caida)", default='caida'),
    'address_family': fields.String(description="IPv4 or IPv6", default='v4'),
    'date': fields.DateTime(description="Date of the record"),
    'first': fields.String(description="For an interval, first date", default=''),
    'last': fields.String(description="For an interval, last date", default=''),
    'precision_delta': fields.String(description="For a specific, the maximal allowed interval", default='{"days": 3}'),
})

asnquery_fields = api.model('ASNQueryFields', {
    'asn': fields.String(description="The ASN to lookup", default="6661", required=True),
    'source': fields.String(description="The source of the data to use (currently, only caida)", default='caida'),
    'address_family': fields.String(description="IPv4 or IPv6", default='v4'),
    'date': fields.DateTime(description="Date of the record"),
    'first': fields.String(description="For an interval, first date", default=''),
    'last': fields.String(description="For an interval, last date", default=''),
    'precision_delta': fields.String(description="For a specific, the maximal allowed interval", default='{"days": 3}'),
})


@api.route('/ip')
@api.doc(description="Search an IP")
class IPQuery(Resource):

    @api.param('ip', 'The IP to lookup', required=True)
    def get(self):
        # The values in request.args and request.form are lists, convert it to unique values
        d = _unpack_query({k: v for k, v in request.args.items()})
        try:
            return query.query(**d)
        except Exception as e:
            return {'error': e}

    @api.doc(body=ipquery_fields)
    def post(self):
        d = _unpack_query(request.get_json(force=True))  # type: ignore
        try:
            return query.query(**d)
        except Exception as e:
            return {'error': e}


mass_ipquery_fields = api.model('MassIPQueryFields', ipquery_fields, as_list=True)


@api.route('/mass_query')
@api.doc(description="Search a list of IP")
class MassQuery(Resource):

    @api.doc(body=mass_ipquery_fields)
    def post(self):
        try:
            to_query: List = request.get_json(force=True)  # type: ignore
            for c in to_query:
                c = _unpack_query(c)
            return query.mass_query(to_query)
        except Exception as e:
            return {'error': str(e)}


@api.route('/mass_cache')
@api.doc(description="Cache a list of IP")
class MassCache(Resource):
    @api.doc(body=mass_ipquery_fields)
    def post(self):
        try:
            to_query: List = request.get_json(force=True)  # type: ignore
            for c in to_query:
                c = _unpack_query(c)
            return query.mass_cache(to_query)
        except Exception as e:
            return {'error': str(e)}


@api.route('/asn_meta', methods=['POST'])
@api.doc(description='Get the ASN meta information')
class ASNMeta(Resource):

    @api.doc(body=asnquery_fields)
    def post(self):
        try:
            to_query = _unpack_query(request.get_json(force=True))  # type: ignore
            return query.asn_meta(**to_query)
        except Exception as e:
            return {'error': str(e)}


@api.route('/meta')
@api.route(description='Returns meta information regarding the data contained in the system')
class Meta(Resource):

    def get(self):
        try:
            return query.meta()
        except Exception as e:
            return {'error': str(e)}

# IP ASN-History

This project allows you to figure out the ASN announcing an IP and the closest prefix
announcing it at a specific date.

The default curently loads announces from [CAIDA](http://data.caida.org/datasets/routing/)
but RIPE will be added soon in order to be able to compare the announces.

# API

The REST API has two entry points:

* /meta (GET): returns meta informations about the information currently stored in the database

```json
{
    'sources': ['caida'],
    'expected_interval': {'first': 'YYYY-MM-DD',
                          'last': 'YYYY-MM-DD'},
    'cached_dates': {
        'v4': {
            'cached': [<all the dates in ISO format>],
            'missing': [<missing dates in ISO format>]},
            'pervent: 90'
        },
        'v6': {
            'cached': [<all the dates in ISO format>],
            'missing': [<missing dates in ISO format>]},
            'pervent: 90'
        }
    }
}

```

* / (POST/GET): Runs a query.

Parameters:

* **ip**: (required) IP to lookup
* **source**: (optional) Source to query (defaults to 'caida') - currently, only caida is supported
* **address_family**: (optional) v4 or v6 (defaults to v4)
* **date**: (optional) Exact date to lookup (defaults to most recent available)
* **first**: (optional) First date in the interval
* **last**: (optional) Last date in the interval
* **cache_only**: (optional) Do not wait for the response. Useful when an other process is expected to request the IP later on. (defaults to False)
* **precision_delta**: (optional) Max delta allowed between the date queried and the one we have in the database. Expects a dictionary to pass to timedelta.
                 Example: {days=1, seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=0}

Response:

```json
{
    'meta': {
        'source': source, 'ip_version': address_family, 'ip': ip
    },
    'error': 'Optional, only if there was an error',
    'info': 'Optional, informational message if needed',
    'response': {
        'YYYY-MM-DD': {'asn': ASN, 'prefix': Prefix},
        # Multiple entries if an interval was queried.
    }
}

```
**Important**: The date returned may differ from the one queried: the system will figure out the closest available date to the one queried.

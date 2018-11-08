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
  "sources": [
    "caida"
  ],
  "expected_interval": {
    "first": "2018-05-12",
    "last": "2018-11-08"
  },
  "cached_dates": {
    "caida": {
      "v4": {
        "cached": [
          "2018-04-26T12:00:00",
          "2018-04-27T12:00:00",
		  //...
          "2018-11-05T12:00:00",
          "2018-11-06T12:00:00"
        ],
        "missing": [
          "2018-11-07"
        ],
        "percent": 99.44444444444444
      },
      "v6": {
        "cached": [
          "2018-04-26T12:00:00",
          "2018-04-27T12:00:00",
		  //...
          "2018-11-06T12:00:00",
          "2018-11-07T12:00:00"
        ],
        "missing": [],
        "percent": 100.0
      }
    }
  }
}
```

**Note**: the percentage will help 3rd party component to decide if they should query the service now or wait.
		  It is expected to miss a few days and probably not important.

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
  "meta": {
    "source": "caida",
    "ip_version": "v4",
    "ip": "146.185.222.49"
  },
  "error": "Optional, only if there was an error",
  "info": "Optional, informational message if needed",
  "response": {
    "2018-11-01T12:00:00": {
      "asn": "44050",
      "prefix": "146.185.222.0/24"
    },
    "2018-11-02T16:00:00": {
      "asn": "44050",
      "prefix": "146.185.222.0/24"
    },
    "2018-11-03T12:00:00": {
      "asn": "44050",
      "prefix": "146.185.222.0/24"
    },
    "2018-11-04T12:00:00": {
      "asn": "44050",
      "prefix": "146.185.222.0/24"
    },
    "2018-11-05T12:00:00": {
      "asn": "44050",
      "prefix": "146.185.222.0/24"
    },
    "2018-11-06T12:00:00": {
      "asn": "44050",
      "prefix": "146.185.222.0/24"
    }
  }
}
```

**Note**: The date returned may differ from the one queried: the system will figure out the closest available date to the one queried.


# Installation

You need to have ardb and redis installed in the parent directory.

ardb (more precisely rocksdb) doesn't compile on ubuntu 18.04 unless you disable warning as error:

```bash
DISABLE_WARNING_AS_ERROR=1 make
```

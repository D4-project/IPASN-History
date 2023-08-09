# IP ASN History

IP ASN History is an open source project to figure out the ASN announcing an IP
and the closest prefix announcing it at a specific date.

The default currently loads announces from [CAIDA](http://data.caida.org/datasets/routing/)
also [RIPE dumps](https://www.ripe.net/analyse/internet-measurements/routing-information-service-ris/ris-raw-data) in MRT format.

IP ASN History is a complete open source project to run your own server locally.

If you don't want to run your own server, there is a [public API](https://www.d4-project.org/open%20source/asn/history/2019/01/09/IP-ASN-History.html) operated by CIRCL.

# Python client

See the [PyIPASNHistory repository](https://github.com/D4-project/pyipasnhistory)

# API

The REST API has two entry points:

* `/meta` (GET): returns meta informations about the information currently stored in the database

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

* **`/` (POST/GET)**: Runs a query. (if you want to resolve more than 1000 IPs, use `mass_cache` and `mass_query`)

    **Parameters**:

	* **ip**: (required) IP to lookup
	* **source**: (optional) Source to query (defaults to 'caida') - currently, only caida is supported
	* **address_family**: (optional) v4 or v6 (defaults to v4)
	* **date**: (optional) Exact date to lookup (defaults to most recent available)
	* **first**: (optional) First date in the interval
	* **last**: (optional) Last date in the interval
	* **precision_delta**: (optional) Max delta allowed between the date queried and the one we have in the database. Expects a dictionary to pass to timedelta.
			 Example: {days=1, seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=0}

    **Response**:

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


    **Curl examples**:

    It works fine for single requests, if you have batches of IPs to lookup, use the python API and `mass_cache` `mass_query`.

    ```bash
    curl https://bgpranking-ng.circl.lu/ipasn_history/?ip=8.8.8.8
    curl "https://bgpranking-ng.circl.lu/ipasn_history/?ip=2001:db8:a0b:12f0::1&address_family=v6"
    ```

    **Note**: The date returned may differ from the one queried: the system will figure out the closest available date to the one queried.

* **`/mass_cache` (POST)**: Caches a lot of queries at once, don't wait for the lookup.

    **Parameters**: A list of dictionaries with the same parameters as the default query.

    **Response**: A list of queries that IPASN History wasn't able to process.

    **Note**: Use this path when you have lots of query to run and (>1000) in order to resolve all of them at once.

* **`/mass_query` (POST)**: Caches a lot of queries at once. Either wait for the lookup to be done, or pick the data from cache.

    **Parameters**: A list of dictionaries with the same parameters as the default query.

    **Response**: A list of responses as the default query.

    **Note**: Use this path when you have lots of query to run and (>1000) in order to resolve all of them at once.

* **`/asn_meta` (POST)**: Returns meta informations about an ASN

    **Parameters**: A list of dictionaries with the same parameters as the default query.

    **Response**

    ```json
    {
      "meta": {
      "address_family": "v4",
      "asn": "137342",
      "source": "caida"
      },
      "response": {
        "2019-01-01T12:00:00": {
          "137342": {
            "ipcount": "512",
            "prefixes": [
              "180.214.250.0/24",
              "103.113.3.0/24"
            ]
          }
        }
      }
    }
    ```


# Installation

**IMPORTANT**: Use [poetry](https://github.com/python-poetry/poetry#installation)

**NOTE**: Yes, it requires python3.8+. No, it will never support anything older.

## Install redis

```bash
git clone https://github.com/antirez/redis.git
cd redis
git checkout 6.2
make
make test
cd ..
```

**Note**: If it fails, have a look at [the documentation](https://github.com/redis/redis#building-redis).

## Install kvrocks

```bash
git clone https://github.com/KvrocksLabs/kvrocks.git
cd kvrocks
./x.py build
cd ..
```

**Note**: If it fails, have a look at [the documentation](https://github.com/KvrocksLabs/kvrocks#building-kvrocks).

## Install & run IP ASN History

```bash
git clone https://github.com/D4-project/IPASN-History.git
cd IPASN-History
poetry install
echo IPASNHISTORY_HOME="'`pwd`'" > .env
poetry shell
start
```

## Shutdown IP ASN History

```bash
stop
```

# (Optional) Build & install bgpdumpy, required to process dumps from RIPE

```bash
sudo apt install ca-certificates gcc make autoconf libbz2-dev zlib1g-dev libffi-dev
poetry install -E ripe
```

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import time
from pathlib import Path
from subprocess import Popen
from typing import Optional, Dict

from redis import Redis
from redis.exceptions import ConnectionError

from ipasnhistory.default import get_homedir, get_socket_path, get_config


def check_running(name: str) -> bool:
    if name == "storage":
        r = Redis(get_config('generic', 'storage_db_hostname'), get_config('generic', 'storage_db_port'))
    else:
        socket_path = get_socket_path(name)
        if not os.path.exists(socket_path):
            return False
        r = Redis(unix_socket_path=socket_path)
    try:
        return True if r.ping() else False
    except ConnectionError:
        return False


def launch_cache(storage_directory: Optional[Path]=None):
    if not storage_directory:
        storage_directory = get_homedir()
    if not check_running('cache'):
        Popen(["./run_redis.sh"], cwd=(storage_directory / 'cache'))


def shutdown_cache(storage_directory: Optional[Path]=None):
    if not storage_directory:
        storage_directory = get_homedir()
    r = Redis(unix_socket_path=get_socket_path('cache'))
    r.shutdown(save=True)
    print('Redis cache database shutdown.')


def launch_storage(storage_directory: Optional[Path]=None):
    if not storage_directory:
        storage_directory = get_homedir()
    if not check_running('storage'):
        Popen(["./run_kvrocks.sh"], cwd=(storage_directory / 'storage'))


def shutdown_storage(storage_directory: Optional[Path]=None):
    redis = Redis(get_config('generic', 'storage_db_hostname'), get_config('generic', 'storage_db_port'))
    redis.shutdown()


def launch_all():
    launch_cache()
    launch_storage()


def check_all(stop: bool=False):
    backends: Dict[str, bool] = {'cache': False, 'storage': False}
    while True:
        for db_name in backends.keys():
            print(backends[db_name])
            try:
                backends[db_name] = check_running(db_name)
            except Exception:
                backends[db_name] = False
        if stop:
            if not any(running for running in backends.values()):
                break
        else:
            if all(running for running in backends.values()):
                break
        for db_name, running in backends.items():
            if not stop and not running:
                print(f"Waiting on {db_name} to start")
            if stop and running:
                print(f"Waiting on {db_name} to stop")
        time.sleep(1)


def stop_all():
    shutdown_cache()
    shutdown_storage()


def main():
    parser = argparse.ArgumentParser(description='Manage backend DBs.')
    parser.add_argument("--start", action='store_true', default=False, help="Start all")
    parser.add_argument("--stop", action='store_true', default=False, help="Stop all")
    parser.add_argument("--status", action='store_true', default=True, help="Show status")
    args = parser.parse_args()

    if args.start:
        launch_all()
    if args.stop:
        stop_all()
    if not args.stop and args.status:
        check_all()


if __name__ == '__main__':
    main()

#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from pathlib import Path
from .exceptions import CreateDirectoryException, MissingEnv
from redis import StrictRedis
from redis.exceptions import ConnectionError
from datetime import datetime, timedelta
import time


def get_storage_path() -> Path:
    if not os.environ.get('VIRTUAL_ENV'):
        raise MissingEnv("VIRTUAL_ENV is missing. This project really wants to run from a virtual envoronment.")
    return Path(os.environ['VIRTUAL_ENV'])


def get_homedir() -> Path:
    if not os.environ.get('IPASNHISTORY_HOME'):
        guessed_home = Path(__file__).resolve().parent.parent.parent
        raise MissingEnv(f"IPASNHISTORY_HOME is missing. \
Run the following command (assuming you run the code from the clonned repository):\
    export IPASNHISTORY_HOME='{guessed_home}'")
    return Path(os.environ['IPASNHISTORY_HOME'])


def safe_create_dir(to_create: Path) -> None:
    if to_create.exists() and not to_create.is_dir():
        raise CreateDirectoryException(f'The path {to_create} already exists and is not a directory')
    os.makedirs(to_create, exist_ok=True)


def set_running(name: str) -> None:
    r = StrictRedis(unix_socket_path=get_socket_path('cache'), db=1, decode_responses=True)
    r.hset('running', name, 1)


def unset_running(name: str) -> None:
    r = StrictRedis(unix_socket_path=get_socket_path('cache'), db=1, decode_responses=True)
    r.hdel('running', name)


def is_running() -> dict:
    r = StrictRedis(unix_socket_path=get_socket_path('cache'), db=1, decode_responses=True)
    return r.hgetall('running')


def get_socket_path(name: str) -> str:
    mapping = {
        'cache': Path('cache', 'cache.sock'),
        'storage': Path('storage', 'storage.sock'),
    }
    return str(get_homedir() / mapping[name])


def check_running(name: str) -> bool:
    socket_path = get_socket_path(name)
    print(socket_path)
    try:
        r = StrictRedis(unix_socket_path=socket_path)
        if r.ping():
            return True
    except ConnectionError:
        return False


def shutdown_requested() -> bool:
    try:
        r = StrictRedis(unix_socket_path=get_socket_path('cache'), db=1, decode_responses=True)
        return r.exists('shutdown')
    except ConnectionRefusedError:
        return True
    except ConnectionError:
        return True


def long_sleep(sleep_in_sec: int, shutdown_check: int=10) -> bool:
    sleep_until = datetime.now() + timedelta(seconds=sleep_in_sec)
    while sleep_until > datetime.now():
        time.sleep(shutdown_check)
        if shutdown_requested():
            return False
    return True

#!/usr/bin/env python3
from functools import lru_cache
from pathlib import Path

from .default import get_homedir, safe_create_dir


@lru_cache(64)
def get_data_dir() -> Path:
    capture_dir = get_homedir() / 'rawdata'
    safe_create_dir(capture_dir)
    return capture_dir

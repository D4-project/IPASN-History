#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from subprocess import Popen
from ipasnhistory.libs.helpers import get_homedir


if __name__ == '__main__':
    # Just fail if the env isn't set.
    get_homedir()
    p = Popen(['run_backend.py', '--start'])
    p.wait()
    Popen(['caida_dl.py'])
    Popen(['caida_loader.py'])
    Popen(['lookup.py'])

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from subprocess import Popen
from ipasnhistory.libs.helpers import get_homedir

if __name__ == '__main__':
    get_homedir()
    p = Popen(['shutdown.py'])
    p.wait()
    Popen(['run_backend.py', '--stop'])

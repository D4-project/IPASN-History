#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from subprocess import Popen
from ipasnhistory.libs.helpers import get_homedir

if __name__ == '__main__':
    try:
        Popen(['gunicorn', '--worker-class', 'gevent', '-w', '10', '-b', '0.0.0.0:5176', 'web:app'],
              cwd=get_homedir() / 'website').communicate()
    except KeyboardInterrupt:
        print('Stopping gunicorn.')

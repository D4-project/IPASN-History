#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from subprocess import Popen, run

from ipasnhistory.default import get_homedir


def main():
    # Just fail if the env isn't set.
    get_homedir()
    print('Start backend (redis)...')
    p = run(['run_backend', '--start'])
    p.check_returncode()
    print('done.')

    Popen(['lookup_manager', '--days_in_memory', '10', '--floating_window_days', '3'])
    Popen(['caida_downloader', '--months_to_download', '1'])
    Popen(['caida_loader'])

    print('Start website...')
    #Popen(['start_website'])
    print('done.')


if __name__ == '__main__':
    main()

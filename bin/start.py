#!/usr/bin/env python3

from subprocess import Popen, run

from ipasnhistory.default import get_homedir


def main():
    # Just fail if the env isn't set.
    get_homedir()
    print('Start backend (redis)...')
    p = run(['run_backend', '--start'])
    p.check_returncode()
    print('done.')

    Popen(['lookup_manager'])

    Popen(['caida_downloader'])
    Popen(['caida_loader'])

    Popen(['ripe_downloader'])
    Popen(['ripe_loader'])

    print('Start website...')
    Popen(['start_website'])
    print('done.')


if __name__ == '__main__':
    main()

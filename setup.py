#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup


setup(
    name='ipasnhistory',
    version='0.1',
    author='Raphaël Vinot',
    author_email='raphael.vinot@circl.lu',
    maintainer='Raphaël Vinot',
    url='https://github.com/D4-project/IPASN-History',
    description='IP ASN History, the new one..',
    packages=['ipasnhistory'],
    scripts=['bin/run_backend.py', 'bin/caida_dl.py', 'bin/start.py', 'bin/stop.py', 'bin/shutdown.py',
             'bin/caida_loader.py', 'bin/lookup.py', 'bin/lookup_manager.py', 'bin/start_website.py',
             'bin/install_bgpdumpy.sh'],
    classifiers=[
        'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)',
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Operating System :: POSIX :: Linux',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Telecommunications Industry',
        'Intended Audience :: Information Technology',
        'Programming Language :: Python :: 3',
        'Topic :: Security',
        'Topic :: Internet',
    ]
)

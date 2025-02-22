#!/usr/bin/env python

import os
from setuptools import find_packages
from distutils.core import setup

version = os.environ.get('ZYN_PY_VERSION', '0.0.1')
path_requirements = os.path.dirname(os.path.abspath(__name__)) + '/requirements.txt'

setup(
    name='PyZyn',
    version=version,
    description='Python clients and utilities for Zyn',
    packages=find_packages(),
    install_requires=open(path_requirements).readlines(),
    entry_points={
        'console_scripts': [
            'zyn-shell=zyn.main:shell',
            'zyn-cli=zyn.main:cli',
            'zyn-webserver=zyn.main:webserver',
        ]
    },
    data_files=[
        ('zyn-web-static', [
            "zyn/client/zyn-web-static/zyn.css",
            "zyn/client/zyn-web-static/zyn.js",
            "zyn/client/zyn-web-static/pdf.worker.mjs",
        ]),
        ('zyn-web-templates', [
            'zyn/client/zyn-web-templates/login.html',
            'zyn/client/zyn-web-templates/main.html',
        ]),
    ],
)

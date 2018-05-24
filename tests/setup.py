#!/usr/bin/env python

from distutils.core import setup

setup(
    name='Zyn Util',
    version='.1',
    description='System tests and Python utilities for Zyn',
    packages=['zyn_util'],
    install_requires=[
        'nose==1.3.7',
        'flake8==3.3.0',
        'tornado==4.5.2',
        'certifi',
    ],
    entry_points={
        'console_scripts': [
            'zyn-cli=zyn_util.cli_client:main',
            'zyn-web-server=zyn_util.web_client:main',
        ]
    }
)

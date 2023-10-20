#!/usr/bin/env python

import os
from setuptools import find_packages
from distutils.core import setup

path_requirements = os.path.dirname(os.path.abspath(__name__)) + '/requirements.txt'

setup(
    name='PyZyn',
    version='0.0.1',
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
        ('zyn-web-static/3pp/icons8', [
            "zyn/client/web-static-files/3pp/icons8/cancel.png"
        ]),
        ('zyn-web-static/3pp/jsdiff', [
            "zyn/client/web-static-files/3pp/jsdiff/diff.min.js",
            "zyn/client/web-static-files/3pp/jsdiff/LICENSE",
        ]),
        ('zyn-web-static/3pp/pdfjs', [
            "zyn/client/web-static-files/3pp/pdfjs/LICENSE",
            "zyn/client/web-static-files/3pp/pdfjs/pdf.js",
            "zyn/client/web-static-files/3pp/pdfjs/pdf.worker.js",
        ]),
        ('zyn-web-static/3pp/showdownjs', [
                "zyn/client/web-static-files/3pp/showdownjs/LICENSE",
                "zyn/client/web-static-files/3pp/showdownjs/showdown.min.js",
        ]),
        ('zyn-web-static/3pp/w3css', [
                "zyn/client/web-static-files/3pp/w3css/w3.css",
        ]),
        ('zyn-web-static', [
            "zyn/client/web-static-files/zyn-client.js",
            "zyn/client/web-static-files/zyn-connection.js",
            "zyn/client/web-static-files/zyn.css",
            "zyn/client/web-static-files/zyn.js",
        ]),
        ('zyn-web-templates', [
            'zyn/client/web-templates/login.html',
            'zyn/client/web-templates/main.html',
        ]),
    ],
)

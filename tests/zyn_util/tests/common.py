import datetime
import logging
import os
import os.path
import subprocess
import sys
import tempfile
import time
import unittest

import zyn_util.connection

PATH_FILE = os.path.dirname(os.path.abspath(__file__))
PATH_BIN = PATH_FILE + '/../../../zyn/target/debug/zyn'
PATH_CERT = os.path.expanduser("~/.zyn-certificates/cert.pem")
PATH_KEY = os.path.expanduser("~/.zyn-certificates/key.pem")
PATH_GPG_FINGERPRINT = os.path.expanduser("~/.zyn-test-user-gpg-fingerprint")

HOUR_SECONDS = 60 * 60
DAY_SECONDS = HOUR_SECONDS * 24


class TestZyn(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(
            format='ZynSystemTest: %(asctime)-15s %(module)s:%(lineno)d %(levelname)s: %(message)s',
            stream=sys.stdout,
            level=logging.DEBUG,
        )
        self._remote_ip = "127.0.0.1"
        self._remote_port = 4433
        self._username = 'admin'
        self._password = 'admin'
        self._work_dir = self._temp_dir()

    def _start_server_process(self, args=[]):
        enviroment_variables = os.environ.copy()
        enviroment_variables['RUST_LOG'] = 'trace'
        enviroment_variables['RUST_BACKTRACE'] = '1'
        logging.debug('Starting node with params: {}'.format(args))
        return subprocess.Popen(
            [PATH_BIN] + args,
            stdout=subprocess.PIPE,
            env=enviroment_variables
        )

    def _temp_dir(self):
        return tempfile.TemporaryDirectory()

    def utc_timestamp(self):
        return int(datetime.datetime.utcnow().timestamp())

    def _start_server(
            self,
            path_data_dir,
            path_cert=None,
            path_key=None,
            local_port=None,
            local_address=None,
            remote_port=None,
            remote_address=None,
            gpg_fingerprint=None,
            default_username=None,
            default_password=None,
            init=False
    ):
        params = []

        if init:
            params.append('--init')

        params.append('--path-data-dir')
        params.append(path_data_dir)

        params.append('--local-port')
        params.append(str(remote_port or self._remote_port))
        params.append('--local-address')
        params.append(remote_address or self._remote_ip)

        params.append('--path-cert')
        params.append(path_cert or PATH_CERT)

        params.append('--path-key')
        params.append(path_key or PATH_KEY)

        params.append('--gpg-fingerprint')
        if gpg_fingerprint is not None:
            params.append(gpg_fingerprint)
        else:
            with open(PATH_GPG_FINGERPRINT, 'r') as fp:
                params.append(fp.read().strip())

        params.append('--default-user-name')
        params.append(default_username or self._username)

        params.append('--default-user-password')
        params.append(default_password or self._password)

        process = self._start_server_process(params)
        time.sleep(.1)  # Give some time for the process to start up
        return process

    def _create_connection_and_connect(
            self,
            path_key=None,
            path_cert=None,
            remote_port=None,
            remote_ip=None,
    ):
        connection = zyn_util.connection.ZynConnection(
            path_key or PATH_KEY,
            path_cert or PATH_CERT
        )
        connection.connect(
            remote_ip or self._remote_ip,
            remote_port or self._remote_port
        )
        return connection

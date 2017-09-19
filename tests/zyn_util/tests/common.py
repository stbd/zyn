import glob
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

    def _start_node(self, args=[]):
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

    def _start_node_default_params(self, path_data_dir, init=False):
        params = []
        if init:
            params.append('--init')

        params.append('--path-data-dir')
        params.append(path_data_dir)

        params.append('--local-port')
        params.append(str(self._remote_port))
        params.append('--local-address')
        params.append(self._remote_ip)

        params.append('--default-user-name')
        params.append(self._username)

        params.append('--default-user-password')
        params.append(self._password)

        params.append('--path-cert')
        params.append(PATH_CERT)

        params.append('--path-key')
        params.append(PATH_KEY)

        params.append('--gpg-fingerprint')
        with open(PATH_GPG_FINGERPRINT, 'r') as fp:
            params.append(fp.read().strip())

        process = self._start_node(params)

        # Give some time for the process to start up
        time.sleep(.1)

        return process

    def _create_connection(self):
        return zyn_util.connection.ZynConnection(PATH_KEY, PATH_CERT)

    def _validate_data_dir(self, path, expected_files):
        files = glob.glob(path + '/*')
        assert len(files) == len(expected_files)

        for f in files:
            assert os.path.basename(f) in expected_files

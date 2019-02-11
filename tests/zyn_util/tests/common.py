import collections
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
import zyn_util.exception

PATH_FILE = os.path.dirname(os.path.abspath(__file__))
PATH_BIN = PATH_FILE + '/../../../zyn/target/debug/zyn'
PATH_CERT = os.path.expanduser("~/.zyn-certificates/cert.pem")
PATH_KEY = os.path.expanduser("~/.zyn-certificates/key.pem")
PATH_GPG_FINGERPRINT = os.path.expanduser("~/.zyn-test-user-gpg-fingerprint")
DEFAULT_TLS_REMOTE_HOSTNAME = 'zyn'
DEFAULT_SERVER_WORKDIR = 'server-workdir'

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

    def _path_server_workdir(self, server_workdir=DEFAULT_SERVER_WORKDIR):
        return '{}/{}'.format(self._work_dir.name, server_workdir)

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
            filesystem_capacity=None,
            max_number_of_files_per_directory=None,
            max_page_size_random_access=None,
            max_page_size_blob=None,
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

        if filesystem_capacity is not None:
            params.append('--filesystem-capacity')
            params.append(str(filesystem_capacity))

        if max_number_of_files_per_directory is not None:
            params.append('--max-number-of-files-per-directory')
            params.append(str(max_number_of_files_per_directory))

        if max_page_size_random_access is not None:
            params.append('--max-page-size-for-random-access')
            params.append(str(max_page_size_random_access))

        if max_page_size_blob is not None:
            params.append('--max-page-size-for-blob')
            params.append(str(max_page_size_blob))

        process = self._start_server_process(params)
        time.sleep(.1)  # Give some time for the process to start up
        return process

    def _create_connection_and_connect(
            self,
            path_cert=None,
            remote_port=None,
            remote_ip=None,
            remote_hostname=DEFAULT_TLS_REMOTE_HOSTNAME
    ):
        connection = zyn_util.connection.ZynConnection(
            path_cert or PATH_CERT

        )
        connection.connect(
            remote_ip or self._remote_ip,
            remote_port or self._remote_port,
            remote_hostname=remote_hostname,
        )
        return connection


class TestCommon(TestZyn):

    def setUp(self):
        super(TestCommon, self).setUp()
        self._process = None

    def tearDown(self):
        if self._process:
            self._stop_node(self._process)

    def _stop_node(self, process, expected_return_code=0):
        ret = process.poll()
        if ret is not None:
            assert ret == expected_return_code
        else:
            logging.info('Process {} was still alive, stopping'.format(process.pid))
            while True:
                process.terminate()
                if process.poll() is None:
                    process.kill()
                time.sleep(1)
                if process.poll() is None:
                    print('Failed to kill server, trying again')
                else:
                    break

    def _validate_socket_is_disconnected(self, connection):
        with self.assertRaises(zyn_util.exception.ZynConnectionLost):
            connection.read_message()

    def _get_files_in_server_workdir(
            self,
            server_workdir=DEFAULT_SERVER_WORKDIR,
            filter_directories=[]
    ):
        server_workdir = self._path_server_workdir(server_workdir)
        elements = collections.OrderedDict()
        for root, dirs, files in os.walk(server_workdir):
            root = root.replace(server_workdir, '')
            if not root.startswith('/'):
                root = '/' + root

            if root in filter_directories:
                continue

            elements[root] = []
            for f in files:
                elements[root].append(f)

        return elements

    def _start_node(self, server_workdir=DEFAULT_SERVER_WORKDIR, init=True, **kwargs):
        server_workdir = self._path_server_workdir(server_workdir)
        if not os.path.exists(server_workdir):
            os.mkdir(server_workdir)
        self._process = self._start_server(
            server_workdir,
            init=init,
            **kwargs
        )

    def _connect_to_node(self):
        connection = self._create_connection_and_connect()
        connection.enable_debug_messages()
        return connection

    def _validate_response(
            self,
            response,
            connection,
            expected_error_code=0,
            expected_transaction_id=None,
    ):

        if expected_transaction_id is None:
            expected_transaction_id = connection.transaction_id() - 1
        self.assertEqual(response.protocol_version(), 1)
        self.assertEqual(response.error_code(), expected_error_code)
        self.assertEqual(response.transaction_id(), expected_transaction_id)

    def _handle_auth(self, connection, username=None, password=None):
        rsp = connection.authenticate(
            username or self._username,
            password or self._password,
        )
        self.assertEqual(rsp.number_of_fields(), 0)
        self._validate_response(rsp, connection)
        return rsp

    def _connect_to_node_and_handle_auth(self):
        c = self._connect_to_node()
        self._handle_auth(c)
        return c

    def _start_and_connect_to_node_and_handle_auth(self, server_workdir=DEFAULT_SERVER_WORKDIR):
        self._start_node(server_workdir)
        return self._connect_to_node_and_handle_auth()

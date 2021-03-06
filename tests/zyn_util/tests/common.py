import collections
import datetime
import logging
import os
import os.path
import random
import string
import subprocess
import sys
import tempfile
import time
import unittest

import zyn_util.connection
import zyn_util.exception

PATH_FILE = os.path.dirname(os.path.abspath(__file__))
PATH_BIN = PATH_FILE + '/../../../zyn/target/debug/zyn'
PATH_CERT = os.path.expanduser("/etc/ssl/certs/zyn-test.pem")
PATH_KEY = os.path.expanduser("/etc/ssl/private/zyn-test.key")
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
            max_block_size_random_access=None,
            max_block_size_blob=None,
            max_inactity_duration_secs=None,
            authentication_token_duration_secs=None,
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

        if max_block_size_random_access is not None:
            params.append('--max-page-size-for-random-access')
            params.append(str(max_block_size_random_access))

        if max_block_size_blob is not None:
            params.append('--max-page-size-for-blob')
            params.append(str(max_block_size_blob))

        if max_inactity_duration_secs is not None:
            params.append('--max-inactivity-duration-seconds')
            params.append(str(max_inactity_duration_secs))

        if authentication_token_duration_secs is not None:
            params.append('--authentication-token-duration')
            params.append(str(authentication_token_duration_secs))

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

        socket = zyn_util.connection.ZynSocket.create_with_custom_cert(
            remote_ip or self._remote_ip,
            remote_port or self._remote_port,
            path_cert or PATH_CERT,
            remote_hostname=remote_hostname,
        )

        return zyn_util.connection.ZynConnection(socket)


class TestCommon(TestZyn):

    def setUp(self):
        super(TestCommon, self).setUp()
        self._process = None

    def tearDown(self):
        if self._process:
            self._stop_node()

    def _stop_node(self, expected_return_code=0, trials=1):
        if self._process is None:
            return

        ret = self._process.poll()
        if ret is not None:
            assert ret == expected_return_code
        else:
            logging.info('Process {} was still alive, stopping'.format(self._process.pid))
            for _ in range(0, trials):
                self._process.terminate()
                if self._process.poll() is not None:
                    break
                time.sleep(1)

            # This may block forever if OS for some reason decides not
            # to stop the process, but at least user is able to notice it
            while True:
                if self._process.poll() is None:
                    self._process.kill()
                    time.sleep(0.5)
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

    def _connect_to_node_and_handle_auth(self, username=None, password=None):
        c = self._connect_to_node()
        self._handle_auth(c, username, password)
        return c

    def _start_and_connect_to_node_and_handle_auth(
            self,
            server_workdir=DEFAULT_SERVER_WORKDIR,
            **kwargs
    ):
        self._start_node(server_workdir, **kwargs)
        return self._connect_to_node_and_handle_auth()

    def _create_binary_blob_for_test_data(self, size):
        path_data_file = '{}/data-nonce-{}.bin'.format(PATH_FILE, size)
        if not os.path.exists(path_data_file) or os.stat(path_data_file).st_size != size:
            logging.info("Creating data file with size %d", size)

            current_size = 0
            block = list((string.ascii_uppercase + string.digits) * 1024)
            block_size = len(block)

            with open(path_data_file, 'wb') as fp:
                while current_size < size:
                    random.shuffle(block)
                    size_to_write = size - current_size
                    size_to_write = min(size_to_write, block_size)
                    data = ''.join(block[0:size_to_write])
                    fp.write(data.encode())
                    current_size += size_to_write

        return path_data_file

    def _megabytes_to_bytes(self, number_of_megabytes):
        return 1024 * 1024 * number_of_megabytes

    def _read_file_and_validate_with_file_on_disk(
            self,
            connection,
            node_id,
            path_data_disk,
            block_size,
    ):
        size = os.stat(path_data_disk).st_size
        fp = open(path_data_disk, 'rb')
        offset_start = 0
        offset_end = block_size

        while offset_end <= size:
            expected_data = fp.read(block_size)
            read_rsp, data = connection.read_file(node_id, offset_start, block_size)
            self.assertEqual(data, expected_data)
            offset_start = offset_end
            offset_end += block_size

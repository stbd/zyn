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
import logging

import zyn.socket
import zyn.connection
import zyn.util
import zyn.exception


PATH_FILE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_SERVER_WORKDIR = 'server-workdir'
PATH_GPG_FINGERPRINT = os.path.expanduser("~/.zyn-test-user-gpg-fingerprint")
HOUR_SECONDS = 60 * 60
DAY_SECONDS = HOUR_SECONDS * 24


class ZynCommon(unittest.TestCase):
    def setUp(self):
        self.log = zyn.util.get_logger('test', logging.DEBUG)
        self.server_address = "127.0.0.1"
        self.server_port = 4433
        self.username = 'admin'
        self.password = 'admin'
        self.path_zyn_binary = os.environ.get(
            'zyn_tests_path_server_binary',
            PATH_FILE + '/../zyn/target/debug/zyn',
        )
        self.manage_server = self._parse_boolean_environment_variable(
            'zyn_tests_manage_server',
            True,
        )
        self.use_tls = self._parse_boolean_environment_variable(
            'zyn_tests_use_tls',
            False,
        )
        self.work_dir = tempfile.TemporaryDirectory()
        self.process = None

        if not os.path.exists(self.path_zyn_binary):
            raise RuntimeError(f'Zyn binary "{self.path_zyn_binary}" not found')

    def tearDown(self):
        if self.process:
            self._stop_node()

    def _parse_boolean_environment_variable(self, name, default_value):
        value = os.environ.get(name)
        if value is None:
            return default_value

        if value == 'true':
            return True
        elif value == 'false':
            return False
        else:
            raise RuntimeError(f'{name} must be either "true" or "false"')

    def utc_timestamp(self):
        return int(datetime.datetime.utcnow().timestamp())

    def _path_test_server_workdir(self, server_workdir):
        return '{}/{}'.format(self.work_dir.name, server_workdir)

    def is_server_running(self):
        if not self.process:
            return False
        return self.process.poll() is None

    def _prepare_node_and_connect(
            self,
            **kwargs,
    ):
        self._start_node(
            init=True,
            **kwargs,
        )
        return self._connect(
            kwargs.get('server_address', self.server_address),
            kwargs.get('server_port', self.server_port),
        )

    def _connect(self, server_address=None, server_port=None):
        if not self.use_tls:
            socket = zyn.socket.ZynSocket.create_no_tls(
                server_address or self.server_address,
                server_port or self.server_port
            )
        else:
            raise NotImplementedError()
        return zyn.connection.ZynConnection(socket, True)

    def _start_node(
        self,
        data_dir=None,
        server_port=None,
        server_address=None,
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
        if self.is_server_running():
            raise RuntimeError('Server already running')

        if not self.manage_server:
            self.log.warn('Server is not managed by tests, skipping node start')
            return

        path_data_dir = self._path_test_server_workdir(data_dir or DEFAULT_SERVER_WORKDIR)
        os.makedirs(path_data_dir, exist_ok=True)
        params = []

        if init:
            params.append('--init')

        params.append('--path-data-dir')
        params.append(path_data_dir)

        params.append('--local-port')
        params.append(str(server_port or self.server_port))
        params.append('--local-address')
        params.append(server_address or self.server_address)

        params.append('--gpg-fingerprint')
        if gpg_fingerprint is not None:
            params.append(gpg_fingerprint)
        else:
            with open(PATH_GPG_FINGERPRINT, 'r') as fp:
                params.append(fp.read().strip())

        params.append('--default-user-name')
        params.append(default_username or self.username)

        params.append('--default-user-password')
        params.append(default_password or self.password)

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

        enviroment_variables = os.environ.copy()
        enviroment_variables['RUST_LOG'] = 'trace'
        enviroment_variables['RUST_BACKTRACE'] = '1'
        self.log.debug(f'Starting node binary "{self.path_zyn_binary}" with arguments: "{" ".join(params)}"')
        self.process = subprocess.Popen(
            [self.path_zyn_binary] + params,
            env=enviroment_variables
        )
        time.sleep(.1)  # Give some time for the process to start up

    def _stop_node(self, expected_return_code=0, trials=1):
        if self.process is None:
            return

        ret = self.process.poll()
        if ret is not None:
            assert ret == expected_return_code
        else:
            self.log.info('Node process {} was still alive, terminating process'.format(self.process.pid))
            for _ in range(0, trials):
                self.process.terminate()
                if self.process.poll() is not None:
                    break
                time.sleep(1)

            # This may block forever if OS for some reason decides not
            # to stop the process, but at least user is able to notice it
            while True:
                if self.process.poll() is None:
                    self.process.kill()
                    time.sleep(0.5)
                else:
                    break
        self.process = None

    def _restart_node(self, server_workdir=None):
        self._stop_node(trials=3)
        self._start_node(server_workdir, init=False)

    def _get_files_in_server_workdir(
            self,
            server_workdir=None,
            filter_directories=[]
    ):
        server_workdir = self._path_test_server_workdir(server_workdir or DEFAULT_SERVER_WORKDIR)
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


class ZynNodeCommon(ZynCommon):

    def _prepare_node_and_authenticate_connection(self, **kwargs):
        c = self._prepare_node_and_connect(**kwargs)
        self._authenticate(c)
        return c

    def _connect_to_node_and_handle_auth(self, username=None, password=None):
        c = self._connect()
        self._authenticate(c, username, password)
        return c

    def _validate_server_is_not_running(self):
        self.assertFalse(self.is_server_running())

    def _validate_socket_is_disconnected(self, connection):
        with self.assertRaises(zyn.exception.ZynConnectionLost):
            connection.read_message()

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

    def _authenticate(self, connection, username=None, password=None):
        rsp = connection.authenticate(
            username or self.username,
            password or self.password,
        )
        self.assertEqual(rsp.number_of_fields(), 0)
        self._validate_response(rsp, connection)
        return rsp

    def _create_file_ra(
            self,
            connection,
            name,
            parent_path=None,
            parent_node_id=None,
            block_size=None,
    ):
        rsp = connection.create_file_random_access(
            name,
            parent_path=parent_path,
            parent_node_id=parent_node_id,
            block_size=block_size,
        )
        return rsp.as_create_rsp()

    def _create_file_blob(
            self,
            connection,
            name,
            parent_path=None,
            parent_node_id=None,
            block_size=None,
    ):
        rsp = connection.create_file_blob(
            name,
            parent_path=parent_path,
            parent_node_id=parent_node_id,
            block_size=block_size,
        )
        return rsp.as_create_rsp()

    def _create_directory(self, connection, name, parent_path=None, parent_node_id=None):
        rsp = connection.create_directory(
            name,
            parent_path=parent_path,
            parent_node_id=parent_node_id,
        )
        return rsp.as_create_rsp()

    def _open_file_read(self, connection, path=None, node_id=None):
        rsp = connection.open_file_read(
            path=path,
            node_id=node_id,
        )
        return rsp.as_open_rsp()

    def _open_file_write(self, connection, path=None, node_id=None):
        rsp = connection.open_file_write(
            path=path,
            node_id=node_id,
        )
        return rsp.as_open_rsp()

    def _close_file(self, connection, node_id):
        rsp = connection.close_file(node_id)
        self._validate_response(rsp, connection)

    def _query_fs_children(self, connection, path=None, node_id=None):
        rsp = connection.query_fs_children(
            path=path,
            node_id=node_id,
        )
        return rsp.as_query_fs_children_rsp()

    def _query_fs_element(self, connection, path=None, node_id=None):
        rsp = connection.query_fs_element(
            node_id=node_id,
            path=path,
        )
        return rsp.as_query_fs_element_rsp()

    def _query_fs_element_properties(
            self,
            connection,
            path=None,
            node_id=None,
            parent_node_id=None,
            parent_path=None
    ):
        rsp = connection.query_fs_element_properties(
            node_id=node_id,
            path=path,
            parent_node_id=parent_node_id,
            parent_path=parent_path,
        )
        return rsp.as_query_fs_element_properties_rsp()

    def _delete(self, connection, node_id=None, path=None):
        rsp = connection.delete(
            node_id=node_id,
            path=path,
        )
        self._validate_response(rsp, connection)

    def _validate_fs_element_does_not_exist(self, connection, node_id=None, path=None):
        rsp = connection.open_file_write(node_id=node_id, path=path)
        self.assertEqual(rsp.error_code(), zyn.errors.NodeIsNotFile)

    def _query_counters(self, connection):
        rsp = connection.query_counters()
        return rsp.as_query_counters_rsp()

    def _query_system(self, connection):
        rsp = connection.query_system()
        return rsp.as_query_system_rsp()

    def _modify_user(self, connection, username, password=None, expiration_in_seconds=None):
        expiration = None
        if expiration_in_seconds is not None:
            expiration = self.utc_timestamp() + expiration_in_seconds

        rsp = connection.modify_user(
            username,
            expiration=expiration,
            password=password,
        )
        self._validate_response(rsp, connection)
        return rsp

    def _validate_msg_is_notification(self, msg):
        self.assertEqual(msg.type(), zyn.connection.Message.NOTIFICATION)

    def _validate_notification_type(self, msg, notification_type):
        self.assertEqual(
            msg.notification_type(),
            notification_type,
        )

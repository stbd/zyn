import logging

import zyn_util.tests.common
import zyn_util.errors


class TestBasic(zyn_util.tests.common.TestZyn):

    def setUp(self):
        super(TestBasic, self).setUp()
        self._process = None
        self._connection = self._create_connection()

    def tearDown(self):
        if self._process:
            self._stop_node(self._process)

    def _stop_node(self, process, expected_return_code=0):
        ret = process.poll()
        if ret is not None:
            assert ret == expected_return_code
        else:
            logging.info('Process {} was still alive, stopping'.format(process.pid))
            process.kill()

    def _validate_response(self, rsp, expected_transaction_id=None, expected_error_code=0):
        if expected_transaction_id is None:
            expected_transaction_id = self._connection.transaction_id() - 1
        self.assertEqual(rsp.protocol_version(), 1)
        self.assertEqual(rsp.error_code(), expected_error_code)
        self.assertEqual(rsp.transaction_id(), expected_transaction_id)

    def _handle_auth(self):
        rsp = self._connection.authenticate(self._username, self._password)
        self.assertEqual(rsp.number_of_fields(), 0)
        self._validate_response(rsp, self._connection.transaction_id() - 1)
        return rsp

    def _start_and_connect_to_node(self):
        self._process = self._start_node_default_params(self._work_dir.name, init=True)
        self._connection.enable_debug_messages()
        self._connection.connect(self._remote_ip, self._remote_port)

    def _start_and_connect_to_node_and_handle_auth(self):
        self._start_and_connect_to_node()
        self._handle_auth()

    def _validate_socket_is_disconnected(self):
        with self.assertRaises(TimeoutError):
            self._connection.read_message()

    def test_node_shutdown_causes_notification(self):
        self._start_and_connect_to_node()
        self._process.terminate()
        msg = self._connection.read_message()
        self.assertEqual(msg.type(), zyn_util.connection.Message.NOTIFICATION)
        self.assertEqual(msg.notification_type(), "DISCONNECTED")
        self.assertNotEqual(msg.field(0).as_string(), "")
        self._validate_socket_is_disconnected()

    def test_authetication_invalid_password(self):
        self._start_and_connect_to_node()

        for i in range(3):
            rsp = self._connection.authenticate(self._username, "invalid")
            self._validate_response(
                rsp,
                self._connection.transaction_id() - 1,
                expected_error_code=zyn_util.errors.InvalidUsernamePassword
            )
        self._validate_socket_is_disconnected()

    def test_create_file_parent_path(self):
        self._start_and_connect_to_node_and_handle_auth()
        rsp = self._connection.create_file_random_access('file-1', parent_path='/')
        self._validate_response(rsp)
        rsp.as_create_rsp()

    def test_create_file_parent_node_id(self):
        self._start_and_connect_to_node_and_handle_auth()
        rsp = self._connection.create_file_random_access('file-1', parent_node_id=0)
        self._validate_response(rsp)
        rsp.as_create_rsp()

    def test_create_folder_parent_node_id(self):
        self._start_and_connect_to_node_and_handle_auth()
        rsp = self._connection.create_folder('folder-1', parent_node_id=0)
        self._validate_response(rsp)
        rsp.as_create_rsp()

    def test_create_folder_with_file(self):
        self._start_and_connect_to_node_and_handle_auth()
        rsp = self._connection.create_folder('folder-1', parent_node_id=0)
        self._validate_response(rsp)
        parent_node_id = rsp.as_create_rsp()
        rsp = self._connection.create_file_random_access('file-1', parent_node_id=parent_node_id)
        rsp.as_create_rsp()

    def _open_close(self, use_node_id, write):
        self._start_and_connect_to_node_and_handle_auth()
        rsp = self._connection.create_file_random_access('file-1', parent_path='/')
        self._validate_response(rsp)
        node_id_create = rsp.as_create_rsp()

        if write:
            open_func = self._connection.open_file_write
        else:
            open_func = self._connection.open_file_read

        if use_node_id:
            rsp = open_func(node_id=node_id_create)
        else:
            rsp = open_func(path='/file-1')
        self._validate_response(rsp)
        node_id_open, _, _, _ = rsp.as_open_rsp()
        self.assertEqual(node_id_create, node_id_open)
        rsp = self._connection.close_file(node_id_create)
        self._validate_response(rsp)

        rsp, _ = self._connection.read_file(node_id_open, 0, 5)
        self.assertEqual(rsp.error_code(), zyn_util.errors.ErrorFileIsNotOpen)

    def test_open_read_with_path_and_close_file(self):
        self._open_close(use_node_id=False, write=False)

    def test_open_read_with_node_id_and_close_file(self):
        self._open_close(use_node_id=True, write=False)

    def test_open_write_with_path_and_close_file(self):
        self._open_close(use_node_id=False, write=True)

    def test_open_write_with_node_id_and_close_file(self):
        self._open_close(use_node_id=True, write=True)

    def test_write_read(self):
        self._start_and_connect_to_node_and_handle_auth()
        node_id = self._connection.create_file_random_access('file-1', parent_path='/') \
                                  .as_create_rsp()

        _, revision, _, _ = self._connection.open_file_write(node_id=node_id).as_open_rsp()

        data = 'data'.encode('utf-8')
        rsp = self._connection.ra_write(node_id, revision, 0, data)
        self._validate_response(rsp)
        revision_write = rsp.as_write_rsp()

        rsp, data_read = self._connection.read_file(node_id, 0, len(data))
        self._validate_response(rsp)
        revision_read, _ = rsp.as_read_rsp()
        self.assertEqual(data, data_read)
        self.assertEqual(revision_read, revision_write)

    def _validate_read(self, node_id, offset, size, expected_revision, expected_text_data):
        expected_data = expected_text_data.encode('utf-8')
        rsp, data = self._connection.read_file(node_id, offset, size)
        self._validate_response(rsp)
        revision, block = rsp.as_read_rsp()
        self.assertEqual(revision, expected_revision)
        self.assertEqual(block[0], offset)
        self.assertEqual(block[1], len(expected_data))
        self.assertEqual(data, expected_data)

    def test_edit_file(self):
        self._start_and_connect_to_node_and_handle_auth()
        node_id = self._connection.create_file_random_access('file-1', parent_path='/') \
                                  .as_create_rsp()

        _, revision, _, _ = self._connection.open_file_write(node_id=node_id).as_open_rsp()
        revision = self._connection.ra_write(node_id, revision, 0, 'data'.encode('utf-8')) \
                                   .as_write_rsp()

        self._validate_read(node_id, 0, 100, revision, 'data')
        revision = self._connection.ra_insert(node_id, revision, 2, '--'.encode('utf-8')) \
                                   .as_insert_rsp()
        self._validate_read(node_id, 0, 100, revision, 'da--ta')
        revision = self._connection.ra_delete(node_id, revision, 4, 2).as_delete_rsp()
        self._validate_read(node_id, 0, 100, revision, 'da--')
        revision = self._connection.ra_write(node_id, revision, 4, 'qwerty'.encode('utf-8')) \
                                   .as_write_rsp()
        self._validate_read(node_id, 0, 100, revision, 'da--qwerty')

    def test_query_list(self):
        self._start_and_connect_to_node_and_handle_auth()
        node_id_file_1 = self._connection.create_file_random_access('file-1', parent_path='/') \
                                         .as_create_rsp()
        node_id_folder = self._connection.create_folder('folder-1', parent_path='/') \
                                         .as_create_rsp()
        node_id_file_2 = self._connection.create_file_random_access('file-2', parent_path='/') \
                                         .as_create_rsp()

        rsp = self._connection.query_list(path='/')
        self._validate_response(rsp)
        query = rsp.as_query_list_rsp()
        self.assertEqual(len(query), 3)
        self.assertEqual(len(query[0]), 3)
        names = [e[0] for e in query]
        index_file_1 = names.index('file-1')
        index_file_2 = names.index('file-2')
        index_folder = names.index('folder-1')
        self.assertEqual(query[index_file_1],
                         ['file-1', node_id_file_1, zyn_util.connection.FILE_TYPE_FILE])
        self.assertEqual(query[index_file_2],
                         ['file-2', node_id_file_2, zyn_util.connection.FILE_TYPE_FILE])
        self.assertEqual(query[index_folder],
                         ['folder-1', node_id_folder, zyn_util.connection.FILE_TYPE_FOLDER])

    def test_query_counters(self):
        self._start_and_connect_to_node_and_handle_auth()
        rsp = self._connection.query_counters()
        self._validate_response(rsp)
        counters = rsp.as_query_counters_rsp()
        self.assertEqual(counters['active-connections'], 1)

    def test_query_filesystem_file(self):
        self._start_and_connect_to_node_and_handle_auth()
        node_id = self._connection.create_file_random_access('file-1', parent_path='/') \
                                  .as_create_rsp()

        rsp = self._connection.query_filesystem(node_id=node_id)
        self._validate_response(rsp)
        desc = rsp.as_query_filesystem_rsp()
        self.assertEqual(desc['type'], zyn_util.connection.FILE_TYPE_FILE)

    def test_query_filesystem_folder(self):
        self._start_and_connect_to_node_and_handle_auth()
        node_id = self._connection.create_folder('folder-1', parent_path='/').as_create_rsp()
        rsp = self._connection.query_filesystem(node_id=node_id)
        self._validate_response(rsp)
        desc = rsp.as_query_filesystem_rsp()
        self.assertEqual(desc['type'], zyn_util.connection.FILE_TYPE_FOLDER)

    def _validate_fs_element_does_not_exist(self, node_id=None, path=None):
        rsp = self._connection.open_file_write(node_id=node_id, path=path)
        self.assertEqual(rsp.error_code(), zyn_util.errors.NodeIsNotFile)

    def test_delete_file_with_node_id(self):
        self._start_and_connect_to_node_and_handle_auth()
        node_id = self._connection.create_file_random_access('file', parent_path='/') \
                                  .as_create_rsp()

        rsp = self._connection.delete(node_id=node_id)
        self._validate_response(rsp)
        self._validate_fs_element_does_not_exist(node_id)

    def test_delete_file_with_path(self):
        self._start_and_connect_to_node_and_handle_auth()
        node_id = self._connection.create_file_random_access('file', parent_path='/') \
                                  .as_create_rsp()

        rsp = self._connection.delete(path='/file')
        self._validate_response(rsp)
        self._validate_fs_element_does_not_exist(node_id)

    def test_delete_folder_with_path(self):
        self._start_and_connect_to_node_and_handle_auth()
        node_id = self._connection.create_folder('folder', parent_path='/') \
                                  .as_create_rsp()

        rsp = self._connection.delete(path='/folder')
        self._validate_response(rsp)
        self._validate_fs_element_does_not_exist(node_id)

    def test_multiple_files_open_at_sametime(self):
        self._start_and_connect_to_node_and_handle_auth()
        node_id_1 = self._connection.create_file_random_access('file-1', parent_path='/') \
                                    .as_create_rsp()
        node_id_2 = self._connection.create_file_random_access('file-2', parent_path='/') \
                                    .as_create_rsp()
        _, revision_1, _, _ = self._connection.open_file_write(node_id=node_id_1).as_open_rsp()
        _, revision_2, _, _ = self._connection.open_file_write(node_id=node_id_2).as_open_rsp()

        data_1 = 'qwerty'.encode('utf-8')
        data_2 = 'zxcvbn'.encode('utf-8')
        self._connection.ra_write(node_id_1, revision_1, 0, data_1).as_write_rsp()
        self._connection.ra_write(node_id_2, revision_2, 0, data_2).as_write_rsp()

        rsp, data = self._connection.read_file(node_id_1, 0, 100)
        self._validate_response(rsp)
        self.assertEqual(data, data_1)

        rsp, data = self._connection.read_file(node_id_2, 0, 100)
        self._validate_response(rsp)
        self.assertEqual(data, data_2)

    def test_create_modify_user(self):
        username = 'user-1'
        self._start_and_connect_to_node_and_handle_auth()
        rsp = self._connection.create_user(username)
        self._validate_response(rsp)
        rsp = self._connection.modify_user(
            username,
            expiration=self.utc_timestamp() + zyn_util.tests.common.DAY_SECONDS,
            password='password'
        )
        self._validate_response(rsp)

    def test_create_modify_group(self):
        group_name = 'group-1'
        self._start_and_connect_to_node_and_handle_auth()
        rsp = self._connection.create_group(group_name)
        self._validate_response(rsp)
        rsp = self._connection.modify_group(
            group_name,
            expiration=self.utc_timestamp() + zyn_util.tests.common.DAY_SECONDS,
        )
        self._validate_response(rsp)

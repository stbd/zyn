import logging

import zyn_util.tests.common
import zyn_util.errors


class BasicsCommon(zyn_util.tests.common.TestZyn):

    def setUp(self):
        super(BasicsCommon, self).setUp()
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
            process.kill()

    def _validate_socket_is_disconnected(self, connection):
        with self.assertRaises(TimeoutError):
            connection.read_message()

    def _start_node(self):
        self._process = self._start_server(
            self._work_dir.name,
            init=True
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

    def _start_and_connect_to_node_and_handle_auth(self):
        self._start_node()
        c = self._connect_to_node()
        self._handle_auth(c)
        return c


class TestBasicUsage(BasicsCommon):
    def test_node_shutdown_causes_notification(self):
        self._start_node()
        c = self._connect_to_node()
        self._process.terminate()
        msg = c.read_message()
        self.assertEqual(msg.type(), zyn_util.connection.Message.NOTIFICATION)
        self.assertEqual(msg.notification_type(), "DISCONNECTED")
        self.assertNotEqual(msg.field(0).as_string(), "")
        self._validate_socket_is_disconnected(c)

    def test_authetication_with_invalid_password(self):
        self._start_node()
        c = self._connect_to_node()
        for i in range(3):
            rsp = c.authenticate(self._username, "invalid")
            self._validate_response(
                rsp,
                c,
                expected_error_code=zyn_util.errors.InvalidUsernamePassword
            )
        self._validate_socket_is_disconnected(c)

    def test_query_counters(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp = c.query_counters()
        self._validate_response(rsp, c)
        counters = rsp.as_query_counters_rsp()
        self.assertEqual(counters.number_of_counters(), 1)
        self.assertEqual(counters.active_connections, 1)


class TestBasicFilesystem(BasicsCommon):
    def test_create_file_with_parent_path(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp = c.create_file_random_access('file-1', parent_path='/')
        self._validate_response(rsp, c)
        rsp.as_create_rsp()

    def test_create_file_parent_node_id(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp = c.create_file_random_access('file-1', parent_node_id=0)
        self._validate_response(rsp, c)
        rsp.as_create_rsp()

    def test_create_folder_parent_node_id(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp = c.create_folder('folder-1', parent_node_id=0)
        self._validate_response(rsp, c)
        rsp.as_create_rsp()

    def test_create_folder_with_file(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp = c.create_folder('folder-1', parent_node_id=0)
        self._validate_response(rsp, c)
        create_rsp = rsp.as_create_rsp()
        rsp = c.create_file_random_access('file-1', parent_node_id=create_rsp.node_id)
        rsp.as_create_rsp()

    def _open_close(self, use_node_id, write):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp = c.create_file_random_access('file-1', parent_path='/')
        self._validate_response(rsp, c)
        create_rsp = rsp.as_create_rsp()

        if write:
            open_func = c.open_file_write
        else:
            open_func = c.open_file_read

        if use_node_id:
            rsp = open_func(node_id=create_rsp.node_id)
        else:
            rsp = open_func(path='/file-1')

        self._validate_response(rsp, c)
        open_rsp = rsp.as_open_rsp()
        self.assertEqual(create_rsp.node_id, open_rsp.node_id)
        rsp = c.close_file(create_rsp.node_id)
        self._validate_response(rsp, c)

        rsp, _ = c.read_file(open_rsp.node_id, 0, 5)
        self.assertEqual(rsp.error_code(), zyn_util.errors.ErrorFileIsNotOpen)

    def test_open_read_with_path_and_close_file(self):
        self._open_close(use_node_id=False, write=False)

    def test_open_read_with_node_id_and_close_file(self):
        self._open_close(use_node_id=True, write=False)

    def test_open_write_with_path_and_close_file(self):
        self._open_close(use_node_id=False, write=True)

    def test_open_write_with_node_id_and_close_file(self):
        self._open_close(use_node_id=True, write=True)

    def _validate_query_response(
            self,
            element,
            expected_name,
            expected_node_id,
            expected_element_type
    ):
        self.assertEqual(element.name, expected_name)
        self.assertEqual(element.node_id, expected_node_id)
        self.assertEqual(element.type_of_element, expected_element_type)

    def test_query_list(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        create_rsp_file_1 = c.create_file_random_access('file-1', parent_path='/').as_create_rsp()
        create_rsp_folder = c.create_folder('folder-1', parent_path='/').as_create_rsp()
        create_rsp_file_2 = c.create_file_random_access('file-2', parent_path='/').as_create_rsp()

        rsp = c.query_list(path='/')
        self._validate_response(rsp, c)
        query_rsp = rsp.as_query_list_rsp()

        self.assertEqual(query_rsp.number_of_elements(), 3)
        names = [e.name for e in query_rsp.elements]
        index_file_1 = names.index('file-1')
        index_file_2 = names.index('file-2')
        index_folder = names.index('folder-1')

        self._validate_query_response(
            query_rsp.elements[index_file_1],
            'file-1',
            create_rsp_file_1.node_id,
            zyn_util.connection.FILE_TYPE_FILE
        )
        self._validate_query_response(
            query_rsp.elements[index_file_2],
            'file-2',
            create_rsp_file_2.node_id,
            zyn_util.connection.FILE_TYPE_FILE
        )
        self._validate_query_response(
            query_rsp.elements[index_folder],
            'folder-1',
            create_rsp_folder.node_id,
            zyn_util.connection.FILE_TYPE_FOLDER
        )

    def test_query_filesystem_file(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        create_rsp = c.create_file_random_access('file-1', parent_path='/').as_create_rsp()
        rsp = c.query_filesystem(node_id=create_rsp.node_id)
        self._validate_response(rsp, c)
        query_rsp = rsp.as_query_filesystem_rsp()
        self.assertEqual(query_rsp.type_of_element, zyn_util.connection.FILE_TYPE_FILE)

    def test_query_filesystem_folder(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        create_rsp = c.create_folder('folder-1', parent_path='/').as_create_rsp()
        rsp = c.query_filesystem(node_id=create_rsp.node_id)
        self._validate_response(rsp, c)
        query_rsp = rsp.as_query_filesystem_rsp()
        self.assertEqual(query_rsp.type_of_element, zyn_util.connection.FILE_TYPE_FOLDER)

    def _validate_fs_element_does_not_exist(self, connection, node_id=None, path=None):
        rsp = connection.open_file_write(node_id=node_id, path=path)
        self.assertEqual(rsp.error_code(), zyn_util.errors.NodeIsNotFile)

    def test_delete_file_with_node_id(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        create_rsp = c.create_file_random_access('file', parent_path='/').as_create_rsp()
        rsp = c.delete(node_id=create_rsp.node_id)
        self._validate_response(rsp, c)
        self._validate_fs_element_does_not_exist(c, create_rsp.node_id)

    def test_delete_file_with_path(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        create_rsp = c.create_file_random_access('file', parent_path='/').as_create_rsp()
        rsp = c.delete(path='/file')
        self._validate_response(rsp, c)
        self._validate_fs_element_does_not_exist(c, create_rsp.node_id)

    def test_delete_folder_with_path(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        create_rsp = c.create_folder('folder', parent_path='/').as_create_rsp()
        rsp = c.delete(path='/folder')
        self._validate_response(rsp, c)
        self._validate_fs_element_does_not_exist(c, create_rsp.node_id)


class TestBasicEditFile(BasicsCommon):
    def _ra_write(self, connection, node_id, revision, offset, data):
        rsp = connection.ra_write(node_id, revision, offset, data.encode('utf-8'))
        self._validate_response(rsp, connection)
        return rsp.as_write_rsp()

    def _ra_insert(self, connection, node_id, revision, offset, data):
        rsp = connection.ra_insert(node_id, revision, offset, data.encode('utf-8'))
        self._validate_response(rsp, connection)
        return rsp.as_insert_rsp()

    def _ra_delete(self, connection, node_id, revision, offset, size):
        rsp = connection.ra_delete(node_id, revision, offset, size)
        self._validate_response(rsp, connection)
        return rsp.as_insert_rsp()

    def _blob_write(self, connection, node_id, revision, data):
        rsp = connection.blob_write(node_id, revision, data.encode('utf-8'), 2)
        self._validate_response(rsp, connection)
        return rsp.as_write_rsp()

    def _read(self, connection, node_id, offset, size, expected_revision, expected_data):
        rsp, data = connection.read_file(node_id, offset, size)
        self._validate_response(rsp, connection)
        read_rsp = rsp.as_read_rsp()
        data = data.decode('utf-8')
        self.assertEqual(data, expected_data)
        self.assertEqual(read_rsp.revision, expected_revision)
        return rsp, data

    def test_write_read_random_access(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        create_rsp = c.create_file_random_access('file-1', parent_path='/').as_create_rsp()
        open_rsp = c.open_file_write(node_id=create_rsp.node_id).as_open_rsp()

        data = 'data'
        write_rsp = self._ra_write(c, create_rsp.node_id, open_rsp.revision, 0, data)
        self._read(c, create_rsp.node_id, 0, len(data), write_rsp.revision, data)

    def test_edit_random_access_file(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        create_rsp = c.create_file_random_access('file-1', parent_path='/').as_create_rsp()
        open_rsp = c.open_file_write(node_id=create_rsp.node_id).as_open_rsp()
        self.assertEqual(open_rsp.type_of_element, zyn_util.connection.FILE_TYPE_RANDOM_ACCESS)

        rsp = self._ra_write(c, create_rsp.node_id, open_rsp.revision, 0, 'data')
        self._read(c, create_rsp.node_id, 0, 10, rsp.revision, 'data')

        rsp = self._ra_insert(c, create_rsp.node_id, rsp.revision, 2, '--')
        self._read(c, create_rsp.node_id, 0, 10, rsp.revision, 'da--ta')

        rsp = self._ra_delete(c, create_rsp.node_id, rsp.revision, 4, 2)
        self._read(c, create_rsp.node_id, 0, 10, rsp.revision, 'da--')

        rsp = self._ra_write(c, create_rsp.node_id, rsp.revision, 4, 'qwerty')
        self._read(c, create_rsp.node_id, 0, 10, rsp.revision, 'da--qwerty')

    def test_edit_blob_file(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        create_rsp = c.create_file_blob('file-1', parent_path='/').as_create_rsp()

        open_rsp = c.open_file_write(node_id=create_rsp.node_id).as_open_rsp()
        self.assertEqual(open_rsp.type_of_element, zyn_util.connection.FILE_TYPE_BLOB)

        rsp = self._blob_write(c, create_rsp.node_id, open_rsp.revision, 'data')
        self._read(c, create_rsp.node_id, 0, 10, rsp.revision, 'data')

        rsp = self._blob_write(c, create_rsp.node_id, rsp.revision, 'qwerty')
        self._read(c, create_rsp.node_id, 0, 10, rsp.revision, 'qwerty')

    def test_multiple_files_open_at_sametime(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        create_rsp_1 = c.create_file_random_access('file-1', parent_path='/').as_create_rsp()
        create_rsp_2 = c.create_file_random_access('file-2', parent_path='/').as_create_rsp()
        open_rsp_1 = c.open_file_write(node_id=create_rsp_1.node_id).as_open_rsp()
        open_rsp_2 = c.open_file_write(node_id=create_rsp_2.node_id).as_open_rsp()

        data_1 = 'qwerty'
        data_2 = 'zxcvbn'
        rsp_1 = self._ra_write(c, create_rsp_1.node_id, open_rsp_1.revision, 0, data_1)
        rsp_2 = self._ra_write(c, create_rsp_2.node_id, open_rsp_2.revision, 0, data_2)

        rsp, data = self._read(c, create_rsp_1.node_id, 0, 100, rsp_1.revision, data_1)
        rsp, data = self._read(c, create_rsp_2.node_id, 0, 100, rsp_2.revision, data_2)


class TestUserAuthority(BasicsCommon):
    def test_create_user(self):
        username = 'user-1'
        password = 'password'
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp = c.create_user(username)
        self._validate_response(rsp, c)
        rsp = c.modify_user(
            username,
            expiration=self.utc_timestamp() + zyn_util.tests.common.DAY_SECONDS,
            password=password
        )
        self._validate_response(rsp, c)

        c_new_user = self._connect_to_node()
        self._handle_auth(c_new_user, username, password)

    def test_create_group(self):
        group_name = 'group-1'
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp = c.create_group(group_name)
        self._validate_response(rsp, c)
        rsp = c.modify_group(
            group_name,
            expiration=self.utc_timestamp() + zyn_util.tests.common.DAY_SECONDS,
        )
        self._validate_response(rsp, c)

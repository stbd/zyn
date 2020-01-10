import time

import zyn_util.tests.common
import zyn_util.errors
from zyn_util.connection import DataStream


class TestBasicOperatinsCommon(zyn_util.tests.common.TestCommon):
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
        self.assertEqual(rsp.error_code(), zyn_util.errors.NodeIsNotFile)

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

    def _restart_node(self):
        self._stop_node(trials=3)
        self._start_node(init=False)

    def _validate_server_is_not_running(self):
        self.assertNotEqual(self._process.poll(), None)

    def _validate_msg_is_notification(self, msg):
        self.assertEqual(msg.type(), zyn_util.connection.Message.NOTIFICATION)

    def _validate_notification_type(self, msg, notification_type):
        self.assertEqual(
            msg.notification_type(),
            notification_type,
        )


class TestBasicServerUsage(TestBasicOperatinsCommon):
    def test_restarting_server_saves_user_specific_settings(self):
        password = 'new-password'
        c = self._start_and_connect_to_node_and_handle_auth()
        self._modify_user(c, 'admin', password)
        self._restart_node()
        self._connect_to_node_and_handle_auth('admin', password)

    def test_restarting_server_with_init_failes_gracefully(self):
        self._start_and_connect_to_node_and_handle_auth()
        self._stop_node()
        self._start_node()
        self._validate_server_is_not_running()
        self._process = None

    def test_node_shutdown_causes_notification(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        self._stop_node(trials=3)
        msg = c.read_message()
        self._validate_msg_is_notification(msg)
        self._validate_notification_type(msg, zyn_util.connection.Notification.TYPE_DISCONNECTED)
        self._validate_socket_is_disconnected(c)

    def test_authetication_with_invalid_password(self):
        self._start_node()
        c = self._connect_to_node()
        for i in range(3):
            rsp = c.authenticate(self._username, "invalid")
            self._validate_response(rsp, c, zyn_util.errors.InvalidUsernamePassword)
        self._validate_socket_is_disconnected(c)

    def test_max_inactivity_duration(self):
        max_inactity_duration_secs = 2
        c = self._start_and_connect_to_node_and_handle_auth(
            max_inactity_duration_secs=max_inactity_duration_secs
        )
        time.sleep(max_inactity_duration_secs + 1)
        msg = c.read_message()
        self._validate_msg_is_notification(msg)
        self._validate_notification_type(msg, zyn_util.connection.Notification.TYPE_DISCONNECTED)
        self._validate_socket_is_disconnected(c)


class TestQuery(TestBasicOperatinsCommon):
    def test_query_counters(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        counters = self._query_counters(c)
        self.assertEqual(counters.number_of_counters(), 3)
        self.assertEqual(counters.active_connections, 1)
        self.assertEqual(counters.number_of_files, 0)
        self.assertEqual(counters.number_of_open_files, 0)

    def test_query_counters_number_of_files(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        counters = self._query_counters(c)
        self.assertEqual(counters.number_of_files, 0)

        self._create_file_ra(c, 'file-1', parent_path='/')
        counters = self._query_counters(c)
        self.assertEqual(counters.number_of_files, 1)

        self._create_file_ra(c, 'file-2', parent_path='/')
        counters = self._query_counters(c)
        self.assertEqual(counters.number_of_files, 2)

    def test_query_counters_number_of_open_files(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        counters = self._query_counters(c)
        self.assertEqual(counters.number_of_open_files, 0)

        self._create_file_ra(c, 'file-1', parent_path='/')
        self._create_file_ra(c, 'file-2', parent_path='/')

        self._open_file_read(c, path='/file-1')
        counters = self._query_counters(c)
        self.assertEqual(counters.number_of_open_files, 1)

        rsp_2 = self._open_file_read(c, path='/file-2')
        counters = self._query_counters(c)
        self.assertEqual(counters.number_of_open_files, 2)

        self._close_file(c, rsp_2.node_id)
        counters = self._query_counters(c)
        self.assertEqual(counters.number_of_open_files, 1)

    def test_query_system(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        query = self._query_system(c)
        self.assertNotEqual(query.started_at, 0)
        self.assertNotEqual(query.server_id, 0)
        self.assertEqual(query.max_number_of_open_files_per_connection, 5)  # todo: hardcoded
        self.assertEqual(query.number_of_open_files, 0)
        self.assertEqual(query.has_admin_information, True)

    def test_query_system_number_of_open_files(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        self._create_file_ra(c, 'file', parent_node_id=0)
        self._open_file_read(c, path='/file')
        self.assertEqual(self._query_system(c).number_of_open_files, 1)
        self._open_file_read(c, path='/file')
        self.assertEqual(self._query_system(c).number_of_open_files, 2)


class TestBasicFilesystem(TestBasicOperatinsCommon):
    def test_create_file_with_parent_path(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        self._create_file_ra(c, 'file-1', parent_path='/')

    def test_create_file_with_parent_node_id(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        self._create_file_ra(c, 'file-1', parent_node_id=0)

    def test_create_file_with_same_name_under_different_parent(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        self._create_file_ra(c, 'file', parent_node_id=0)
        rsp = self._create_directory(c, 'dir', parent_node_id=0)
        self._create_file_ra(c, 'file', parent_node_id=rsp.node_id)

    def test_reopened_file_is_not_vissible_to_client(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp = self._create_file_ra(c, 'file', parent_node_id=0)
        self._open_file_read(c, path='/file')

        # Opening the file for second time succeeds, but the new descriptor is
        # added after read descriptor in open files list, and because of this it is not visible
        # for user and edit fails
        self._open_file_write(c, path='/file')
        rsp = c.ra_insert(rsp.node_id, rsp.revision, 0, 'data'.encode('utf-8'))
        self._validate_response(rsp, c, zyn_util.errors.FileOpenedInReadModeError)

    def test_max_number_of_files_open(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp = self._create_file_ra(c, 'file', parent_node_id=0)
        query = self._query_system(c)
        for _ in range(0, query.max_number_of_open_files_per_connection):
            self._open_file_read(c, path='/file')
        rsp = c.open_file_read(path='/file')
        self._validate_response(rsp, c, zyn_util.errors.TooManyFilesOpenError)

    def test_open_read_with_path_and_close_file(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp = self._create_file_ra(c, 'file', parent_node_id=0)
        self._open_file_read(c, path='/file')
        self._close_file(c, rsp.node_id)

    def test_open_read_with_node_id_and_close_file(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp = self._create_file_ra(c, 'file', parent_node_id=0)
        self._open_file_read(c, node_id=rsp.node_id)
        self._close_file(c, rsp.node_id)

    def test_open_write_with_path_and_close_file(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp = self._create_file_ra(c, 'file', parent_node_id=0)
        self._open_file_write(c, path='/file')
        self._close_file(c, rsp.node_id)

    def test_open_write_with_node_id_and_close_file(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp = self._create_file_ra(c, 'file', parent_node_id=0)
        self._open_file_write(c, node_id=rsp.node_id)
        self._close_file(c, rsp.node_id)

    def _validate_query_fs_children_response(
            self,
            elements,
            name,
            expected_node_id,
            expected_element_type,
            expected_file_type=None,
    ):
        for e in elements:
            if e.name != name:
                continue

            self.assertEqual(e.node_id, expected_node_id)
            self.assertEqual(e.type_of_element, expected_element_type)
            if expected_file_type is not None:
                self.assertEqual(e.file_type, expected_file_type)
            return
        raise RuntimeError('Element "{}" not found'.format(name))

    def test_query_fs_children(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp_ra = self._create_file_ra(c, 'file-ra', parent_path='/')
        rsp_blob = self._create_file_blob(c, 'file-blob', parent_path='/')
        rsp_dir = self._create_directory(c, 'dir', parent_path='/')
        rsp_query = self._query_fs_children(c, path='/')

        self._validate_query_fs_children_response(
            rsp_query.elements,
            'file-ra',
            rsp_ra.node_id,
            zyn_util.connection.FILESYSTEM_ELEMENT_FILE,
            zyn_util.connection.FILE_TYPE_RANDOM_ACCESS,
        )
        self._validate_query_fs_children_response(
            rsp_query.elements,
            'file-blob',
            rsp_blob.node_id,
            zyn_util.connection.FILESYSTEM_ELEMENT_FILE,
            zyn_util.connection.FILE_TYPE_BLOB,
        )
        self._validate_query_fs_children_response(
            rsp_query.elements,
            'dir',
            rsp_dir.node_id,
            zyn_util.connection.FILESYSTEM_ELEMENT_DIRECTORY,
        )

    def test_query_fs_children_when_file_is_being_edited(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        create = self._create_file_ra(c, 'file-ra', parent_path='/')

        # Query element and query element children should be up-to-date since file is closed
        query_list = self._query_fs_children(c, path='/')
        query_element = self._query_fs_element(c, node_id=create.node_id)
        self.assertEqual(query_list.elements[0].revision, create.revision)
        self.assertEqual(query_element.revision, create.revision)

        # Open file and edit it
        self._open_file_write(c, node_id=create.node_id)
        edit = c.ra_insert(create.node_id, create.revision, 0, 'dd'.encode('utf-8')).as_write_rsp()

        # Query children has the revision from last time file was closed since it
        # does not query open file thread.
        # Query element has latest revision as it does send request to file thread
        query_list = self._query_fs_children(c, path='/')
        query_element = self._query_fs_element(c, node_id=create.node_id)
        self.assertEqual(query_list.elements[0].revision, create.revision)
        self.assertEqual(query_element.revision, edit.revision)

        # Close file which updates the values seen by query children
        self._close_file(c, node_id=create.node_id)
        query_list = self._query_fs_children(c, path='/')
        query_element = self._query_fs_element(c, node_id=create.node_id)
        self.assertEqual(query_list.elements[0].revision, edit.revision)
        self.assertEqual(query_element.revision, edit.revision)

    def _validate_fs_query_element(
            self,
            query,
            expected_node_id,
            expected_element_type,
            expected_file_type=None,
            expected_block_size=None,
    ):
        self.assertEqual(query.type_of_element, expected_element_type)
        self.assertEqual(query.node_id, expected_node_id)
        if expected_block_size is not None:
            self.assertEqual(query.block_size, expected_block_size)
        if expected_file_type is not None:
            self.assertEqual(query.type_of_file, expected_file_type)

    def test_query_fs_element_random_access_file(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp_create = self._create_file_ra(c, 'file-ra', parent_path='/', block_size=5)
        rsp_query = self._query_fs_element(c, node_id=rsp_create.node_id)
        self._validate_fs_query_element(
            rsp_query,
            rsp_create.node_id,
            zyn_util.connection.FILESYSTEM_ELEMENT_FILE,
            zyn_util.connection.FILE_TYPE_RANDOM_ACCESS,
            5
        )

    def test_query_fs_element_blob_file(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp_create = self._create_file_blob(c, 'file-blob', parent_path='/', block_size=5)
        rsp_query = self._query_fs_element(c, node_id=rsp_create.node_id)
        self._validate_fs_query_element(
            rsp_query,
            rsp_create.node_id,
            zyn_util.connection.FILESYSTEM_ELEMENT_FILE,
            zyn_util.connection.FILE_TYPE_BLOB,
            5,
        )

    def test_query_fs_element_directory(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp_create = self._create_directory(c, 'dir', parent_path='/')
        rsp_query = self._query_fs_element(c, node_id=rsp_create.node_id)
        self._validate_fs_query_element(
            rsp_query,
            rsp_create.node_id,
            zyn_util.connection.FILESYSTEM_ELEMENT_DIRECTORY,
        )

    def test_query_fs_element_properties_file_ra(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp_create = self._create_file_ra(c, 'file', parent_path='/')
        rsp_query = self._query_fs_element_properties(
            c,
            node_id=rsp_create.node_id,
            parent_path='/'
        )
        self.assertTrue(rsp_query.is_file())
        self.assertTrue(rsp_query.is_random_access_file())
        self.assertEqual(rsp_query.node_id, rsp_create.node_id)
        self.assertEqual(rsp_query.revision, rsp_create.revision)
        self.assertEqual(rsp_query.name, 'file')

    def test_query_fs_element_properties_file_ra_after_edit(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp_create = self._create_file_ra(c, 'file', parent_path='/')
        node_id = rsp_create.node_id
        data = 'data'.encode('utf-8')

        rsp_query_1 = self._query_fs_element_properties(c, node_id=node_id, parent_path='/')
        self._open_file_write(c, node_id=node_id)
        rsp_edit = c.ra_write(node_id, rsp_create.revision, 0, data).as_write_rsp()

        rsp_query_2 = self._query_fs_element_properties(c, node_id=node_id, parent_path='/')
        self.assertEqual(rsp_query_1.revision, rsp_create.revision)
        self.assertEqual(rsp_query_2.revision, rsp_edit.revision)
        self.assertEqual(rsp_query_1.size, 0)
        self.assertEqual(rsp_query_2.size, len(data))

    def test_query_fs_element_properties_directory(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp_create = self._create_directory(c, 'dir', parent_path='/')
        rsp_query = self._query_fs_element_properties(
            c,
            node_id=rsp_create.node_id,
            parent_path='/'
        )
        self.assertTrue(rsp_query.is_directory())
        self.assertEqual(rsp_query.name, 'dir')

    def test_delete_file_with_node_id(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp = self._create_file_ra(c, 'file', parent_path='/')
        self._delete(c, node_id=rsp.node_id)
        self._validate_fs_element_does_not_exist(c, rsp.node_id)

    def test_delete_file_with_path(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp = self._create_file_ra(c, 'file', parent_path='/')
        self._delete(c, path='/file')
        self._validate_fs_element_does_not_exist(c, rsp.node_id)

    def test_delete_directory_with_path(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp = self._create_directory(c, 'dir', parent_path='/')
        self._delete(c, path='/dir')
        self._validate_fs_element_does_not_exist(c, rsp.node_id)

    def test_delete_non_empty_directory(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp = self._create_directory(c, 'dir', parent_path='/')
        rsp = self._create_file_ra(c, 'file', parent_node_id=rsp.node_id)
        rsp = c.delete(path='/dir')
        self._validate_response(rsp, c, zyn_util.errors.DirectoryIsNotEmpty)

    def test_files_created_on_server_workdir(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        files_1 = self._get_files_in_server_workdir()

        # After startup, system has two folders, root has three files
        self.assertEqual(len(files_1), 2)
        self.assertEqual(len(files_1['/']), 3)

        # Creating directory does not cause new fs element
        c.create_directory('folder', parent_path='/').as_create_rsp()
        files_2 = self._get_files_in_server_workdir(filter_directories=files_1.keys())
        self.assertEqual(len(files_2), 0)

        c.create_file_random_access('file-1', parent_path='/').as_create_rsp()
        files_3 = self._get_files_in_server_workdir(filter_directories=files_1.keys())

        # Creating file should create containing directory and directory for file content which is
        # split into two files by default
        self.assertEqual(len(files_3), 2)
        self.assertEqual(len(files_3[list(files_3)[0]]), 0)
        self.assertEqual(len(files_3[list(files_3)[1]]), 2)

        # Containing file exists so new file should only create one directory
        c.create_file_random_access('file-2', parent_path='/').as_create_rsp()
        files_4 = self._get_files_in_server_workdir(
            filter_directories=list(files_1) + list(files_3)
        )
        self.assertEqual(len(files_4), 1)
        self.assertEqual(len(files_4[list(files_4)[0]]), 2)

        # todo: validate blob creates new block file

    def test_delete_file_deletes_elements_on_disk(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        files_1 = self._get_files_in_server_workdir()

        rsp = c.create_file_random_access('file', parent_path='/').as_create_rsp()
        files_2 = self._get_files_in_server_workdir(filter_directories=list(files_1))
        self.assertEqual(len(files_2), 2)

        # Deleting file deletes the directory for file content, but leaves the
        # parent directory intact
        c.delete(node_id=rsp.node_id)
        files_3 = self._get_files_in_server_workdir(filter_directories=list(files_1))
        self.assertEqual(len(files_3), 1)


class TestBasicEditFile(TestBasicOperatinsCommon):
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

    def _blob_write(self, connection, node_id, revision, data, block_size=None):
        rsp = connection.blob_write(node_id, revision, data.encode('utf-8'), block_size)
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
        create_rsp = self._create_file_ra(c, 'file-1', parent_path='/')
        open_rsp = self._open_file_write(c, node_id=create_rsp.node_id)

        data = 'data'
        write_rsp = self._ra_write(c, create_rsp.node_id, open_rsp.revision, 0, data)
        self._read(c, create_rsp.node_id, 0, len(data), write_rsp.revision, data)

    def test_edit_random_access_file(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        create_rsp = self._create_file_ra(c, 'file-1', parent_path='/')
        open_rsp = self._open_file_write(c, node_id=create_rsp.node_id)

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
        create_rsp = self._create_file_blob(c, 'file-1', parent_path='/')
        open_rsp = self._open_file_write(c, node_id=create_rsp.node_id)

        rsp = self._blob_write(c, create_rsp.node_id, open_rsp.revision, 'data')
        self._read(c, create_rsp.node_id, 0, 10, rsp.revision, 'data')

        rsp = self._blob_write(c, create_rsp.node_id, rsp.revision, 'qwerty')
        self._read(c, create_rsp.node_id, 0, 10, rsp.revision, 'qwerty')

    def test_edit_blob_file_with_stream(self):
        block_size = 10
        c = self._start_and_connect_to_node_and_handle_auth()
        create_rsp = self._create_file_blob(c, 'file-1', parent_path='/', block_size=block_size)
        open_rsp = self._open_file_write(c, node_id=create_rsp.node_id)

        data = ('a' * block_size + 'b' * block_size)
        stream = DataStream(data.encode('utf-8'))
        rsp = c.blob_write_stream(open_rsp.node_id, open_rsp.revision, stream, block_size)
        self._validate_response(rsp, c)
        rsp = rsp.as_write_rsp()
        self._read(c, create_rsp.node_id, 0, 10, rsp.revision, data[0:10])
        self._read(c, create_rsp.node_id, 10, 10, rsp.revision, data[10:20])

    def test_read_blob_file_with_stream(self):
        block_size = 10
        c = self._start_and_connect_to_node_and_handle_auth()
        create_rsp = self._create_file_blob(c, 'file-1', parent_path='/', block_size=block_size)
        open_rsp = self._open_file_write(c, node_id=create_rsp.node_id)

        data = (
            'a' * block_size + 'b' * block_size + 'c' * 2
        ).encode('utf-8')
        c.blob_write(open_rsp.node_id, open_rsp.revision, data, block_size)

        class TestStream():
            def __init__(self, block_size):
                self.count = 0
                self.data = b''
                self.block_size = block_size

            def transaction_id(self):
                return None

            def handle_data(self, offset, data):
                self.count += 1
                self.data += data
                assert offset % self.block_size == 0

        stream = TestStream(block_size)
        c.read_file_stream(open_rsp.node_id, 0, len(data), block_size, stream)
        self.assertEqual(stream.count, 3)
        self.assertEqual(data, stream.data)

    def test_multiple_files_open_at_sametime(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        create_rsp_1 = self._create_file_blob(c, 'file-1', parent_path='/')
        create_rsp_2 = self._create_file_blob(c, 'file-2', parent_path='/')
        open_rsp_1 = self._open_file_write(c, node_id=create_rsp_1.node_id)
        open_rsp_2 = self._open_file_write(c, node_id=create_rsp_2.node_id)

        data_1 = 'qwerty'
        data_2 = 'zxcvbn'
        rsp_1 = self._ra_write(c, create_rsp_1.node_id, open_rsp_1.revision, 0, data_1)
        rsp_2 = self._ra_write(c, create_rsp_2.node_id, open_rsp_2.revision, 0, data_2)

        rsp, data = self._read(c, create_rsp_1.node_id, 0, 100, rsp_1.revision, data_1)
        rsp, data = self._read(c, create_rsp_2.node_id, 0, 100, rsp_2.revision, data_2)


class TestArguments(zyn_util.tests.common.TestCommon):
    def test_filesystem_capacity(self):
        self._start_node(filesystem_capacity=1)
        c = self._connect_to_node_and_handle_auth()
        self.assertFalse(c.create_file_random_access('file-1', parent_path='/').is_error())
        self.assertTrue(c.create_file_random_access('file-2', parent_path='/').is_error())

    def test_max_number_of_files_per_directory(self):
        self._start_node(max_number_of_files_per_directory=2)
        c = self._connect_to_node_and_handle_auth()

        files_1 = self._get_files_in_server_workdir()
        c.create_file_random_access('file-1', parent_path='/').as_create_rsp()

        # Creating one file should create two directories, one for file and one for its parent
        files_2 = self._get_files_in_server_workdir(filter_directories=list(files_1))
        self.assertEqual(len(files_2), 2)

        # Creating second file should only create one directory for file
        c.create_file_random_access('file-2', parent_path='/').as_create_rsp()
        files_3 = self._get_files_in_server_workdir(
            filter_directories=list(files_1) + list(files_2)
        )
        self.assertEqual(len(files_3), 1)

        # Creating third file should again create two directories, one for file and for its parent
        c.create_file_random_access('file-3', parent_path='/').as_create_rsp()
        files_4 = self._get_files_in_server_workdir(
            filter_directories=list(files_1) + list(files_2) + list(files_3)
        )
        self.assertEqual(len(files_4), 2)

    def test_create_file_block_size_random_access(self):
        max_block_size = 1024
        self._start_node(max_block_size_random_access=max_block_size)
        c = self._connect_to_node_and_handle_auth()
        self.assertFalse(c.create_file_random_access(
            'file-1',
            parent_path='/',
            block_size=max_block_size - 100
        ).is_error())
        self.assertTrue(c.create_file_random_access(
            'file-2',
            parent_path='/',
            block_size=max_block_size + 100
        ).is_error())

    def test_create_file_block_size_blob(self):
        max_block_size = 1024
        self._start_node(max_block_size_blob=max_block_size)
        c = self._connect_to_node_and_handle_auth()
        self.assertFalse(c.create_file_blob(
            'file-1',
            parent_path='/',
            block_size=max_block_size - 100
        ).is_error())
        self.assertTrue(c.create_file_blob(
            'file-2',
            parent_path='/',
            block_size=max_block_size + 100
        ).is_error())


class TestUserAuthority(zyn_util.tests.common.TestCommon):
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

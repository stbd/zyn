import zyn_util.tests.common
import zyn_util.errors


class TestBasicUsage(zyn_util.tests.common.TestCommon):
    def test_node_shutdown_causes_notification(self):
        self._start_node()
        c = self._connect_to_node()
        self._process.terminate()
        msg = c.read_message()
        self.assertEqual(msg.type(), zyn_util.connection.Message.NOTIFICATION)
        self.assertEqual(
            msg.notification_type(),
            zyn_util.connection.Notification.TYPE_DISCONNECTED
        )
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

    def test_query_system(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp = c.query_system()
        self._validate_response(rsp, c)
        query = rsp.as_query_system_rsp()
        self.assertNotEqual(query.started_at, 0)
        self.assertNotEqual(query.server_id, 0)


class TestBasicFilesystem(zyn_util.tests.common.TestCommon):
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

    def test_create_file_with_same_name_under_different_parent(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp = c.create_file_random_access('file', parent_node_id=0)
        self._validate_response(rsp, c)
        rsp = c.create_directory('folder', parent_node_id=0)
        self._validate_response(rsp, c)
        rsp = rsp.as_create_rsp()
        rsp = c.create_file_random_access('file', parent_node_id=rsp.node_id)
        self._validate_response(rsp, c)

    def test_create_directory_parent_node_id(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp = c.create_directory('folder-1', parent_node_id=0)
        self._validate_response(rsp, c)
        rsp.as_create_rsp()

    def test_create_directory_with_file(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        rsp = c.create_directory('folder-1', parent_node_id=0)
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
        self.assertNotEqual(open_rsp.block_size, 0)
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

    def _validate_query_fs_children_response(
            self,
            element,
            expected_name,
            expected_node_id,
            expected_element_type
    ):
        self.assertEqual(element.name, expected_name)
        self.assertEqual(element.node_id, expected_node_id)
        self.assertEqual(element.type_of_element, expected_element_type)

    def test_query_fs_children(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        create_rsp_file_1 = c.create_file_random_access('file-1', parent_path='/').as_create_rsp()
        create_rsp_folder = c.create_directory('folder-1', parent_path='/').as_create_rsp()
        create_rsp_file_2 = c.create_file_blob('file-2', parent_path='/').as_create_rsp()

        rsp = c.query_fs_children(path='/')
        self._validate_response(rsp, c)
        query_rsp = rsp.as_query_fs_children_rsp()

        self.assertEqual(query_rsp.number_of_elements(), 3)
        names = [e.name for e in query_rsp.elements]
        index_file_1 = names.index('file-1')
        index_file_2 = names.index('file-2')
        index_folder = names.index('folder-1')

        self._validate_query_fs_children_response(
            query_rsp.elements[index_file_1],
            'file-1',
            create_rsp_file_1.node_id,
            zyn_util.connection.FILESYSTEM_ELEMENT_FILE
        )
        self.assertEqual(query_rsp.elements[index_file_1].revision, 0)
        self.assertEqual(
            query_rsp.elements[index_file_1].file_type,
            zyn_util.connection.FILE_TYPE_RANDOM_ACCESS
        )
        self.assertEqual(query_rsp.elements[index_file_1].size, 0)

        self._validate_query_fs_children_response(
            query_rsp.elements[index_file_2],
            'file-2',
            create_rsp_file_2.node_id,
            zyn_util.connection.FILESYSTEM_ELEMENT_FILE
        )
        self.assertEqual(query_rsp.elements[index_file_2].revision, 0)
        self.assertEqual(
            query_rsp.elements[index_file_2].file_type,
            zyn_util.connection.FILE_TYPE_BLOB
        )
        self.assertEqual(query_rsp.elements[index_file_1].size, 0)

        self._validate_query_fs_children_response(
            query_rsp.elements[index_folder],
            'folder-1',
            create_rsp_folder.node_id,
            zyn_util.connection.FILESYSTEM_ELEMENT_DIRECTORY
        )

    def test_query_fs_element_random_access_file(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        create_rsp = c.create_file_random_access('file-1', parent_path='/').as_create_rsp()
        rsp = c.query_fs_element(node_id=create_rsp.node_id)
        self._validate_response(rsp, c)
        query_rsp = rsp.as_query_fs_element_rsp()
        self.assertEqual(query_rsp.type_of_element, zyn_util.connection.FILESYSTEM_ELEMENT_FILE)
        self.assertEqual(query_rsp.node_id, create_rsp.node_id)
        self.assertNotEqual(query_rsp.block_size, 0)
        self.assertTrue(query_rsp.is_random_access_file())
        self.assertFalse(query_rsp.is_blob_file())

    def test_query_fs_element_blob_file(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        create_rsp = c.create_file_blob('file-1', parent_path='/').as_create_rsp()
        rsp = c.query_fs_element(node_id=create_rsp.node_id)
        self._validate_response(rsp, c)
        query_rsp = rsp.as_query_fs_element_rsp()
        self.assertFalse(query_rsp.is_random_access_file())
        self.assertTrue(query_rsp.is_blob_file())

    def test_query_fs_element_folder(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        create_rsp = c.create_directory('folder-1', parent_path='/').as_create_rsp()
        rsp = c.query_fs_element(node_id=create_rsp.node_id)
        self._validate_response(rsp, c)
        query_rsp = rsp.as_query_fs_element_rsp()
        self.assertEqual(
            query_rsp.type_of_element,
            zyn_util.connection.FILESYSTEM_ELEMENT_DIRECTORY
        )
        self.assertEqual(query_rsp.node_id, create_rsp.node_id)

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

    def test_delete_directory_with_path(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        create_rsp = c.create_directory('folder', parent_path='/').as_create_rsp()
        rsp = c.delete(path='/folder')
        self._validate_response(rsp, c)
        self._validate_fs_element_does_not_exist(c, create_rsp.node_id)

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


class TestBasicEditFile(zyn_util.tests.common.TestCommon):
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
        create_rsp = c.create_file_random_access('file-1', parent_path='/').as_create_rsp()
        open_rsp = c.open_file_write(node_id=create_rsp.node_id).as_open_rsp()

        data = 'data'
        write_rsp = self._ra_write(c, create_rsp.node_id, open_rsp.revision, 0, data)
        self._read(c, create_rsp.node_id, 0, len(data), write_rsp.revision, data)

    def test_edit_random_access_file(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        create_rsp = c.create_file_random_access('file-1', parent_path='/').as_create_rsp()
        open_rsp = c.open_file_write(node_id=create_rsp.node_id).as_open_rsp()
        self.assertEqual(open_rsp.type_of_file, zyn_util.connection.FILE_TYPE_RANDOM_ACCESS)

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
        self.assertEqual(open_rsp.type_of_file, zyn_util.connection.FILE_TYPE_BLOB)

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

    def test_create_file_page_size_random_access(self):
        max_page_size = 1024
        self._start_node(max_page_size_random_access=max_page_size)
        c = self._connect_to_node_and_handle_auth()
        self.assertFalse(c.create_file_random_access(
            'file-1',
            parent_path='/',
            page_size=max_page_size - 100
        ).is_error())
        self.assertTrue(c.create_file_random_access(
            'file-2',
            parent_path='/',
            page_size=max_page_size + 100
        ).is_error())

    def test_create_file_page_size_blob(self):
        max_page_size = 1024
        self._start_node(max_page_size_blob=max_page_size)
        c = self._connect_to_node_and_handle_auth()
        self.assertFalse(c.create_file_blob(
            'file-1',
            parent_path='/',
            page_size=max_page_size - 100
        ).is_error())
        self.assertTrue(c.create_file_blob(
            'file-2',
            parent_path='/',
            page_size=max_page_size + 100
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

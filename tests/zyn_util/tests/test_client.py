import os

import zyn_util.tests.common
import zyn_util.errors
import zyn_util.client


def _join_paths(path_1, path_2):
    return '{}/{}'.format(path_1, path_2)


class ClientState:
    def __init__(self, path_workdir, path_state_file, path_data, zyn_client):
        self.path_workdir = path_workdir
        self.path_state_file = path_state_file
        self.path_data = path_data
        self.client = zyn_client

    def validate_text_file_content(self, path_in_remote, expected_text_content=None):
        expected_content = bytearray()
        if expected_text_content is not None:
            expected_content = expected_text_content.encode('utf-8')

        path_local = _join_paths(self.path_data, path_in_remote)
        assert os.path.exists(path_local)
        assert open(path_local, 'rb').read() == expected_content

    def write_local_file_text(self, path_in_remote, text_content):
        path_file = _join_paths(self.path_data, path_in_remote)
        assert os.path.exists(path_file)
        data = text_content.encode('utf-8')
        open(path_file, 'wb').write(data)

    def sync(self, path_in_remote):
        path_local = _join_paths(self.path_data, path_in_remote)
        self.client.sync(path_local)


class TestClient(zyn_util.tests.common.TestCommon):
    def _path_clients_data(self):
        return '{}/clients'.format(self._work_dir.name)

    def _init_client(self, client_id, connection, init_data=True):
        path_clients_data = self._path_clients_data()
        if not os.path.exists(path_clients_data):
            os.mkdir(path_clients_data)

        path_client_workdir = '{}/{}'.format(path_clients_data, client_id)
        path_client_state = path_client_workdir + '/client-state'
        path_client_data = path_client_workdir + '/client-data'

        if init_data:
            os.mkdir(path_client_workdir)
            os.mkdir(path_client_data)
            zyn_util.client.ZynFilesystemClient.init_state_file(path_client_state, path_client_data)

        client = zyn_util.client.ZynFilesystemClient(
            connection,
            path_client_state
        )
        return ClientState(
            path_client_workdir,
            path_client_state,
            path_client_data,
            client,
        )

    def _create_client(self, client_id, init_data=True):
        connection = self._connect_to_node()
        self._handle_auth(connection)
        return self._init_client(client_id, connection, init_data)

    def _start_server_and_create_client(self, client_id):
        connection = self._start_and_connect_to_node_and_handle_auth()
        return self._init_client(client_id, connection)

    def _start_server_and_create_number_of_clients(self, number_of_clients):
        client_1 = self._start_server_and_create_client(client_id=1)
        clients = [self._create_client(i) for i in range(2, number_of_clients + 1)]
        return [client_1] + clients

    def test_resume_client(self):
        client_state_1 = self._start_server_and_create_client(1)
        path_in_remote = '/test_file'
        data_1 = 'data'
        data_2 = 'datazxcc'

        client_state_1.client.create_random_access_file(path_in_remote)
        client_state_1.client.fetch(path_in_remote)
        client_state_1.write_local_file_text(path_in_remote, data_1)
        client_state_1.sync(path_in_remote)
        client_state_1.client.store()

        client_state_2 = self._create_client(1, False)
        client_state_2.write_local_file_text(path_in_remote, data_2)
        client_state_1.sync(path_in_remote)

    def test_edit_fetch_random_access_file(self):
        client_state_1, client_state_2 = self._start_server_and_create_number_of_clients(2)
        path_in_remote = '/test_file'
        data = 'data'

        client_state_1.client.create_random_access_file(path_in_remote)
        client_state_1.client.fetch(path_in_remote)
        client_state_1.validate_text_file_content(path_in_remote, None)
        client_state_1.write_local_file_text(path_in_remote, data)
        client_state_1.sync(path_in_remote)

        client_state_2.client.fetch(path_in_remote)
        client_state_2.validate_text_file_content(path_in_remote, data)

    def test_sync_random_access_file_after_file_already_fetched(self):
        client_state_1, client_state_2 = self._start_server_and_create_number_of_clients(2)
        path_in_remote = '/test_file'
        data_1 = 'data'

        client_state_1.client.create_random_access_file(path_in_remote)
        client_state_1.client.fetch(path_in_remote)
        client_state_2.client.fetch(path_in_remote)

        client_state_1.write_local_file_text(path_in_remote, data_1)
        client_state_1.sync(path_in_remote)
        client_state_2.sync(path_in_remote)


'''
    def _prepare_data_directories(self):
        self._client_temp_dir = self._temp_dir()
        self._client_data_dir = self._client_temp_dir.name + '/data'
        self._client_work_dir = self._client_temp_dir.name + '/work-dir'
        os.mkdir(self._client_data_dir)
        os.mkdir(self._client_work_dir)

    def _create_client(self):
        conn = self._create_connection()
        conn.enable_debug_messages()
        conn.connect(self._remote_ip, self._remote_port)

        c = zyn_util.client.ZynFilesystemClient(
            conn,
            self._client_data_dir,
            self._client_work_dir,
        )
        return c

    def _create_client_and_authenticate(self):
        client = self._create_client()
        client.authenticate(self._username, self._password)
        return client

    def _open_from_data_dir(self, path, mode='rb'):
        return open(self._client_data_dir + path, mode)

    def _open_from_work_dir(self, path, mode='rb'):
        return open(self._client_work_dir + path, mode)

    def _open_local_file_for_edit(self, path):
        return self._open_from_data_dir(path, 'wb')

    def _encode(self, content):
        return content.encode('utf-8')

    def _write_to_file_sync_validate_content(self, client, path, content):
        self._open_local_file_for_edit(path).write(content)
        client.sync(path)
        self._validate_file_content(client, path, content)

    def _validate_file_content(self, client, path, content):
        client.remove_local_file(path)
        client.fetch(path)
        self.assertEqual(self._open_from_data_dir(path).read(), content)

    def test_create_file(self):
        self._start_node()
        client = self._create_client_and_authenticate()
        client.create_random_access_file('/file')

    def test_fetch(self):
        self._start_node()
        client = self._create_client_and_authenticate()
        client.create_random_access_file('/file')
        client.fetch('/file')
        self._open_from_data_dir('/file')
        self._open_from_work_dir('/file')

    def test_sync(self):
        content = self._encode('line-1\nline-2\n')
        self._start_node()
        client = self._create_client_and_authenticate()
        client.create_random_access_file('/file')
        client.fetch('/file')
        self._open_local_file_for_edit('/file').write(content)
        client.sync('/file')
        self._validate_file_content(client, '/file', content)

    def test_sync_multiple_times(self):
        self._start_node()
        client = self._create_client_and_authenticate()
        client.create_random_access_file('/file')
        client.fetch('/file')
        content_1 = self._encode('line1\nline2\n')
        self._open_local_file_for_edit('/file').write(content_1)
        client.sync('/file')

        content_2 = self._encode('line1\nline2\nline3\n')
        self._open_local_file_for_edit('/file').write(content_2)
        client.sync('/file')
        self._validate_file_content(client, '/file', content_2)

    def test_serialization(self):
        content = self._encode('line-1\nline-2\n')
        self._start_node()
        client = self._create_client_and_authenticate()
        client.create_random_access_file('/file')
        client.fetch('/file')
        client.store()
        client.disconnect()

        self._open_local_file_for_edit('/file').write(content)
        client = self._create_client_and_authenticate()
        client.sync('/file')
        self._validate_file_content(client, '/file', content)

    def test_edit(self):
        line_1 = self._encode('line-1\n')
        line_2 = self._encode('line-2\n')
        line_3 = self._encode('line-3\n')

        self._start_node()
        client = self._create_client_and_authenticate()
        client.create_random_access_file('/file')
        client.fetch('/file')

        self._write_to_file_sync_validate_content(client, '/file', line_1 + line_2 + line_3)
        self._write_to_file_sync_validate_content(client, '/file', line_1 + line_3)
        self._write_to_file_sync_validate_content(client, '/file', line_1 + line_3 + line_3)
        self._write_to_file_sync_validate_content(client, '/file', line_1 + line_2 + line_3)

    def test_complex_file_hierarchy(self):
        self._start_node()
        client = self._create_client_and_authenticate()
        client.create_folder('/folder-1')
        client.create_folder('/folder-2')
        client.create_random_access_file('/file-1')
        client.create_random_access_file('/folder-1/file-1')
        client.create_random_access_file('/folder-1/file-2')
        client.create_random_access_file('/folder-2/file-1')

        self.assertEqual(len(client.query_list('/')), 3)
        self.assertEqual(len(client.query_list('/folder-1')), 2)
        self.assertEqual(len(client.query_list('/folder-2')), 1)

        self.assertEqual(
            client.query_filesystem('/folder-1')['type'],
            zyn_util.connection.FILE_TYPE_FOLDER)
        self.assertEqual(
            client.query_filesystem('/folder-1/file-1')['type'],
            zyn_util.connection.FILE_TYPE_FILE)
'''

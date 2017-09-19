import logging
import os

import zyn_util.tests.common
import zyn_util.errors
import zyn_util.client


class TestClient(zyn_util.tests.common.TestZyn):

    def setUp(self):
        super(TestClient, self).setUp()
        self._process = self._start_node_default_params(self._work_dir.name, init=True)
        self._client_temp_dir = None
        self._client_data_dir = None
        self._client_work_dir = None
        self._prepare_data_directories()

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

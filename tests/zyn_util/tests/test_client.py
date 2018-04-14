import os
import random

import zyn_util.tests.common
import zyn_util.errors
import zyn_util.client


def _join_paths(path_1, path_2):
    return '{}/{}'.format(path_1, path_2)


def _join_paths_(list_of_paths):
    path = os.path.normpath('/'.join(list_of_paths))
    if path.startswith('//'):
        path = path[1:]
    return path


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

    def create_directory(self, path_remote):
        path_local = _join_paths(self.path_data, path_remote)
        os.makedirs(path_local)

    def create_local_file(self, path_remote):
        path_local = _join_paths_([self.path_data, path_remote])
        open(path_local, 'w')


class TestClient(zyn_util.tests.common.TestCommon):
    def _path_clients_data(self):
        return '{}/clients'.format(self._work_dir.name)

    def _restart_server_and_replace_connection(self, client_state):
        connection = self._restart_node_and_connect_and_handle_auth(
            init=True,
            server_workdir='server-workdir-{}'.format(random.random())
        )
        client_state.client.set_connection(connection)

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
        data_1 = 'data'
        data_2 = 'datazxcc'
        filename, path_remote = self._name_to_remote_path('file-1')

        client_state_1 = self._start_server_and_create_client(1)
        client_state_1.client.create_random_access_file(path_remote)
        client_state_1.client.fetch(path_remote)
        client_state_1.write_local_file_text(path_remote, data_1)
        client_state_1.client.sync(path_remote)

        tracked_files, _ = client_state_1.client.list('/')
        self.assertEqual(len(tracked_files), 1)
        self._validate_tracked_file(tracked_files, filename, exists_locally=True, tracked=True)
        client_state_1.client.store()

        client_state_2 = self._create_client(1, False)
        tracked_files, _ = client_state_2.client.list('/')
        self.assertEqual(len(tracked_files), 1)
        self._validate_tracked_file(tracked_files, filename, exists_locally=True, tracked=True)

        client_state_2.write_local_file_text(path_remote, data_2)
        client_state_2.client.sync(path_remote)

    def test_validating_server(self):
        client_state_1 = self._start_server_and_create_client(1)

        self.assertFalse(client_state_1.client.is_server_info_initialized())
        client_state_1.client.initialize_server_info()
        self.assertTrue(client_state_1.client.is_server_info_initialized())
        client_state_1.client.store()

        client_state_2 = self._create_client(1, False)
        self.assertTrue(client_state_2.client.is_server_info_initialized())
        self.assertTrue(client_state_2.client.is_connected_to_same_server())

        # todo: add case where server is restarted to verify that it is noticed

    def test_edit_fetch_random_access_file(self):
        client_state_1, client_state_2 = self._start_server_and_create_number_of_clients(2)
        path_in_remote = '/test_file'
        data = 'data'

        client_state_1.client.create_random_access_file(path_in_remote)
        client_state_1.client.fetch(path_in_remote)
        client_state_1.validate_text_file_content(path_in_remote, None)
        client_state_1.write_local_file_text(path_in_remote, data)
        client_state_1.client.sync(path_in_remote)

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
        client_state_1.client.sync(path_in_remote)
        client_state_2.client.sync(path_in_remote)

    def _validate_tracked_file(self, tracked_files, name, exists_locally, tracked):
        for f in tracked_files:
            if f.remote_file.name != name:
                continue
            self.assertEqual(f.local_file.tracked, tracked)
            self.assertEqual(f.local_file.exists_locally, exists_locally)
            return
        self.assertFalse('tracked file not found')

    def _name_to_remote_path(self, filename, path_parent='/'):
        path = _join_paths_([path_parent, filename])
        return filename, path

    def test_list(self):
        client_state_1 = self._start_server_and_create_client(1)
        remote_tracked_1, path_remote_tracked_1 = self._name_to_remote_path('tracked_file-1')
        remote_tracked_2, path_remote_tracked_2 = self._name_to_remote_path('tracked_file-2')
        remote_tracked_3, path_remote_tracked_3 = self._name_to_remote_path('tracked_file-3')
        remote_untracked_1, path_remote_untracked_1 = self._name_to_remote_path('untracked_file-1')

        client_state_1.client.create_random_access_file(path_remote_tracked_1)
        client_state_1.client.fetch(path_remote_tracked_1)

        client_state_1.client.create_random_access_file(path_remote_tracked_2)
        client_state_1.create_local_file(path_remote_tracked_2)

        client_state_1.client.create_random_access_file(path_remote_tracked_3)

        client_state_1.create_local_file(path_remote_untracked_1)

        tracked_files, untracked_files = client_state_1.client.list('/')
        self.assertEqual(len(tracked_files), 3)
        self._validate_tracked_file(
            tracked_files, remote_tracked_1, exists_locally=True, tracked=True)
        self._validate_tracked_file(
            tracked_files, remote_tracked_2, exists_locally=True, tracked=False)
        self._validate_tracked_file(
            tracked_files, remote_tracked_3, exists_locally=False, tracked=False)
        self.assertEqual(len(untracked_files), 1)
        self.assertTrue(path_remote_untracked_1 in untracked_files)

    def test_add(self):
        client_state = self._start_server_and_create_client(1)
        path_remote_1 = '/file-1'
        path_remote_2 = '/file-2'

        client_state.create_local_file(path_remote_1)
        client_state.client.add(path_remote_1)

        client_state.create_local_file(path_remote_2)
        client_state.write_local_file_text(path_remote_2, 'Hello')
        client_state.client.add(path_remote_2)

    def test_add_tracked_files_to_remote_after_restart(self):
        client_state = self._start_server_and_create_client(1)
        filename_1, path_remote_1 = self._name_to_remote_path('file-1')
        filename_2, path_remote_2 = self._name_to_remote_path('file-2')

        client_state.create_local_file(path_remote_1)
        client_state.write_local_file_text(path_remote_1, 'data-1')
        client_state.create_local_file(path_remote_2)
        client_state.write_local_file_text(path_remote_2, 'data-2')
        client_state.client.add(path_remote_1)
        client_state.client.add(path_remote_2)

        self._restart_server_and_replace_connection(client_state)
        client_state.client.add_tracked_files_to_remote()

        tracked_files, _ = client_state.client.list('/')
        self.assertEqual(len(tracked_files), 2)
        self._validate_tracked_file(tracked_files, filename_1, exists_locally=True, tracked=True)
        self._validate_tracked_file(tracked_files, filename_1, exists_locally=True, tracked=True)

    def test_reconnecting_to_server(self):
        client_state = self._start_server_and_create_client(1)
        filename_1, path_remote_1 = self._name_to_remote_path('file-1')
        filename_2, path_remote_2 = self._name_to_remote_path('file-2')

        client_state.create_local_file(path_remote_1)
        client_state.write_local_file_text(path_remote_1, 'data-1')
        client_state.create_local_file(path_remote_2)
        client_state.write_local_file_text(path_remote_2, 'data-2')
        client_state.client.add(path_remote_1)
        client_state.client.add(path_remote_2)

        client_state.client.set_connection(self._connect_to_node_and_handle_auth())

        tracked_files, _ = client_state.client.list('/')
        self.assertEqual(len(tracked_files), 2)
        self._validate_tracked_file(tracked_files, filename_1, exists_locally=True, tracked=True)
        self._validate_tracked_file(tracked_files, filename_1, exists_locally=True, tracked=True)

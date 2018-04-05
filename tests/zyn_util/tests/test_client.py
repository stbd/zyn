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
        client_state_1.client.sync(path_in_remote)
        client_state_1.client.store()

        client_state_2 = self._create_client(1, False)
        client_state_2.write_local_file_text(path_in_remote, data_2)
        client_state_1.client.sync(path_in_remote)

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

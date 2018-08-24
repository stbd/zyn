import io
import glob
import os
import os.path
import random
import sys

import zyn_util.tests.common
import zyn_util.errors
import zyn_util.client
import zyn_util.cli_client


def _join_paths(list_of_paths):
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

    def validate_local_data(self, expected_elements):
        elements = \
                   glob.glob(_join_paths([self.path_data, '/**/*'])) \
                   + glob.glob(_join_paths([self.path_data, '/*']))

        assert len(elements) == len(expected_elements)

        for e in elements:
            path_zyn = e.replace(self.path_data, '')
            assert path_zyn in expected_elements
            desc = expected_elements[path_zyn]
            if os.path.isdir(e):
                assert desc['type'] == 'd'
            elif os.path.isfile(e):
                assert desc['type'] == 'f'
            else:
                assert not 'Invalid file type'

    def validate_text_file_content(self, path_in_remote, expected_text_content=None):
        expected_content = bytearray()
        if expected_text_content is not None:
            expected_content = expected_text_content.encode('utf-8')

        path_local = _join_paths([self.path_data, path_in_remote])
        assert os.path.exists(path_local)
        content = open(path_local, 'rb').read()
        if content != expected_content:
            print('File content does not match the excepted content')
            print('Content:')
            print(content)
            print('----')
            print('Expected:')
            print(expected_content)
            print('----')
        assert open(path_local, 'rb').read() == expected_content

    def write_local_file_text(self, path_in_remote, text_content):
        path_file = _join_paths([self.path_data, path_in_remote])
        assert os.path.exists(path_file)
        data = text_content.encode('utf-8')
        open(path_file, 'wb').write(data)

    def create_directory(self, path_remote):
        path_local = _join_paths([self.path_data, path_remote])
        os.makedirs(path_local)

    def create_local_file(self, path_remote, content=None):
        path_local = _join_paths([self.path_data, path_remote])
        open(path_local, 'w').close()
        if content is not None:
            self.write_local_file_text(path_remote, content)


class TestClients(zyn_util.tests.common.TestCommon):
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


class TestClient(TestClients):
    def _cli_client(self, number_of_clients=1):
        if number_of_clients == 1:
            client_state, = self._start_server_and_create_number_of_clients(number_of_clients)
            return client_state, zyn_util.cli_client.ZynCliClient(client_state.client)
        else:
            client_states = self._start_server_and_create_number_of_clients(number_of_clients)
            return [(s, zyn_util.cli_client.ZynCliClient(s.client)) for s in client_states]

    def _params(self, params):
        params = [p for p in params if p]
        if len(params) > 1:
            return ' '.join(params)
        else:
            return params[0]

    def _to_filenames(self, elements):
        return [e.split('/')[-1] for e in elements]

    def _create_directory(self, cli, path):
        cli.do_create_directory(path)
        return path

    def _create_file(self, cli, path, create_parameters=None):
        cli.do_create_file(self._params([create_parameters, path]))
        return path

    def _create_directory_and_fetch(self, cli, path):
        cli.do_create_directory(path)
        cli.do_fetch('-p ' + path)
        return path

    def _create_file_and_fetch(self, cli, path, create_parameters=None):
        cli.do_create_file(self._params([create_parameters, path]))
        cli.do_fetch('-p ' + path)
        return path

    def _create_file_and_add(self, state, cli, path, add_parameters=None, content=None):
        state.create_local_file(path, content)
        cli.do_add(self._params([add_parameters, path]))
        return path

    def _validate_tracked_files(self, state, path, expected_files):
        tracked_files, _ = state.client.list(path)
        tracked_files_names = sorted([f.remote_file.name for f in tracked_files])
        expected_files = sorted(expected_files)
        assert len(tracked_files_names) == len(expected_files)
        if tracked_files_names != expected_files:
            print('Expected files do not match tracked files')
            print('Expected')
            print(expected_files)
            print('Tracked')
            print(tracked_files_names)
            assert False

    def _write_to_stdin(self, content):
        sys.stdin = io.StringIO(content)

    def test_resume_client(self):
        state_1, cli = self._cli_client()
        path_dir = self._create_directory_and_fetch(cli, "/dir")
        path_file_1 = self._create_file_and_fetch(cli, "/dir/file", '-ra')
        path_file_2 = self._create_file_and_fetch(cli, "/file-1", '-ra')
        state_1.client.store()
        state_1.validate_local_data({
            path_dir: {'type': 'd'},
            path_file_1: {'type': 'f'},
            path_file_2: {'type': 'f'},
        })

        state_2 = self._create_client(1, False)
        self._validate_tracked_files(state_2, '/', self._to_filenames([path_dir, path_file_2]))
        self._validate_tracked_files(state_2, path_dir, self._to_filenames([path_file_1]))

    def test_validating_server(self):
        state_1 = self._start_server_and_create_client(1)
        self.assertFalse(state_1.client.is_server_info_initialized())
        state_1.client.initialize_server_info()
        self.assertTrue(state_1.client.is_server_info_initialized())
        state_1.client.store()
        self._restart_server_and_replace_connection(state_1)
        self.assertTrue(state_1.client.is_server_info_initialized())
        self.assertFalse(state_1.client.is_connected_to_same_server())

    def test_add_tracked_files_to_remote_after_restart(self):
        state_1, cli = self._cli_client()
        path_dir = self._create_directory_and_fetch(cli, "/dir")
        path_file_1 = self._create_file_and_fetch(cli, "/dir/file-1", '-ra')
        path_file_2 = self._create_file_and_fetch(cli, "/dir/file-2", '-ra')
        path_file_3 = self._create_file_and_fetch(cli, "/file", '-ra')

        self._restart_server_and_replace_connection(state_1)
        state_1.client.add_tracked_files_to_remote()
        self._validate_tracked_files(state_1, '/', self._to_filenames([path_dir, path_file_3]))
        self._validate_tracked_files(
            state_1,
            path_dir,
            self._to_filenames([path_file_1, path_file_2])
        )

    def test_create_random_access_file_and_add(self):
        state, cli = self._cli_client()
        self._create_file_and_add(state, cli, '/file', '-ra')

    def test_create_blob_file_and_add(self):
        state, cli = self._cli_client()
        self._create_file_and_add(state, cli, '/file', '-b')

    def test_create_random_access_file_and_fetch(self):
        state, cli = self._cli_client()
        self._create_file_and_fetch(cli, "/file", '-ra')

    def test_create_blob_file_and_fetch(self):
        state, cli = self._cli_client()
        self._create_file_and_fetch(cli, "/file", '-b')

    def test_change_directory(self):
        state, cli = self._cli_client()
        path_dir_1 = self._create_directory_and_fetch(cli, "/dir-1")
        path_dir_2 = self._create_directory_and_fetch(cli, "/dir-2")
        self.assertEqual(cli.get_pwd(), '/')
        cli.do_cd(path_dir_1)
        self.assertEqual(cli.get_pwd(), path_dir_1)
        cli.do_cd('..')
        self.assertEqual(cli.get_pwd(), '/')
        cli.do_cd(path_dir_2)
        self.assertEqual(cli.get_pwd(), path_dir_2)
        cli.do_cd('../' + path_dir_1)
        self.assertEqual(cli.get_pwd(), path_dir_1)

    def test_remove_file_without_deleting_local_file(self):
        state, cli = self._cli_client()
        path_file = self._create_file_and_fetch(cli, "/file", '-b')
        cli.do_remove(self._params([path_file]))
        state.validate_local_data({path_file: {'type': 'f'}})

    def test_remove_file_and_delete_local_file(self):
        state, cli = self._cli_client()
        path_file = self._create_file_and_fetch(cli, "/file", '-b')
        self._write_to_stdin('yes')
        cli.do_remove(self._params([path_file, '-dl']))
        state.validate_local_data({})

    def test_remove_remote_only_file(self): pass  # todo: update server to support deleting dir

    def test_sync_all_with_changes(self):
        data = 'qwerty'
        state, cli = self._cli_client()
        self._create_directory_and_fetch(cli, '/dir-1')
        self._create_directory_and_fetch(cli, '/dir-2')
        path_file_1 = self._create_file_and_fetch(cli, '/dir-1/file-1', '-ra')
        path_file_2 = self._create_file_and_fetch(cli, '/dir-1/file-2', '-ra')
        path_file_3 = self._create_file_and_fetch(cli, '/dir-2/file', '-ra')

        state.write_local_file_text(path_file_1, data)
        state.write_local_file_text(path_file_2, data)
        state.write_local_file_text(path_file_3, '')
        self.assertEqual(cli.do_sync(''), 2)

    def test_sync_random_access_file(self):
        state, cli = self._cli_client()
        self._create_file_and_fetch(cli, '/file-1', '-ra')
        path_file_2 = self._create_file_and_fetch(cli, '/file-3', '-ra')
        state.write_local_file_text(path_file_2, 'data')
        self.assertEqual(cli.do_sync('-p ' + path_file_2), 1)

    def test_sync_blob_file(self):
        state, cli = self._cli_client()
        self._create_file_and_fetch(cli, '/file-1', '-b')
        path_file_2 = self._create_file_and_fetch(cli, '/file-3', '-b')
        state.write_local_file_text(path_file_2, 'data')
        self.assertEqual(cli.do_sync('-p ' + path_file_2), 1)

    def test_sync_directory(self):
        data = 'qwerty'
        state, cli = self._cli_client()
        path_dir_1 = self._create_directory_and_fetch(cli, '/dir-1')
        self._create_directory_and_fetch(cli, '/dir-2')
        path_file_1 = self._create_file_and_fetch(cli, '/dir-1/file-1', '-ra')
        path_file_2 = self._create_file_and_fetch(cli, '/dir-1/file-2', '-ra')
        path_file_3 = self._create_file_and_fetch(cli, '/dir-2/file', '-ra')
        path_file_4 = self._create_file_and_fetch(cli, '/file-root', '-ra')

        state.write_local_file_text(path_file_1, data)
        state.write_local_file_text(path_file_2, data)
        state.write_local_file_text(path_file_3, data)
        state.write_local_file_text(path_file_4, data)
        self.assertEqual(cli.do_sync('-p ' + path_dir_1), 2)

    def test_sync_between_multiple_clients(self):
        data_1 = '1111111111'
        data_2 = '2222222222'
        [(state_1, cli_1), (state_2, cli_2)] = self._cli_client(2)
        self._create_directory_and_fetch(cli_1, '/dir-1')
        path_file_1 = self._create_file_and_fetch(cli_1, '/dir-1/file-1', '-ra')
        self._create_file_and_fetch(cli_1, '/dir-1/file-2', '-ra')

        state_1.write_local_file_text(path_file_1, data_1)
        self.assertEqual(cli_1.do_sync(''), 1)

        cli_2.do_fetch('')
        state_2.validate_text_file_content(path_file_1, data_1)

        state_2.write_local_file_text(path_file_1, data_2)
        self.assertEqual(cli_2.do_sync(''), 1)

        self.assertEqual(cli_1.do_sync(''), 1)
        state_1.validate_text_file_content(path_file_1, data_2)

    def _init_two_clients_into_state_where_one_file_conflicts(self):
        data_1 = '1111'
        data_2 = '2222'
        [(state_1, cli_1), (state_2, cli_2)] = self._cli_client(2)
        path_file_1 = self._create_file_and_fetch(cli_1, '/file-1', '-ra')
        path_file_2 = self._create_file_and_fetch(cli_1, '/file-2', '-ra')
        path_file_3 = self._create_file_and_fetch(cli_1, '/file-3', '-ra')

        cli_2.do_fetch('')

        state_1.write_local_file_text(path_file_1, data_1)
        state_1.write_local_file_text(path_file_2, data_1)
        state_1.write_local_file_text(path_file_3, data_1)
        cli_1.do_sync('')

        # path_file_2 now has edits on both sides
        state_2.write_local_file_text(path_file_2, data_2)

        return [(state_1, cli_1), (state_2, cli_2)]

    def test_sync_error_does_not_stop_processing(self):
        [(state_1, cli_1), (state_2, cli_2)] = \
            self._init_two_clients_into_state_where_one_file_conflicts()

        self.assertEqual(cli_2.do_sync(''), 2)

    def test_sync_error_stops_processing(self):
        [(state_1, cli_1), (state_2, cli_2)] = \
            self._init_two_clients_into_state_where_one_file_conflicts()

        with self.assertRaises(NotImplementedError):
            cli_2.do_sync('--stop-on-error')

    def test_fetch_all(self):
        state, cli = self._cli_client()
        path_dir_1 = self._create_directory(cli, '/dir-1')
        path_dir_2 = self._create_directory(cli, '/dir-2')
        path_file_1 = self._create_file(cli, '/dir-1/file-1', '-ra')
        path_file_2 = self._create_file(cli, '/dir-1/file-2', '-ra')
        path_file_3 = self._create_file(cli, '/dir-2/file', '-ra')
        path_file_4 = self._create_file(cli, '/root-file', '-ra')

        cli.do_fetch('')
        state.validate_local_data({
            path_dir_1: {'type': 'd'},
            path_dir_2: {'type': 'd'},
            path_file_1: {'type': 'f'},
            path_file_2: {'type': 'f'},
            path_file_3: {'type': 'f'},
            path_file_4: {'type': 'f'},
        })

    def test_fetch_directory(self):
        state, cli = self._cli_client()
        path_dir_1 = self._create_directory(cli, '/dir-1')
        self._create_directory(cli, '/dir-2')
        path_file_1 = self._create_file(cli, '/dir-1/file-1', '-ra')
        path_file_2 = self._create_file(cli, '/dir-1/file-2', '-ra')
        self._create_file(cli, '/dir-2/file', '-ra')
        self._create_file(cli, '/root-file', '-ra')

        cli.do_fetch('-p ' + path_dir_1)
        state.validate_local_data({
            path_dir_1: {'type': 'd'},
            path_file_1: {'type': 'f'},
            path_file_2: {'type': 'f'},
        })

    def test_fetch_file(self):
        state, cli = self._cli_client()
        self._create_directory(cli, '/dir-1')
        self._create_file(cli, '/dir-1/file', '-ra')
        path_file_2 = self._create_file(cli, '/file-1', '-ra')
        self._create_file(cli, '/file-2', '-ra')

        cli.do_fetch('-p ' + path_file_2)
        state.validate_local_data({
            path_file_2: {'type': 'f'},
        })

    def test_fetch_only_new_elements(self):
        state, cli = self._cli_client()
        path_dir_1 = self._create_directory(cli, '/dir-1')
        path_file_1 = self._create_file(cli, '/dir-1/file', '-ra')
        path_file_2 = self._create_file(cli, '/file-1', '-ra')
        path_file_3 = self._create_file(cli, '/file-2', '-ra')

        cli.do_fetch('')
        state.validate_local_data({
            path_dir_1: {'type': 'd'},
            path_file_1: {'type': 'f'},
            path_file_2: {'type': 'f'},
            path_file_3: {'type': 'f'},
        })

        path_dir_2 = self._create_directory(cli, '/dir-2')
        path_file_4 = self._create_file(cli, '/file-3', '-ra')
        path_file_5 = self._create_file(cli, '/dir-2/file-4', '-ra')

        cli.do_fetch('')
        state.validate_local_data({
            path_dir_1: {'type': 'd'},
            path_dir_2: {'type': 'd'},
            path_file_1: {'type': 'f'},
            path_file_2: {'type': 'f'},
            path_file_3: {'type': 'f'},
            path_file_4: {'type': 'f'},
            path_file_5: {'type': 'f'},
        })

    def test_open_file(self): pass  # Only for random access

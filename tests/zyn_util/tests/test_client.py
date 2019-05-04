import glob
import io
import os
import os.path
import random
import sys

import zyn_util.cli_client
import zyn_util.client
import zyn_util.errors
import zyn_util.tests.common
import zyn_util.util


class ClientData:
    def __init__(self, client_id, path_workdir, path_client_state, path_data):
        self.client_id = client_id
        self.path_workdir = path_workdir
        self.path_data = path_data
        self.path_state_file = path_client_state
        self.client = zyn_util.client.ZynFilesystemClient.init_from_saved_state(
            self.path_state_file
        )
        self.cli = zyn_util.cli_client.ZynCliClient(self.client)

    def restart_client(self):
        self.client.store(self.path_state_file)
        self.client = zyn_util.client.ZynFilesystemClient.init_from_saved_state(
            self.path_state_file
        )
        self.cli = zyn_util.cli_client.ZynCliClient(self.client)

    def validate_local_data(self, expected_elements):
        elements = \
                   glob.glob(zyn_util.util.join_remote_paths([self.path_data, '/**/*'])) \
                   + glob.glob(zyn_util.util.join_remote_paths([self.path_data, '/*']))

        if len(elements) != len(expected_elements):
            print('Number of filesystem elements do not match')
            print('Expected: {}, found: {}'.format(len(elements), len(expected_elements)))
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

        path_local = zyn_util.util.join_remote_paths([self.path_data, path_in_remote])
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

    def element(self, path_remote):
        return self.client.element(path_remote=path_remote)

    def validate_element_is_tracked(self, path_remote, expected=True):
        is_tracked = True
        try:
            self.client.element(path_remote=path_remote)
        except Exception:
            is_tracked = False

        if is_tracked != expected:
            print('Element "{}" did not match tracked status'.format(path_remote))
            assert is_tracked == expected

    def validate_local_file_exists(self, path_in_remote, expected=True):
        path_local = zyn_util.util.join_remote_paths([self.path_data, path_in_remote])
        assert os.path.isfile(path_local) is expected

    def validate_local_directory_exists(self, path_in_remote, expected=True):
        path_local = zyn_util.util.join_remote_paths([self.path_data, path_in_remote])
        assert os.path.isdir(path_local) is expected

    def write_local_file_text(self, path_in_remote, text_content):
        path_file = zyn_util.util.join_remote_paths([self.path_data, path_in_remote])
        assert os.path.exists(path_file)
        data = text_content.encode('utf-8')
        fp = open(path_file, 'wb')
        fp.write(data)
        fp.close()

    def delete_local_file(self, path_in_remote):
        path_local = zyn_util.util.join_remote_paths([self.path_data, path_in_remote])
        os.remove(path_local)

    def create_local_directory(self, path_remote):
        path_local = zyn_util.util.join_remote_paths([self.path_data, path_remote])
        print('Creating directory, path="{}"'.format(path_local))
        os.makedirs(path_local)
        return path_remote

    def create_local_file(self, path_remote, content=None):
        path_local = zyn_util.util.join_remote_paths([self.path_data, path_remote])
        print('Creating file, path="{}"'.format(path_local))
        open(path_local, 'w').close()
        if content is not None:
            self.write_local_file_text(path_remote, content)
        return path_remote

    def validate_dir_state(self, path, is_tracked, exists_locally):
        self.validate_element_is_tracked(path, is_tracked)
        self.validate_local_directory_exists(path, exists_locally)

    def validate_file_state(self, path, is_tracked, exists_locally):
        self.validate_element_is_tracked(path, is_tracked)
        self.validate_local_file_exists(path, exists_locally)


class TestClient(zyn_util.tests.common.TestCommon):
    def _path_clients_data(self):
        return '{}/clients'.format(self._work_dir.name)

    def _restart_server_and_replace_connection(self, client_state):
        connection = self._restart_node_and_connect_and_handle_auth(
            init=True,
            server_workdir='server-workdir-{}'.format(random.random())
        )
        client_state.client.set_connection(connection)

    def _init_client(self, client_id, init_data=True):
        path_clients_data = self._path_clients_data()
        if not os.path.exists(path_clients_data):
            os.mkdir(path_clients_data)

        path_client_workdir = '{}/{}'.format(path_clients_data, client_id)
        path_client_state = path_client_workdir + '/client-state'
        path_client_data = path_client_workdir + '/client-data'

        if init_data:
            os.mkdir(path_client_workdir)
            os.mkdir(path_client_data)
            zyn_util.client.ZynFilesystemClient.init(
                path_client_state,
                path_client_data,
                self._username,
                self._remote_ip,
                self._remote_port,
            )

        state = ClientData(
            client_id,
            path_client_workdir,
            path_client_state,
            path_client_data,
        )
        state.client.connect_and_authenticate(
            self._password,
            zyn_util.tests.common.DEFAULT_TLS_REMOTE_HOSTNAME,
            zyn_util.tests.common.PATH_CERT,
            True,
        )
        return state

    def _restart_client(self, state):
        state.restart_client()
        state.client.connect_and_authenticate(
            self._password,
            zyn_util.tests.common.DEFAULT_TLS_REMOTE_HOSTNAME,
            zyn_util.tests.common.PATH_CERT,
            True,
        )

    def _server_workdir(self, server_workdir_id):
        return 'server-workdir-{}'.format(server_workdir_id)

    def _restart_server(self):
        self._stop_node()
        self._start_node(
            server_workdir=self._server_workdir(self._server_workdir_id),
            init=False,
        )

    def _start_new_server_with_different_work_dir(self):
        self._server_workdir_id += 1
        self._stop_node()
        self._start_node(self._server_workdir(self._server_workdir_id))

    def _start_server_and_client(self, client_id=0):
        self._server_workdir_id = 0
        self._start_node(self._server_workdir(self._server_workdir_id))
        return self._init_client(client_id)

    def _params(self, params):
        params = [p for p in params if p]
        if len(params) > 1:
            return ' '.join(params)
        else:
            return params[0]

    def _fetch(self, state, path='/', additional_params=None):
        params = ['-p', path]
        if additional_params is not None:
            params += additional_params
        state.cli.do_fetch(self._params(params))

    def _remove(self, state, path, additional_params=None):
        params = [path]
        if additional_params is not None:
            params += additional_params
        state.cli.do_remove(self._params(params))

    def _sync(self, state, path='/', additional_params=None):
        params = ['-p', path]
        if additional_params is not None:
            params += additional_params
        state.cli.do_sync(self._params(params))

    def _revision(self, state, path):
        return state.element(path).revision()

    def _local_revision_increased(self, state, path, revision):
        current = self._revision(state, path)
        self.assertTrue(current > revision)
        return current

    def _create_remote_ra_and_fetch(self, state, path, create_parameters=[]):
        state.cli.do_create_file(self._params(create_parameters + ['-ra', path]))
        state.cli.do_fetch('-p ' + path)
        state.validate_element_is_tracked(path)
        return path

    def _create_remote_blob_and_fetch(self, state, path, create_parameters=[]):
        state.cli.do_create_file(self._params(create_parameters + ['-b', path]))
        state.cli.do_fetch('-p ' + path)
        state.validate_element_is_tracked(path)
        return path

    def _create_remote_directory_and_fetch(self, state, path):
        state.cli.do_create_directory(path)
        state.cli.do_fetch('-p ' + path)
        state.validate_element_is_tracked(path)
        return path

    def _create_local_file_and_add_ra(self, state, path, content=None):
        state.create_local_file(path, content)
        state.cli.do_add(self._params(['-ra', path]))
        state.validate_element_is_tracked(path)
        return path

    def _create_local_file_and_add_blob(self, state, path, content=None):
        state.create_local_file(path, content)
        state.cli.do_add(self._params(['-b', path]))
        state.validate_element_is_tracked(path)
        return path

    def _create_local_directory_and_add(self, state, path, add_parameters=None):
        state.create_local_directory(path)
        state.cli.do_add(self._params([add_parameters, path]))
        state.validate_element_is_tracked(path)
        return path

    def _create_remote_ra(self, state, path):
        state.cli.do_create_file(self._params(['-ra', path]))
        return path

    def _create_remote_blob(self, state, path):
        state.cli.do_create_file(self._params(['-b', path]))
        return path

    def _create_remote_directory(self, state, path):
        state.cli.do_create_directory(self._params([path]))
        return path

    def _write_to_stdin(self, content):
        sys.stdin = io.StringIO(content)

    def _list(self, state, params=''):
        stdout = None
        output = None
        try:
            print('')
            stdout = sys.stdout
            stream = io.StringIO()
            sys.stdout = stream
            state.cli.do_list(params)
        finally:
            if stdout is not None:
                sys.stdout = stdout
                print('----------Output----------')
                output = stream.getvalue()
                print(output, end='')
                print('----------Output----------')
        return output

    def _validate_list_remote_element(
            self,
            output,
            validated_element_name,
            element_type=None,
            file_type=None,
            revision=None,
            node_id=None,
            local_element=None,
    ):
        lines = output.split('\n')
        element_found = False
        for line in lines[1:]:
            if not line:
                break
            fields = line.split()
            print(line, fields)

            if validated_element_name != fields[5]:
                continue

            element_found = True
            if element_type is not None:
                self.assertEqual(element_type, fields[0])

            if file_type is not None:
                self.assertEqual(file_type, fields[1])

            if node_id is not None:
                self.assertEqual(node_id, fields[2])

            if local_element is not None:
                self.assertEqual(local_element, fields[3])

            if revision is not None:
                self.assertEqual(revision, fields[4])

        self.assertTrue(element_found)

    def _validate_list_untracked_element(
            self,
            output,
            validated_element_name,
    ):
        lines = output.split('\n')
        element_found = False
        untracked_files_found = False
        for line in lines[1:]:
            if not line:
                continue
            fields = line.split()
            if 'Untracked' in fields[0]:
                untracked_files_found = True
                continue

            if not untracked_files_found:
                continue

            if fields[0] == validated_element_name:
                element_found = True

        self.assertTrue(element_found)


class TestClientCd(TestClient):
    def test_client_cd_child_parent(self):
        state = self._start_server_and_client()
        path = self._create_local_directory_and_add(state, '/dir-1')
        state.cli.do_cd(path)
        self.assertEqual(state.cli.get_pwd(), path)
        state.cli.do_cd('../')
        self.assertEqual(state.cli.get_pwd(), '/')

    def test_client_cd_root(self):
        state = self._start_server_and_client()
        path = self._create_local_directory_and_add(state, '/dir-1')
        state.cli.do_cd(path)
        self.assertEqual(state.cli.get_pwd(), path)
        state.cli.do_cd('/')
        self.assertEqual(state.cli.get_pwd(), '/')


class TestClientCreateAndFetch(TestClient):
    def test_client_create_empty_file_file_and_add_random_access(self):
        state = self._start_server_and_client()
        self._create_local_file_and_add_ra(state, '/file')

    def test_client_create_file_and_add_random_access(self):
        state = self._start_server_and_client()
        self._create_local_file_and_add_ra(state, '/file', content='data')

    def test_client_create_empty_file_and_add_blob(self):
        state = self._start_server_and_client()
        self._create_local_file_and_add_blob(state, '/file')

    def test_client_create_file_and_add_blob(self):
        state = self._start_server_and_client()
        self._create_local_file_and_add_blob(state, '/file', content='data')

    def test_client_create_directory_and_add(self):
        state = self._start_server_and_client()
        self._create_local_directory_and_add(state, '/dir')

    def test_client_create_empty_random_access_file_and_fetch(self):
        state = self._start_server_and_client()
        self._create_remote_ra_and_fetch(state, "/file")
        state.validate_local_file_exists('/file')

    def test_client_create_empty_blob_file_and_fetch(self):
        state = self._start_server_and_client()
        self._create_remote_blob_and_fetch(state, "/file")
        state.validate_local_file_exists('/file')

    def test_client_create_empty_remote_directory_and_fetch(self):
        state = self._start_server_and_client()
        self._create_remote_directory_and_fetch(state, "/dir")
        state.validate_local_directory_exists('/dir')

    def test_client_create_remote_directory_with_file_and_fetch(self):
        state = self._start_server_and_client()
        self._create_remote_directory_and_fetch(state, "/dir")
        self._create_remote_ra_and_fetch(state, "/dir/file")
        state.validate_local_directory_exists('/dir')
        state.validate_local_file_exists('/dir/file')

    def test_client_fetch_recursive_multiple_files(self):
        state = self._start_server_and_client()
        self._create_remote_ra(state, '/file-1')
        self._create_remote_directory(state, '/dir')
        self._create_remote_directory(state, '/dir/nested')
        self._create_remote_blob(state, '/dir/file-2')
        self._create_remote_blob(state, '/dir/nested/file-3')
        self._fetch(state, '/')
        state.validate_local_file_exists('/dir/nested/file-3')

    def test_client_fetch_new_files_are_fetch_in_dir(self):
        state = self._start_server_and_client()
        self._create_remote_directory(state, '/dir')
        self._create_remote_ra(state, '/dir/file-1')
        self._fetch(state, '/')
        state.validate_local_file_exists('/dir/file-1')

        self._create_remote_ra(state, '/dir/file-2')
        self._fetch(state, '/')
        state.validate_local_file_exists('/dir/file-2')

    def test_client_fetch_aborts_when_local_file(self):
        state = self._start_server_and_client()
        path = self._create_remote_ra(state, 'file-1')
        state.create_local_file(path)
        with self.assertRaises(zyn_util.client_data.ZynClientException):
            self._fetch(state, path)
        state.validate_file_state(path, is_tracked=False, exists_locally=True)

    def test_client_fetch_overwrite(self):
        state = self._start_server_and_client()
        path = self._create_remote_ra(state, '/file-1')
        state.create_local_file(path)
        self._fetch(state, path, ['--overwrite-local'])
        state.validate_file_state(path, is_tracked=True, exists_locally=True)

    def test_client_fetch_conflict_does_not_prevent_fetching_other_files(self):
        state = self._start_server_and_client()
        path_1 = self._create_remote_ra(state, '/file-1')
        path_2 = self._create_remote_ra(state, '/file-2')
        state.create_local_file(path_1)
        self._fetch(state, '/')
        state.validate_file_state(path_1, is_tracked=False, exists_locally=True)
        state.validate_file_state(path_2, is_tracked=True, exists_locally=True)

    def test_client_validate_fetch_works_mutitple_times_in_succession(self):
        state = self._start_server_and_client()
        self._create_remote_ra(state, '/file-1')
        self._fetch(state, '/')
        self._fetch(state, '/')


class TestClientSync(TestClient):
    def _edit_and_sync_file_validate_revision_increased(
            self,
            state,
            path_file,
            path_sync=None,
            data='data',
    ):
        if path_sync is None:
            path_sync = path_file
        state.write_local_file_text(path_file, data)
        revision = self._revision(state, path_file)
        self._sync(state, path_sync)
        return self._local_revision_increased(state, path_file, revision)

    def test_client_sync_random_access_file_incrases_revision(self):
        state = self._start_server_and_client()
        path = self._create_remote_ra_and_fetch(state, '/file-1')
        self._edit_and_sync_file_validate_revision_increased(state, path)

    def test_client_sync_blob_file_incrases_revision(self):
        state = self._start_server_and_client()
        path = self._create_remote_blob_and_fetch(state, '/file-1')
        self._edit_and_sync_file_validate_revision_increased(state, path)

    def test_client_sync_edit_file_multiple_times(self):
        state = self._start_server_and_client()
        path = self._create_remote_blob_and_fetch(state, '/file-1')
        for i in range(0, 4):
            self._edit_and_sync_file_validate_revision_increased(state, path, data=str(i) * 4)

    def test_client_sync_file_without_edits(self):
        state = self._start_server_and_client()
        path = self._create_remote_blob_and_fetch(state, '/file-1')
        revision = self._edit_and_sync_file_validate_revision_increased(state, path)
        self._sync(state, path)
        self.assertEqual(revision, self._revision(state, path))

    def test_client_sync_file_in_dir(self):
        state = self._start_server_and_client()
        self._create_remote_directory_and_fetch(state, '/dir')
        path = self._create_remote_blob_and_fetch(state, '/dir/file-1')
        self._edit_and_sync_file_validate_revision_increased(state, path)

    def test_client_sync_dir_syncs_file(self):
        state = self._start_server_and_client()
        path_dir = self._create_remote_directory_and_fetch(state, '/dir')
        path_file = self._create_remote_blob_and_fetch(state, '/dir/file-1')
        self._edit_and_sync_file_validate_revision_increased(state, path_file, path_dir)

    def test_client_sync_dir_is_recursive(self):
        state = self._start_server_and_client()
        self._create_remote_directory_and_fetch(state, '/dir-1')
        self._create_remote_directory_and_fetch(state, '/dir-1/dir-2')
        path_file = self._create_remote_blob_and_fetch(state, '/dir-1/dir-2/file-1')
        self._edit_and_sync_file_validate_revision_increased(state, path_file, '/')

    def test_client_sync_multiple_files(self):
        state = self._start_server_and_client()
        file_1 = self._create_remote_blob_and_fetch(state, '/file-1')
        file_2 = self._create_remote_blob_and_fetch(state, '/file-2')
        file_3 = self._create_remote_blob_and_fetch(state, '/file-3')
        r_1 = self._revision(state, file_1)
        r_2 = self._revision(state, file_2)
        r_3 = self._revision(state, file_3)
        state.write_local_file_text(file_1, 'data')
        state.write_local_file_text(file_3, 'data')
        self._sync(state, file_1)
        self._sync(state, file_2)
        self._sync(state, file_3)
        self.assertTrue(self._revision(state, file_1) > r_1)
        self.assertTrue(self._revision(state, file_2) == r_2)
        self.assertTrue(self._revision(state, file_3) > r_3)

    def test_client_sync_remote_file_is_fetched_in_sync(self):
        state_1 = self._start_server_and_client(client_id=0)
        state_2 = self._init_client(client_id=1)
        path = self._create_remote_blob_and_fetch(state_1, '/file-1')
        revision = self._revision(state_1, path)
        self._fetch(state_2, path)
        state_2.write_local_file_text(path, 'data')
        self._sync(state_2, path)
        self._sync(state_1, path)
        self.assertTrue(self._revision(state_1, path) > revision)
        self.assertTrue(self._revision(state_1, path) == self._revision(state_2, path))

    def test_client_sync_both_remote_and_local_with_changes(self):
        state_1 = self._start_server_and_client(client_id=0)
        state_2 = self._init_client(client_id=1)
        path = self._create_remote_blob_and_fetch(state_1, '/file-1')
        self._fetch(state_2, path)
        state_1.write_local_file_text(path, 'data')
        state_2.write_local_file_text(path, 'data')
        self._sync(state_1, path)
        r_1 = self._revision(state_2, path)
        self._sync(state_2, path)
        # Make sure revision was not increased
        self.assertEqual(self._revision(state_2, path), r_1)

    def test_client_sync_removed_remote_file_is_noticed(self):
        state_1 = self._start_server_and_client(client_id=0)
        state_2 = self._init_client(client_id=1)
        self._create_remote_directory_and_fetch(state_1, '/dir')
        self._create_remote_ra_and_fetch(state_1, '/dir/file-1')
        path = self._create_remote_ra_and_fetch(state_1, '/dir/file-2')
        self._list(state_2)
        self._fetch(state_2)
        state_2.validate_file_state(path, is_tracked=True, exists_locally=True)
        self._remove(state_1, path, ['--delete-remote'])
        self._sync(state_2)
        state_2.validate_file_state(path, is_tracked=False, exists_locally=True)

    def test_client_sync_discard_local_change(self):
        state_1 = self._start_server_and_client(client_id=0)
        state_2 = self._init_client(client_id=1)
        path = self._create_remote_ra_and_fetch(state_1, '/file')
        self._fetch(state_2, path)
        state_1.write_local_file_text(path, 'qwerty')
        state_2.write_local_file_text(path, 'asdasd')
        self._sync(state_1, path)
        self._sync(state_2, path, ['--discard-local-changes'])

    def test_client_sync_conflict_does_not_abort_other(self):
        state_1 = self._start_server_and_client(client_id=0)
        state_2 = self._init_client(client_id=1)
        path_1 = self._create_remote_ra_and_fetch(state_1, '/file-1')
        path_2 = self._create_remote_ra_and_fetch(state_1, '/file-2')
        self._fetch(state_2)
        state_1.write_local_file_text(path_1, 'qwerty')
        state_2.write_local_file_text(path_1, 'asdasd')
        state_1.write_local_file_text(path_2, 'asdasd')
        r_1 = self._revision(state_2, path_1)
        r_2 = self._revision(state_2, path_2)
        self._sync(state_1)
        self._sync(state_2)
        self.assertEqual(self._revision(state_2, path_1), r_1)
        self.assertNotEqual(self._revision(state_1, path_1), r_1)
        self.assertNotEqual(self._revision(state_2, path_2), r_2)
        self.assertEqual(self._revision(state_1, path_2), self._revision(state_2, path_2))

    def test_client_validate_sync_works_mutitple_times_in_succession(self):
        state = self._start_server_and_client()
        self._create_remote_ra_and_fetch(state, '/file-1')
        self._sync(state)
        self._sync(state)


class TestClientRemove(TestClient):
    def test_client_remove_tracked_file(self):
        state = self._start_server_and_client()
        path = self._create_remote_blob_and_fetch(state, '/file')
        state.validate_file_state(path, is_tracked=True, exists_locally=True)
        self._remove(state, path)
        state.validate_file_state(path, is_tracked=False, exists_locally=True)

    def test_client_remove_tracked_file_with_local_file(self):
        state = self._start_server_and_client()
        path = self._create_remote_blob_and_fetch(state, '/file')
        state.validate_file_state(path, is_tracked=True, exists_locally=True)
        self._remove(state, path, ['--delete-local'])
        state.validate_file_state(path, is_tracked=False, exists_locally=False)

    def test_client_remove_untracked_file(self):
        state = self._start_server_and_client()
        path = state.create_local_file('/file')
        state.validate_file_state(path, is_tracked=False, exists_locally=True)
        with self.assertRaises(zyn_util.client_data.ZynClientException):
            self._remove(state, path)

    def test_client_remove_directory(self):
        state = self._start_server_and_client()
        path = self._create_remote_directory_and_fetch(state, '/dir')
        state.validate_dir_state(path, is_tracked=True, exists_locally=True)
        self._remove(state, path)
        state.validate_dir_state(path, is_tracked=False, exists_locally=True)

    def test_client_remove_directory_and_local_directory(self):
        state = self._start_server_and_client()
        path = self._create_remote_directory_and_fetch(state, '/dir')
        state.validate_dir_state(path, is_tracked=True, exists_locally=True)
        self._remove(state, path, ['--delete-local'])
        state.validate_dir_state(path, is_tracked=False, exists_locally=False)

    def test_client_remove_directory_and_local_directory_which_has_untacked_file(self):
        state = self._start_server_and_client()
        path = self._create_remote_directory_and_fetch(state, '/dir')
        state.create_local_file('/dir/file')
        with self.assertRaises(zyn_util.client_data.ZynClientException):
            self._remove(state, path, ['--delete-local'])

    def test_client_remove_remote(self):
        state = self._start_server_and_client()
        path = self._create_remote_ra_and_fetch(state, '/file')
        self._remove(state, path, ['--delete-remote'])
        state.validate_file_state(path, is_tracked=False, exists_locally=True)

    def test_client_remove_local_and_remote(self):
        state = self._start_server_and_client()
        path = self._create_remote_ra_and_fetch(state, '/file')
        self._remove(state, path, ['--delete-remote', '--delete-local'])


class TestClientCommon(TestClient):
    def test_client_restart(self):
        state = self._start_server_and_client()
        path_file = self._create_remote_ra_and_fetch(state, '/file')
        path_dir = self._create_remote_directory_and_fetch(state, '/dir')
        node_id_file = state.element(path_file).node_id()
        node_id_dir = state.element(path_dir).node_id()
        self._restart_client(state)
        self.assertEqual(node_id_file, state.element(path_file).node_id())
        self.assertEqual(node_id_dir, state.element(path_dir).node_id())

    def test_client_server_instance_detection(self):
        state = self._start_server_and_client()
        self._restart_server()
        self._restart_client(state)
        self.assertFalse(state.client.server_info().is_connected_to_same_server())
        state.client.server_info().initialize()
        self.assertTrue(state.client.server_info().is_connected_to_same_server())

    def test_client_synchronization(self):
        state = self._start_server_and_client()
        path_1 = self._create_remote_ra_and_fetch(state, '/file')
        path_2 = self._create_remote_directory_and_fetch(state, '/dir')
        path_3 = self._create_remote_ra_and_fetch(state, '/dir/file')

        # Edit file so that revision is increased
        state.write_local_file_text(path_1, 'contennt')
        self._sync(state, path_1)
        state.write_local_file_text(path_1, 'morecontent')
        self._sync(state, path_1)
        revision = self._revision(state, path_1)

        self._start_new_server_with_different_work_dir()
        self._restart_client(state)
        state.client.synchronize_local_files_with_remote()
        state.validate_file_state(path_1, is_tracked=True, exists_locally=True)
        state.validate_dir_state(path_2, is_tracked=True, exists_locally=True)
        state.validate_file_state(path_3, is_tracked=True, exists_locally=True)

        # Revision should be different
        self.assertNotEqual(self._revision(state, path_1), revision)

        output = self._list(state)
        self._validate_list_remote_element(output, 'file', element_type='file')
        self._validate_list_remote_element(output, 'dir/', element_type='dir')
        output = self._list(state, '-p /dir')
        self._validate_list_remote_element(output, 'file', element_type='file')

    def test_client_synchronization_conflicting_file_on_remote(self):
        state_1 = self._start_server_and_client(client_id=0)
        path_1 = self._create_local_file_and_add_ra(state_1, '/file-1', 'data')
        path_2 = self._create_local_file_and_add_ra(state_1, '/file-2', 'data')

        self._start_new_server_with_different_work_dir()
        state_2 = self._init_client(client_id=1)
        self._create_local_file_and_add_ra(state_2, path_2, 'different data')

        self._restart_client(state_1)
        state_1.client.synchronize_local_files_with_remote()
        state_1.validate_file_state(path_1, is_tracked=True, exists_locally=True)
        state_1.validate_file_state(path_2, is_tracked=False, exists_locally=True)

    def test_client_synchronization_directory(self):
        state = self._start_server_and_client()
        path_dir = self._create_remote_directory_and_fetch(state, '/dir')
        path_file = self._create_remote_ra_and_fetch(state, '/dir/file')

        self._start_new_server_with_different_work_dir()
        self._restart_client(state)
        state.client.synchronize_local_files_with_remote()
        state.validate_dir_state(path_dir, is_tracked=True, exists_locally=True)
        state.validate_file_state(path_file, is_tracked=True, exists_locally=True)


class TestClientList(TestClient):
    def test_client_list_remote_files(self):
        state = self._start_server_and_client()
        self._create_remote_ra(state, '/file')
        self._create_remote_directory(state, '/dir')
        output = self._list(state, '')
        self._validate_list_remote_element(output, 'file', element_type='file')
        self._validate_list_remote_element(output, 'dir/', element_type='dir')

    def test_client_list_on_file_path(self):
        state = self._start_server_and_client()
        path = self._create_remote_ra(state, '/file')
        with self.assertRaises(zyn_util.exception.ZynServerException):
            self._list(state, '-p {}'.format(path))

    def test_client_list_fetched_files(self):
        state = self._start_server_and_client()
        self._create_local_file_and_add_ra(state, '/file')
        self._create_local_directory_and_add(state, '/dir')
        output = self._list(state, '')
        self._validate_list_remote_element(output, 'file', element_type='file')
        self._validate_list_remote_element(output, 'dir/', element_type='dir')

    def test_client_list_untracked_files(self):
        state = self._start_server_and_client()
        self._create_local_file_and_add_ra(state, '/file')
        state.create_local_file('untracked-file')
        state.create_local_directory('untracked-dir')
        output = self._list(state, '')
        self._validate_list_untracked_element(output, 'untracked-file')
        self._validate_list_untracked_element(output, 'untracked-dir/')

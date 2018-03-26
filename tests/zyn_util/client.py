import difflib
import json
import logging
import os.path
import hashlib

import zyn_util.errors


class ZynException(Exception):
    pass


class ZynClientException(ZynException):
    def __init__(self, description):
        super(ZynClientException, self).__init__(description)


class ZynServerException(ZynException):
    def __init__(self, error_code, description):
        super(ZynServerException, self).__init__(description)
        self.zyn_error_code = error_code


class LocalFile:
    def __init__(self, revision, file_type, node_id, path_in_remote, path_local, checksum=None):
        self._revision = revision
        self._file_type = file_type
        self._node_id = node_id
        self._path_local = path_local
        self._path_in_remote = path_in_remote
        self._checksum = checksum

    def to_json(self):
        return {
            'data-format': 1,
            'revision': self._revision,
            'file-type': self._file_type,
            'node-id': self._node_id,
            'path-local': self._path_local,
            'path-in-remote': self._path_in_remote,
            'checksum': self._checksum,
        }

    def from_json(data):
        if data['data-format'] != 1:
            raise ZynClientException("Trying to import from unsupported version")
        return LocalFile(
            data['revision'],
            data['file-type'],
            data['node-id'],
            data['path-in-remote'],
            data['path-local'],
            data['checksum'],
        )

    def calculate_cheksum(self):
        return hashlib.md5(open(self._path_local, 'rb').read()).hexdigest()

    def update_checksum(self):
        self._checksum = self.calculate_cheksum()

    def node_id(self):
        return self._node_id

    def revision(self):
        return self._revision

    def has_changes(self):
        if self._checksum is None:
            raise ZynClientException('File checksum not set, path="{}"'.format(self._path_local))
        return self._checksum != self.calculate_cheksum()

    def update_revision(self, revision):
        if self._revision >= revision:
            raise NotImplementedError()
        self._revision = revision


class ZynFilesystemClient:

    def init_state_file(path_file, path_data_directory):
        if os.path.exists(path_file):
            raise RuntimeError('Client state file already exists, path={}'.format(path_file))

        with open(path_file, 'w') as fp:
            json.dump({
                'path-data-directory': path_data_directory,
                'local-files': {},
            }, fp)

    def __init__(
            self,
            connection,
            path_state,
    ):
        self._log = logging.getLogger(__name__)
        self._local_files = {}
        self._connection = connection
        self._path_state = path_state
        self._path_data = None

        if not os.path.exists(path_state):
            raise RuntimeError()
        self._load()

    def _load(self):
        self._log.info('Loading client state, path="{}"'.format(self._path_state))
        with open(self._path_state, 'r') as fp:
            content = json.load(fp)
            self._path_data = content['path-data-directory']
            for key, desc in content['local-files'].items():
                self._local_files[key] = LocalFile.from_json(desc)

    def store(self):
        self._log.info('Storing client state, path="{}", number_of_files={}'.format(
            self._path_state, len(self._local_files)))

        with open(self._path_state, 'w') as fp:
            local_files = {
                key: desc.to_json()
                for key, desc in self._local_files.items()
            }

            json.dump({
                'path-data-directory': self._path_data,
                'local-files': local_files,
            }, fp)

    def _check_rsp(self, rsp):
        if rsp.is_error():
            desc = zyn_util.errors.error_to_string(rsp.error_code())
            raise ZynServerException(rsp.error_code(), desc)

    def _split_path(self, path):  # todo: rename?
        path_1, path_2 = os.path.split(path)
        if not path_1 or not path_2:
            raise ZynClientException('Path could not be split, path="{}"'.format(path))
        return path_1, path_2

    def _path_in_local_filesystem(self, path_in_remote):
        return '{}/{}'.format(self._path_data, path_in_remote)

    def create_random_access_file(self, path_in_remote):
        dirname, filename = self._split_path(path_in_remote)

        self._log.debug('Creating random access file: parent="{}", file="{}"'.format(
            dirname, filename))

        rsp = self._connection.create_file_random_access(filename, parent_path=dirname)
        self._check_rsp(rsp)
        return rsp.as_create_rsp()

    def create_folder(self, path_in_remote):
        dirname, folder_name = self._split_path(path_in_remote)

        self._log.debug('Creating folder: parent="{}", folder_name="{}"'.format(
            dirname, folder_name))

        rsp = self._connection.create_folder(folder_name, parent_path=dirname)
        self._check_rsp(rsp)
        return rsp.as_create_rsp()

    def _read_random_access_file_to_local_file(self, path_local, node_id=None, path_in_remote=None):
        try:
            open_rsp = self._connection.open_file_read(node_id=node_id, path=path_in_remote)
            self._check_rsp(open_rsp)
            open_rsp = open_rsp.as_open_rsp()

            data = bytearray()
            if open_rsp.size > 0:  # todo: check file size and handle large files
                rsp, data = self._connection.read_file(open_rsp.node_id, 0, open_rsp.size)
                self._check_rsp(rsp)

            with open(path_local, 'wb') as fp:
                fp.write(data)

        finally:
            close_rsp = self._connection.close_file(open_rsp.node_id)
            self._check_rsp(close_rsp)

        return open_rsp, close_rsp

    def _sync_local_random_access_changes_to_remote(self, path_local, file, open_rsp):

        remote_data = bytearray()
        if open_rsp.size > 0:
            rsp, remote_data = self._connection.read_file(open_rsp.node_id, 0, open_rsp.size)
            self._check_rsp(rsp)
        local_data = open(path_local, 'rb').read()

        revision = open_rsp.revision
        differ = difflib.SequenceMatcher(None, remote_data, local_data)
        for type_of_change, i1, i2, j1, j2 in differ.get_opcodes():

            self._log.debug('type="{}", (i1={}, i2={}) "{}" - (j1={}, j2={}) "{}"'.format(
                type_of_change, i1, i2, remote_data[i1:i2], j1, j2, local_data[j1:j2]))

            if type_of_change == 'equal':
                pass
            elif type_of_change == 'delete':
                delete_size = i2 - i1
                rsp = self._connection.ra_delete(file.node_id(), revision, i1, delete_size)
                self._check_rsp(rsp)
                revision = rsp.as_delete_rsp().revision
            elif type_of_change == 'replace':
                delete_size = i2 - i1
                if delete_size > 0:
                    rsp = self._connection.ra_delete(file.node_id(), revision, i1, delete_size)
                    self._check_rsp(rsp)
                    revision = rsp.as_delete_rsp().revision
                rsp = self._connection.ra_insert(file.node_id(), revision, i1, local_data[j1:j2])
                self._check_rsp(rsp)
                revision = rsp.as_write_rsp()
            elif type_of_change == 'insert':
                rsp = self._connection.ra_insert(file.node_id(), revision, i1, local_data[j1:j2])
                self._check_rsp(rsp)
                revision = rsp.as_insert_rsp().revision
            else:
                raise ZynClientException('Unhandled change type, type="{}"'.format(type_of_change))

        return revision

    def fetch(self, path_in_remote):
        path_local = self._path_in_local_filesystem(path_in_remote)

        if os.path.exists(path_local):
            raise ZynClientException('Local file already exists, path="{}"'.format(path_in_remote))

        path_local_folder, filename = self._split_path(path_local)
        if not os.path.exists(path_local_folder):
            os.makedirs(path_local_folder)

        self._log.debug('Fetching: path_in_remote={}, path_local={}'.format(
            path_in_remote,
            path_local
        ))

        open_rsp, _ = self._read_random_access_file_to_local_file(
            path_local,
            path_in_remote=path_in_remote)

        file = LocalFile(
            open_rsp.revision,
            open_rsp.type_of_element,
            open_rsp.node_id,
            path_in_remote,
            path_local,
        )
        file.update_checksum()
        self._local_files[path_local] = file
        assert os.path.exists(path_local)

    def sync(self, path_local):
        if not os.path.exists(path_local):
            raise ZynClientException('Local file does not exist, path_local="{}"'.format(
                path_local
            ))

        try:
            file = self._local_files[path_local]
        except KeyError:
            raise ZynClientException('Local file does not exist, path_local="{}"'.format(
                path_local
            ))

        local_file_changed = file.has_changes()

        self._log.debug('Synchronizing: path_local={}, local_file_changed={}'.format(
            path_local,
            local_file_changed
        ))

        try:
            open_rsp = None
            open_rsp = self._connection.open_file_write(node_id=file.node_id())
            self._check_rsp(open_rsp)
            open_rsp = open_rsp.as_open_rsp()

            if open_rsp.revision <= file.revision() and local_file_changed:
                if open_rsp.type_of_element == zyn_util.connection.FILE_TYPE_RANDOM_ACCESS:
                    revision = self._sync_local_random_access_changes_to_remote(
                        path_local,
                        file,
                        open_rsp
                    )
                    file.update_revision(revision)
                else:
                    raise NotImplementedError()
            elif open_rsp.revision > file.revision() and not local_file_changed:
                open_rsp, _ = self._read_random_access_file_to_local_file(
                    path_local,
                    node_id=file.node_id()
                )
                file.update_revision(open_rsp.revision)
                file.update_checksum()
            elif open_rsp.revision > file.revision() and local_file_changed:
                raise NotImplementedError()
            else:
                raise NotImplementedError()

        finally:
            if open_rsp is not None:
                self._connection.close_file(open_rsp.node_id)


'''
    def query_list(self, path_in_remote):
        rsp = self._connection.query_list(path=path_in_remote)
        self._check_error(rsp)
        return rsp.as_query_list_rsp()

    def query_filesystem(self, path_in_remote):
        rsp = self._connection.query_filesystem(path=path_in_remote)
        self._check_error(rsp)
        return rsp.as_query_filesystem_rsp()

    def _convert_to_local_paths(self, remote_path):
        if remote_path.endswith('/'):
            raise ZynClientException('Path must end to file')
        if not remote_path.startswith('/'):
            raise ZynClientException('Path must be absolute')

        path_in_data = self._data_dir + remote_path
        path_in_work = self._work_dir + remote_path
        return path_in_data, path_in_work

    def _check_file_exists(self, local_path, should_exist):
        if os.path.exists(local_path) != should_exist:
            if should_exist:
                raise ZynClientException('Path does not exist: path="{}"'.format(local_path))
            else:
                raise ZynClientException('Path already exist: path="{}"'.format(local_path))

    def _create_containing_folder(self, path_file):
        folder, filename = os.path.split(path_file)
        if not os.path.exists(folder):
            os.makedirs(folder)

    def _check_file_is_known(self, path):
        if path not in self._local_files:
            raise ZynClientException('Unknown file, path="{}"'.format(path))

    def fetch(self, path_in_remote):
        path_in_data, path_in_work = self._convert_to_local_paths(path)

        self._log.debug('Fetching path="{}" to path_in_data="{}" and path_in_work="{}"'.format(
            path, path_in_data, path_in_work))

        self._check_file_exists(path_in_data, False)
        self._check_file_exists(path_in_work, False)

        self._create_containing_folder(path_in_data)
        self._create_containing_folder(path_in_work)

        rsp = self._connection.open_file_read(path=path)
        self._check_error(rsp)
        node_id, revision, size, file_type = rsp.as_open_rsp()

        try:
            data = bytearray()
            if size > 0:
                rsp, data = self._connection.read_file(node_id, 0, size)
                self._check_error(rsp)

            with open(path_in_work, 'wb') as fp:
                fp.write(data)

            if file_type == zyn_util.connection.FILE_TYPE_RANDOM_ACCESS:
                with open(path_in_data, 'wb') as fp:
                    fp.write(data)

            self._local_files[path] = LocalFile(revision, file_type)

        finally:
            self._connection.close_file(node_id)

        return node_id, revision, size, file_type, path_in_data

    def _sync_random_access_file(self, node_id, revision, path_in_data_dir, path_in_work_dir):
        fp_data = open(path_in_data_dir, 'rb')
        fp_work = open(path_in_work_dir, 'rb')

        content_data = fp_data.read()
        content_work = fp_work.read()

        differ = difflib.SequenceMatcher(None, content_work, content_data)
        for type_of_change, i1, i2, j1, j2 in differ.get_opcodes():

            self._log.debug('type="{}", (i1={}, i2={}) "{}" - (j1={}, j2={}) "{}"'.format(
                type_of_change, i1, i2, content_work[i1:i2], j1, j2, content_data[j1:j2]))

            if type_of_change == 'equal':
                pass
            elif type_of_change == 'delete':
                delete_size = i2 - i1
                rsp = self._connection.ra_delete(node_id, revision, i1, delete_size)
                self._check_error(rsp)
                revision = rsp.as_delete_rsp()
            elif type_of_change == 'replace':
                delete_size = i2 - i1
                if delete_size > 0:
                    rsp = self._connection.ra_delete(node_id, revision, i1, delete_size)
                    self._check_error(rsp)
                    revision = rsp.as_delete_rsp()
                rsp = self._connection.ra_insert(node_id, revision, i1, content_data[j1:j2])
                self._check_error(rsp)
                revision = rsp.as_write_rsp()
            elif type_of_change == 'insert':
                rsp = self._connection.ra_insert(node_id, revision, i1, content_data[j1:j2])
                self._check_error(rsp)
                revision = rsp.as_insert_rsp()
            else:
                raise ZynClientException('Unhandled change type, type="{}"'.format(type_of_change))

        return revision

    def sync(self, path):
        path_in_data, path_in_work = self._convert_to_local_paths(path)

        self._log.debug('Synchronizing path="{}" to path_in_data="{}" and path_in_work="{}"'.format(
            path, path_in_data, path_in_work))

        rsp = self._connection.open_file_write(path=path)
        self._check_error(rsp)
        node_id, revision, size, file_type = rsp.as_open_rsp()

        try:
            self._check_file_is_known(path)
            file_desc = self._local_files[path]

            if file_desc.revision != revision:
                # todo: Should use some kind of hash instead reading content
                if open(path_in_data, 'rb').read() != open(path_in_work, 'rb').read():
                    print('Remote has a more recent version of a file that has local modifications')
                    print('Files should be merged before synching')
                    print('This is currently not implemented')
                    print('path="{}", local_revision={}, remote_revision={}'
                          .format(path, file_desc.revision, revision))
                    raise NotImplementedError(
                        'Remote has more recent version of a modified local file, path="{}"'
                        .format(path))

            self._check_file_exists(path_in_data, True)
            if file_type == zyn_util.connection.FILE_TYPE_RANDOM_ACCESS:
                self._check_file_exists(path_in_work, True)
                revision = self._sync_random_access_file(
                    node_id,
                    revision,
                    path_in_data,
                    path_in_work
                )
                open(path_in_work, 'wb').write(open(path_in_data, 'rb').read())
                self._local_files[path].revision = revision
            else:
                self._check_file_exists(path_in_work, False)
                raise NotImplementedError()

        finally:
            self._connection.close_file(node_id)

        return self._local_files[path].revision

    def remove_local_file(self, path):
        path_in_data, path_in_work = self._convert_to_local_paths(path)

        self._log.debug(
            'Removing local file, path="{}", path_in_data="{}", path_in_work="{}"'
            .format(path, path_in_data, path_in_work))

        self._check_file_is_known(path)
        desc = self._local_files[path]

        os.remove(path_in_data)
        if desc.file_type == zyn_util.connection.FILE_TYPE_RANDOM_ACCESS:
            os.remove(path_in_work)

        del self._local_files[path]
'''

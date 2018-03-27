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


def _check_rsp(rsp):
    if rsp.is_error():
        desc = zyn_util.errors.error_to_string(rsp.error_code())
        raise ZynServerException(rsp.error_code(), desc)


def _split_path(path):  # todo: rename?
    path_1, path_2 = os.path.split(path)
    if not path_1 or not path_2:
        raise ZynClientException('Path could not be split, path="{}"'.format(path))
    return path_1, path_2


def _join_paths(path_1, path_2):
    return '{}/{}'.format(path_1, path_2)


class LocalFile:
    def __init__(self, path_in_remote):
        self._path_in_remote = path_in_remote
        self._file_type = None
        self._node_id = None
        self._checksum = None
        self._revision = None

    def is_initialized(self):
        return self._node_id is not None

    def to_json(self):
        return {
            'data-format': 1,
            'revision': self._revision,
            'file-type': self._file_type,
            'node-id': self._node_id,
            'path-in-remote': self._path_in_remote,
            'checksum': self._checksum,
        }

    def from_json(data):
        if data['data-format'] != 1:
            raise ZynClientException("Trying to import from unsupported version, version={}".format(
                data['data-version'],
            ))

        file = LocalFile(data['path-in-remote'])
        file._file_type = data['file-type']
        file._node_id = data['node-id']
        file._checksum = data['checksum']
        file._revision = data['revision']
        return file

    def _calculate_checksum(self, content):
        return hashlib.md5(content).hexdigest()

    def _calculate_cheksum_from_file(self, path_data_root):
        return self._calculate_checksum(open(self.path_to_local_file(path_data_root), 'rb').read())

    def update_checksum(self, path_data_root):
        self._cheksum = self._calculate_cheksum_from_file(path_data_root)

    def path_to_local_file(self, path_data_root):
        return _join_paths(path_data_root, self._path_in_remote)

    def exists_locally(self, path_data_root):
        return os.path.exists(self.path_to_local_file(path_data_root))

    def has_changes(self, path_data_root):
        return self._checksum != self._calculate_cheksum_from_file(path_data_root)

    def _download_full_file(self, connection, path_local, node_id=None, path_in_remote=None):

        open_rsp = None
        try:
            open_rsp = connection.open_file_read(node_id=node_id, path=path_in_remote)
            _check_rsp(open_rsp)
            open_rsp = open_rsp.as_open_rsp()

            data = bytearray()
            if open_rsp.size > 0:  # todo: check file size and handle large files
                rsp, data = connection.read_file(open_rsp.node_id, 0, open_rsp.size)
                _check_rsp(rsp)

            self._checksum = self._calculate_checksum(data)
            with open(path_local, 'wb') as fp:
                fp.write(data)

        finally:
            if open_rsp is not None:
                close_rsp = connection.close_file(open_rsp.node_id)
                _check_rsp(close_rsp)

        self._revision = open_rsp.revision
        return open_rsp, close_rsp

    def _sync_local_random_access_changes_to_remote(self, connection, path_local, logger):

        open_rsp = None
        try:
            open_rsp = connection.open_file_write(node_id=self._node_id)
            _check_rsp(open_rsp)
            open_rsp = open_rsp.as_open_rsp()

            remote_data = bytearray()
            if open_rsp.size > 0:
                rsp, remote_data = connection.read_file(open_rsp.node_id, 0, open_rsp.size)
                _check_rsp(rsp)

            local_data = open(path_local, 'rb').read()

            self._revision = open_rsp.revision
            differ = difflib.SequenceMatcher(None, remote_data, local_data)

            for type_of_change, i1, i2, j1, j2 in differ.get_opcodes():

                logger.debug('type="{}", (i1={}, i2={}) "{}" - (j1={}, j2={}) "{}"'.format(
                    type_of_change, i1, i2, remote_data[i1:i2], j1, j2, local_data[j1:j2]))

                if type_of_change == 'equal':
                    pass

                elif type_of_change == 'delete':
                    delete_size = i2 - i1
                    rsp = connection.ra_delete(self._node_id, self._revision, i1, delete_size)
                    _check_rsp(rsp)
                    self._revision = rsp.as_delete_rsp().revision

                elif type_of_change == 'replace':
                    delete_size = i2 - i1
                    if delete_size > 0:
                        rsp = connection.ra_delete(self._node_id, self._revision, i1, delete_size)
                        _check_rsp(rsp)
                        self._revision = rsp.as_delete_rsp().revision
                    rsp = connection.ra_insert(self._node_id, self._revision, i1, local_data[j1:j2])
                    _check_rsp(rsp)
                    self._revision = rsp.as_write_rsp()

                elif type_of_change == 'insert':
                    rsp = connection.ra_insert(self._node_id, self._revision, i1, local_data[j1:j2])
                    _check_rsp(rsp)
                    self._revision = rsp.as_insert_rsp().revision

                else:
                    raise ZynClientException('Unhandled change type, type="{}"'.format(
                        type_of_change
                    ))
        finally:
            if open_rsp is not None:
                close_rsp = connection.close_file(open_rsp.node_id)
                _check_rsp(close_rsp)

    def fetch(self, connection, path_data_root):

        path_local = self.path_to_local_file(path_data_root)
        if os.path.exists(path_local):
            raise ZynClientException('Local file already exists, path_local="{}"'.format(
                path_local
            ))

        open_rsp, _ = self._download_full_file(
            connection,
            path_local,
            path_in_remote=self._path_in_remote
        )

        if not self.is_initialized():
            self._node_id = open_rsp.node_id
            self._file_type = open_rsp.type_of_element

    def sync(self, connection, path_data_root, logger):

        path_local = self.path_to_local_file(path_data_root)
        if not os.path.exists(path_local):
            raise ZynClientException('Local file does not exist, path_in_remote="{}"'.format(
                self._path_in_remote
            ))

        has_changes = self.has_changes(path_data_root)

        # todo: Replace following lines with something simples that just fetches
        # the latest revision
        open_rsp = None
        try:
            open_rsp = connection.open_file_write(node_id=self._node_id)
            _check_rsp(open_rsp)
            open_rsp = open_rsp.as_open_rsp()
        finally:
            if open_rsp is not None:
                close_rsp = connection.close_file(open_rsp.node_id)
                _check_rsp(close_rsp)

        if open_rsp.revision == self._revision and has_changes:
            # If local file is at same level as remote and has changes,
            # push changes to remote
            if self._file_type == zyn_util.connection.FILE_TYPE_RANDOM_ACCESS:
                self._sync_local_random_access_changes_to_remote(connection, path_local, logger)
            else:
                raise NotImplementedError()

        elif open_rsp.revision > self._revision and not has_changes:
            # If remote file is newer and file has no local changes,
            # fetch file from remote
            self._download_full_file(connection, path_local, node_id=self._node_id)

        elif open_rsp.revision > self._revision and has_changes:
            # If remote file is newer and local file has changes,
            # this requires some kind of merge
            raise NotImplementedError()

        else:
            raise NotImplementedError()


class ZynFilesystemClient:

    def init_state_file(path_file, path_data_directory):
        if os.path.exists(path_file):
            raise RuntimeError('Client state file already exists, path={}'.format(path_file))

        with open(path_file, 'w') as fp:
            json.dump({
                'path-data-directory': path_data_directory,
                'local-files': [],
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
            for desc in content['local-files']:
                file = LocalFile.from_json(desc)
                self._local_files[file.path_to_local_file(self._path_data)] = file

    def store(self):
        self._log.info('Storing client state, path="{}", number_of_files={}'.format(
            self._path_state, len(self._local_files)))

        with open(self._path_state, 'w') as fp:
            local_files = [
                desc.to_json()
                for key, desc in self._local_files.items()
            ]

            json.dump({
                'path-data-directory': self._path_data,
                'local-files': local_files,
            }, fp)

    def create_random_access_file(self, path_in_remote):
        dirname, filename = _split_path(path_in_remote)

        self._log.debug('Creating random access file: parent="{}", file="{}"'.format(
            dirname, filename))

        rsp = self._connection.create_file_random_access(filename, parent_path=dirname)
        _check_rsp(rsp)
        return rsp.as_create_rsp()

    def create_folder(self, path_in_remote):
        dirname, folder_name = self._split_path(path_in_remote)

        self._log.debug('Creating folder: parent="{}", folder_name="{}"'.format(
            dirname, folder_name))

        rsp = self._connection.create_folder(folder_name, parent_path=dirname)
        _check_rsp(rsp)
        return rsp.as_create_rsp()

    def fetch(self, path_in_remote):

        file = LocalFile(path_in_remote)
        if file.exists_locally(self._path_data):
            raise ZynClientException('Local file already exists, path="{}"'.format(path_in_remote))

        self._log.debug('Fetching file: path_in_remote={}, path_data={}'.format(
            path_in_remote,
            self._path_data
        ))

        file.fetch(self._connection, self._path_data)
        self._local_files[file.path_to_local_file(self._path_data)] = file

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

        local_file_changed = file.has_changes(self._path_data)
        self._log.debug('Synchronizing: path_local={}, local_file_changed={}'.format(
            path_local,
            local_file_changed
        ))

        file.sync(self._connection, self._path_data, self._log)


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

import difflib
import glob
import hashlib
import json
import logging
import posixpath
import os.path

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


def check_rsp(rsp):
    if rsp.is_error():
        desc = zyn_util.errors.error_to_string(rsp.error_code())
        raise ZynServerException(rsp.error_code(), desc)


def _split_path(path):  # todo: rename?
    path_1, path_2 = posixpath.split(path)
    if not path_1 or not path_2:
        raise ZynClientException('Path could not be split, path="{}"'.format(path))
    return path_1, path_2


def _join_paths(path_1, path_2):
    return '{}/{}'.format(path_1, path_2)


def _join_paths_(list_of_paths):
    path = posixpath.normpath('/'.join(list_of_paths))
    if path.startswith('//'):
        path = path[1:]
    return path


class FileState:
    def __init__(self, file, path_data):
        self.revision = file._revision
        self.node_id = file._node_id
        self.revision = file._revision
        self.path_remote = file._path_in_remote
        self.path_local = file.path_to_local_file(path_data)


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

    def path_remote(self):
        return self._path_in_remote

    def update_checksum(self, path_data_root):
        self._cheksum = self._calculate_cheksum_from_file(path_data_root)

    def path_to_local_file(self, path_data_root):
        return _join_paths(path_data_root, self._path_in_remote)

    def exists_locally(self, path_data_root):
        return posixpath.exists(self.path_to_local_file(path_data_root))

    def has_changes(self, path_data_root):
        return self._checksum != self._calculate_cheksum_from_file(path_data_root)

    def _download_full_file(self, connection, path_local, node_id=None, path_in_remote=None):

        open_rsp = None
        try:
            open_rsp = connection.open_file_read(node_id=node_id, path=path_in_remote)
            check_rsp(open_rsp)
            open_rsp = open_rsp.as_open_rsp()

            data = bytearray()
            if open_rsp.size > 0:  # todo: check file size and handle large files
                rsp, data = connection.read_file(open_rsp.node_id, 0, open_rsp.size)
                check_rsp(rsp)

            self._checksum = self._calculate_checksum(data)
            with open(path_local, 'wb') as fp:
                fp.write(data)

        finally:
            if open_rsp is not None:
                close_rsp = connection.close_file(open_rsp.node_id)
                check_rsp(close_rsp)

        self._revision = open_rsp.revision
        return open_rsp, close_rsp

    def _sync_local_random_access_changes_to_remote(self, connection, path_local, logger):

        open_rsp = None
        try:
            open_rsp = connection.open_file_write(node_id=self._node_id)
            check_rsp(open_rsp)
            open_rsp = open_rsp.as_open_rsp()

            remote_data = bytearray()
            if open_rsp.size > 0:
                rsp, remote_data = connection.read_file(open_rsp.node_id, 0, open_rsp.size)
                check_rsp(rsp)

            local_data = open(path_local, 'rb').read()

            self._revision = open_rsp.revision
            differ = difflib.SequenceMatcher(None, remote_data, local_data)
            remote_index_offset = 0

            for type_of_change, i1, i2, j1, j2 in differ.get_opcodes():

                logger.debug(
                    ('type="{}", remote_index_offset={}, ' +
                     '(i1={}, i2={}) "{}" - (j1={}, j2={}) "{}"').format(
                         type_of_change,
                         remote_index_offset,
                         i1,
                         i2,
                         remote_data[i1:i2],
                         j1,
                         j2,
                         local_data[j1:j2]
                    ))

                if type_of_change == 'equal':
                    pass

                elif type_of_change == 'delete':
                    delete_size = i2 - i1
                    remote_index = i1 + remote_index_offset
                    remote_index_offset -= delete_size
                    rsp = connection.ra_delete(
                        self._node_id,
                        self._revision,
                        remote_index,
                        delete_size
                    )
                    check_rsp(rsp)
                    self._revision = rsp.as_delete_rsp().revision

                elif type_of_change == 'replace':
                    delete_size = i2 - i1
                    remote_index = i1 + remote_index_offset
                    if delete_size > 0:
                        rsp = connection.ra_delete(
                            self._node_id,
                            self._revision,
                            remote_index,
                            delete_size
                        )
                        check_rsp(rsp)
                        self._revision = rsp.as_delete_rsp().revision

                    insert_size = j2 - j1
                    remote_index_offset += insert_size - delete_size
                    rsp = connection.ra_insert(
                        self._node_id,
                        self._revision,
                        remote_index,
                        local_data[j1:j2]
                    )
                    check_rsp(rsp)
                    self._revision = rsp.as_write_rsp().revision

                elif type_of_change == 'insert':
                    remote_index = i1 + remote_index_offset
                    insert_size = j2 - j1
                    remote_index_offset += insert_size
                    rsp = connection.ra_insert(
                        self._node_id,
                        self._revision,
                        remote_index,
                        local_data[j1:j2]
                    )
                    check_rsp(rsp)
                    self._revision = rsp.as_insert_rsp().revision
                    insert_size = j2 - j1

                else:
                    raise ZynClientException('Unhandled change type, type="{}"'.format(
                        type_of_change
                    ))
        finally:
            if open_rsp is not None:
                close_rsp = connection.close_file(open_rsp.node_id)
                check_rsp(close_rsp)

    def push(self, connection, path_data_root):

        path_local = self.path_to_local_file(path_data_root)
        try:
            open_rsp = connection.open_file_write(path=self._path_in_remote)
            check_rsp(open_rsp)
            open_rsp = open_rsp.as_open_rsp()
            if not self.is_initialized():
                self._node_id = open_rsp.node_id
                self._file_type = open_rsp.type_of_element
                self._revision = open_rsp.revision

            if open_rsp.size > 0:
                raise ZynClientException('Remote file was not empty')

            local_data = open(path_local, 'rb').read()
            if len(local_data) > 0:
                rsp = connection.ra_insert(self._node_id, self._revision, 0, local_data)
                check_rsp(rsp)
                self._revision = rsp.as_insert_rsp().revision

        finally:
            if open_rsp is not None:
                close_rsp = connection.close_file(open_rsp.node_id)
                check_rsp(close_rsp)

    def fetch(self, connection, path_data_root):

        path_local = self.path_to_local_file(path_data_root)
        if posixpath.exists(path_local):
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
        if not posixpath.exists(path_local):
            raise ZynClientException('Local file does not exist, path_in_remote="{}"'.format(
                self._path_in_remote
            ))

        has_changes = self.has_changes(path_data_root)

        # todo: Replace following lines with something simples that just fetches
        # the latest revision
        open_rsp = None
        try:
            open_rsp = connection.open_file_write(node_id=self._node_id)
            check_rsp(open_rsp)
            open_rsp = open_rsp.as_open_rsp()
        finally:
            if open_rsp is not None:
                close_rsp = connection.close_file(open_rsp.node_id)
                check_rsp(close_rsp)

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


class ServerInfo:
    def __init__(self):
        self.server_id = None
        self.server_started_at = None

    def is_initialized(self):
        return not (
            self.server_id is None
            or self.server_started_at is None
        )

    def set(self, server_id, server_started_at):
        self.server_id = server_id
        self.server_started_at = server_started_at

    def is_same_server(self, server_id, server_started_at):
        return \
            self.server_id == server_id \
            and self.server_started_at == server_started_at


class ZynFilesystemClient:
    def init_state_file(path_file, path_data_directory):
        if posixpath.exists(path_file):
            raise RuntimeError('Client state file already exists, path={}'.format(path_file))

        with open(path_file, 'w') as fp:
            json.dump({
                'server-started': None,
                'server-id': None,
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
        self._server_info = ServerInfo()

        if not posixpath.exists(path_state):
            raise RuntimeError()
        self._load()

    def connection(self):
        return self._connection

    def set_connection(self, connection):
        self._connection = connection

    def is_server_info_initialized(self):
        return self._server_info.is_initialized()

    def initialize_server_info(self):
        rsp = self._connection.query_system()
        check_rsp(rsp)
        rsp = rsp.as_query_system_rsp()
        self._server_info.set(rsp.server_id, rsp.started_at)

    def is_connected_to_same_server(self):
        rsp = self._connection.query_system()
        check_rsp(rsp)
        rsp = rsp.as_query_system_rsp()
        return self._server_info.is_same_server(rsp.server_id, rsp.started_at)

    def server_info(self):
        return self._server_info

    def file(self, path_in_remote):
        return FileState(self._local_files[path_in_remote], self._path_data)

    def _load(self):
        self._log.info('Loading client state, path="{}"'.format(self._path_state))
        with open(self._path_state, 'r') as fp:
            content = json.load(fp)

            self._path_data = content['path-data-directory']
            self._server_info.set(
                content['server-id'],
                content['server-started'],
            )

            for desc in content['local-files']:
                file = LocalFile.from_json(desc)
                self._local_files[file.path_remote()] = file

    def store(self):
        self._log.info('Storing client state, path="{}", number_of_files={}'.format(
            self._path_state, len(self._local_files)))

        with open(self._path_state, 'w') as fp:
            local_files = [
                desc.to_json()
                for key, desc in self._local_files.items()
            ]

            json.dump({
                'server-started': self._server_info.server_started_at,
                'server-id': self._server_info.server_id,
                'path-data-directory': self._path_data,
                'local-files': local_files,
            }, fp)

    def create_random_access_file(self, path_in_remote):
        dirname, filename = _split_path(path_in_remote)

        self._log.debug('Creating random access file: parent="{}", file="{}"'.format(
            dirname, filename))

        rsp = self._connection.create_file_random_access(filename, parent_path=dirname)
        check_rsp(rsp)
        return rsp.as_create_rsp()

    def create_folder(self, path_in_remote):
        dirname, folder_name = _split_path(path_in_remote)

        self._log.debug('Creating folder: parent="{}", folder_name="{}"'.format(
            dirname, folder_name))

        rsp = self._connection.create_folder(folder_name, parent_path=dirname)
        check_rsp(rsp)
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
        self._local_files[path_in_remote] = file

    def sync(self, path_in_remote):

        try:
            file = self._local_files[path_in_remote]
            path_local = file.path_to_local_file(self._path_data)
        except KeyError:
            raise ZynClientException('Local file does not exist, path_in_remote="{}"'.format(
                path_in_remote
            ))

        if not os.path.exists(path_local):
            raise ZynClientException('Local file does not exist, path_local="{}"'.format(
                path_local
            ))

        local_file_changed = file.has_changes(self._path_data)
        self._log.debug('Synchronizing: path_local={}, local_file_changed={}'.format(
            path_local,
            local_file_changed
        ))

        file.sync(self._connection, self._path_data, self._log)

    def list(self, path_parent):

        # todo: handle case where node id is used

        rsp = self._connection.query_list(path=path_parent)
        check_rsp(rsp)
        rsp = rsp.as_query_list_rsp()

        local_files = [
            os.path.basename(p)
            for p in glob.glob(_join_paths_([self._path_data, path_parent, '*']))
        ]

        self._log.debug('Client has {} known local file'.format(len(self._local_files)))
        self._log.debug('Found local files: {}'.format(local_files))

        class Localfile:
            def __init__(self, exists, tracked):
                self.tracked = tracked
                self.exists_locally = exists

        class Element:
            def __init__(self, remote_file, local_file):
                self.remote_file = remote_file
                self.local_file = local_file

        elements = []
        for element in rsp.elements:
            exists_locally = False
            tracked = False
            file_path = _join_paths_([path_parent, element.name])

            self._log.debug('Processing "{}"'.format(file_path))

            if file_path in self._local_files:

                self._log.debug('"{}" found in local files'.format(file_path))

                file = self._local_files[file_path]
                if file.exists_locally(self._path_data):
                    exists_locally = True
                    tracked = True
                    local_files.remove(element.name)
            else:

                if element.name in local_files:
                    exists_locally = True
                    local_files.remove(element.name)

            elements.append(Element(element, Localfile(exists_locally, tracked)))

        return elements, \
            [
                _join_paths_([path_parent, f])
                for f in local_files
            ]

    def add(self, path_in_remote):

        file = LocalFile(path_in_remote)
        if not file.exists_locally(self._path_data):
            raise ZynClientException('Local file "{}" does not exist'.format(path_in_remote))

        rsp = self._connection.query_list(path=path_in_remote)
        if rsp.error_code() == zyn_util.errors.InvalidPath:
            exists = False
        else:
            exists = True

        if exists:
            raise ZynClientException('File "{}" already exists'.format(path_in_remote))

        self.create_random_access_file(path_in_remote)  # todo: parametritize
        file.push(self._connection, self._path_data)
        self._local_files[path_in_remote] = file

    def add_tracked_files_to_remote(self):
        for path_remote, file in self._local_files.items():
            self.add(path_remote)

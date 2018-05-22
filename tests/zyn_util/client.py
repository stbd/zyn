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


class LocalFileSystemElement:
    def __init__(self, path_in_remote):
        self._path_remote = path_in_remote
        self._node_id = None

    def is_file(self):
        raise NotImplementedError()

    def is_directory(self):
        raise NotImplementedError()

    def to_json(self):
        raise NotImplementedError()

    def from_json(data):
        raise NotImplementedError()

    def path_remote(self):
        return self._path_remote

    def path_to_local_file(self, path_data_root):
        return _join_paths(path_data_root, self._path_remote)

    def exists_locally(self, path_data_root):
        return posixpath.exists(self.path_to_local_file(path_data_root))


class DirectoryState:
    def __init__(self, directory, path_data):
        self.node_id = directory._node_id
        self.path_remote = directory._path_remote
        self.path_local = directory.path_to_local_file(path_data)

    def is_file(self):
        return False

    def is_directory(self):
        return True


class LocalDirectory(LocalFileSystemElement):
    def __init__(self, path_in_remote):
        super().__init__(path_in_remote)

    def is_file(self):
        return False

    def is_directory(self):
        return True

    def to_json(self):
        return {
            'data-format': 1,
            'node-id': self._node_id,
            'path-remote': self._path_remote,
        }

    def from_json(data):
        if data['data-format'] != 1:
            raise ZynClientException(
                "Trying to import LocalDirectory from unsupported version, version={}".format(
                    data['data-version'],
                ))

        dir = LocalDirectory(data['path-remote'])
        dir._node_id = data['node-id']
        return dir

    def from_filesystem_query(path_in_remote, rsp):
        if not rsp.is_directory():
            raise ValueError()
        dir = LocalDirectory(path_in_remote)
        dir._node_id = rsp.node_id
        return dir

    def craete(self, path_data_root):
        os.mkdir(self.path_to_local_file(path_data_root))


class FileState:
    def __init__(self, file, path_data):
        self.revision = file._revision
        self.node_id = file._node_id
        self.path_remote = file._path_remote
        self.path_local = file.path_to_local_file(path_data)

    def is_file(self):
        return True

    def is_directory(self):
        return False


class LocalFile(LocalFileSystemElement):
    def __init__(self, path_in_remote):
        super().__init__(path_in_remote)
        self._node_id = None
        self._checksum = None
        self._file_type = None
        self._revision = 0

    def is_file(self):
        return True

    def is_directory(self):
        return False

    def is_initialized(self):
        return self._node_id is not None

    def file_type(self):
        return self._file_type

    def to_json(self):
        return {
            'data-format': 1,
            'revision': self._revision,
            'file-type': self._file_type,
            'node-id': self._node_id,
            'path-remote': self._path_remote,
            'checksum': self._checksum,
        }

    def from_json(data):
        if data['data-format'] != 1:
            raise ZynClientException(
                "Trying to import LocalFile from unsupported version, version={}".format(
                    data['data-version'],
                ))

        file = LocalFile(data['path-remote'])
        file._file_type = data['file-type']
        file._node_id = data['node-id']
        file._checksum = data['checksum']
        file._revision = data['revision']
        return file

    def from_filesystem_query(path_in_remote, rsp):
        if not rsp.is_file():
            raise ValueError()
        dir = LocalFile(path_in_remote)
        dir._node_id = rsp.node_id
        dir._file_type = rsp.type_of_file
        # dir._revision = rsp.revision todo: add revision to query
        return dir

    def _calculate_checksum(self, content):
        return hashlib.md5(content).hexdigest()

    def _calculate_cheksum_from_file(self, path_data_root):
        return self._calculate_checksum(open(self.path_to_local_file(path_data_root), 'rb').read())

    def update_checksum(self, path_data_root):
        self._cheksum = self._calculate_cheksum_from_file(path_data_root)

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
            open_rsp = connection.open_file_write(path=self._path_remote)
            check_rsp(open_rsp)
            open_rsp = open_rsp.as_open_rsp()

            local_data = open(path_local, 'rb').read()
            if len(local_data) > 0:
                if self._file_type == zyn_util.connection.FILE_TYPE_RANDOM_ACCESS:
                    if open_rsp.size > 0:
                        raise ZynClientException('Remote file was not empty')
                    rsp = connection.ra_insert(self._node_id, self._revision, 0, local_data)
                elif self._file_type == zyn_util.connection.FILE_TYPE_BLOB:
                    # todo: use block size
                    rsp = connection.blob_write(self._node_id, self._revision, local_data)
                else:
                    raise RuntimeError()

                check_rsp(rsp)
                self._revision = rsp.as_insert_rsp().revision
                self._checksum = self._calculate_checksum(local_data)

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
            path_in_remote=self._path_remote
        )

    def sync(self, connection, path_data_root, logger):

        path_local = self.path_to_local_file(path_data_root)
        if not posixpath.exists(path_local):
            raise ZynClientException('Local file does not exist, path_remote="{}"'.format(
                self._path_remote
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
                logger.debug('Synchronizing random access file')
                self._sync_local_random_access_changes_to_remote(connection, path_local, logger)
            elif self._file_type == zyn_util.connection.FILE_TYPE_BLOB:
                logger.debug('Synchronizing blob file')
                self.push(connection, path_data_root)
            else:
                raise RuntimeError()

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
                'local-filesystem-elements': [],
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

    def filesystem_element(self, path_in_remote):
        try:
            element = self._local_files[path_in_remote]
        except KeyError:
            raise ZynClientException('Unknown filesystem element, path="{}"'.format(path_in_remote))

        if element.is_file():
            return FileState(self._local_files[path_in_remote], self._path_data)
        elif element.is_directory():
            return DirectoryState(self._local_files[path_in_remote], self._path_data)
        else:
            raise RuntimeError()

    def _load(self):
        self._log.info('Loading client state, path="{}"'.format(self._path_state))
        with open(self._path_state, 'r') as fp:
            content = json.load(fp)

            self._path_data = content['path-data-directory']
            self._server_info.set(
                content['server-id'],
                content['server-started'],
            )

            for desc in content['local-filesystem-elements']:
                if 'file' in desc:
                    file = LocalFile.from_json(desc['file'])
                    self._local_files[file.path_remote()] = file
                elif 'directory' in desc:
                    dir = LocalDirectory.from_json(desc['directory'])
                    self._local_files[dir.path_remote()] = dir
                else:
                    raise RuntimeError()

        self._log.info('Client state loaded, number_of_filesystem_elements={}, path="{}"'.format(
            len(self._local_files), self._path_state
        ))

    def store(self):
        self._log.info('Storing client state, path="{}", number_of_files={}'.format(
            self._path_state, len(self._local_files)))

        with open(self._path_state, 'w') as fp:
            local_fs_elements = []
            for _, desc in self._local_files.items():
                if desc.is_file():
                    local_fs_elements.append({'file': desc.to_json()})
                elif desc.is_directory():
                    local_fs_elements.append({'directory': desc.to_json()})
                else:
                    raise RuntimeError()

            json.dump({
                'server-started': self._server_info.server_started_at,
                'server-id': self._server_info.server_id,
                'path-data-directory': self._path_data,
                'local-filesystem-elements': local_fs_elements,
            }, fp)

    def create_random_access_file(self, path_in_remote):
        return self.create_file(path_in_remote, zyn_util.connection.FILE_TYPE_RANDOM_ACCESS)

    def create_blob_file(self, path_in_remote):
        return self.create_file(path_in_remote, zyn_util.connection.FILE_TYPE_BLOB)

    def create_file(self, path_in_remote, type_of_file):
        dirname, filename = _split_path(path_in_remote)

        self._log.debug('Creating file: type_of_file={}, parent="{}", file="{}"'.format(
            type_of_file, dirname, filename))

        if type_of_file == zyn_util.connection.FILE_TYPE_RANDOM_ACCESS:
            rsp = self._connection.create_file_random_access(filename, parent_path=dirname)
        elif type_of_file == zyn_util.connection.FILE_TYPE_BLOB:
            rsp = self._connection.create_file_blob(filename, parent_path=dirname)
        check_rsp(rsp)
        return rsp.as_create_rsp()

    def create_directory(self, path_in_remote):
        dirname, dir_name = _split_path(path_in_remote)

        self._log.debug('Creating directory: dirname="{}", dir_name="{}"'.format(
            dirname, dir_name))

        rsp = self._connection.create_folder(dir_name, parent_path=dirname)
        check_rsp(rsp)
        return rsp.as_create_rsp()

    def _query_filesystem(self, path):
        rsp = self._connection.query_filesystem(path=path)
        check_rsp(rsp)
        return rsp.as_query_filesystem_rsp()

    def fetch(self, path_in_remote):

        rsp = self._query_filesystem(path_in_remote)
        if rsp.is_directory():
            dir = LocalDirectory.from_filesystem_query(path_in_remote, rsp)
            if dir.exists_locally(self._path_data):
                raise ZynClientException('Local directory already exists, path="{}"'.format(
                    path_in_remote
                ))
            dir.craete(self._path_data)
            self._local_files[path_in_remote] = dir

        elif rsp.is_file():
            file = LocalFile.from_filesystem_query(path_in_remote, rsp)
            if file.exists_locally(self._path_data):
                raise ZynClientException('Local file already exists, path="{}"'.format(
                    path_in_remote
                ))
            self._log.debug(
                'Fetching file: path_in_remote={}, path_data={}, type_of_file={}'.format(
                    path_in_remote,
                    self._path_data,
                    rsp.type_of_file,
                ))

            file.fetch(self._connection, self._path_data)
            self._local_files[path_in_remote] = file
        else:
            raise RuntimeError()

    def sync(self, path_in_remote):

        try:
            element = self._local_files[path_in_remote]
        except KeyError:
            raise ZynClientException(
                'Local filesystem elements does not exist, path_in_remote="{}"'.format(
                    path_in_remote
                ))

        if not element.is_file():
            raise ZynClientException(
                'Synchronizing directories it not supported, path_local="{}"'.format(
                    path_in_remote
                ))

        path_local = element.path_to_local_file(self._path_data)
        if not os.path.exists(path_local):
            raise ZynClientException('Local file does not exist, path_local="{}"'.format(
                path_local
            ))

        local_file_changed = element.has_changes(self._path_data)
        self._log.debug('Synchronizing: path_local={}, local_file_changed={}'.format(
            path_local,
            local_file_changed
        ))

        element.sync(self._connection, self._path_data, self._log)

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

    def _add(self, element):
        if not element.exists_locally(self._path_data):
            raise ZynClientException('"{}" does not exist'.format(element.path_remote()))

        rsp = self._connection.query_list(path=element.path_remote())
        if rsp.error_code() == zyn_util.errors.InvalidPath:
            exists = False
        else:
            exists = True

        if exists:
            raise ZynClientException(
                'Filesystem element "{}" already exists'.format(element.path_remote())
            )

    def add_directory(self, path_in_remote):
        element = LocalDirectory(path_in_remote)
        self._add(element)
        path_local = element.path_to_local_file(self._path_data)
        if not os.path.isdir(path_local):
            raise ZynClientException('"{}" must be directory'.format(element.path_remote()))

        self.create_directory(path_in_remote)
        rsp = self._query_filesystem(path_in_remote)
        dir = LocalDirectory.from_filesystem_query(path_in_remote, rsp)
        self._local_files[path_in_remote] = dir

    def add_file(self, path_in_remote, type_of_file):

        element = LocalFile(path_in_remote)
        self._add(element)
        path_local = element.path_to_local_file(self._path_data)
        if not os.path.isfile(path_local):
            raise ZynClientException('"{}" must be file'.format(element.path_remote()))

        self.create_file(path_in_remote, type_of_file)
        rsp = self._query_filesystem(path_in_remote)
        file = LocalFile.from_filesystem_query(path_in_remote, rsp)
        file.push(self._connection, self._path_data)
        self._local_files[path_in_remote] = file

    def add_tracked_files_to_remote(self):
        for path_remote, element in self._local_files.items():
            if element.is_directory():
                self.add_directory(path_remote)
            elif element.is_file():
                self.add_file(path_remote, element.file_type())
            else:
                raise RuntimeError()

    def remove_local_files(self):
        self._local_files = {}

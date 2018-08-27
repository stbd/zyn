import glob
import hashlib
import json
import logging
import os.path
import posixpath
import traceback

import zyn_util.errors
import zyn_util.exception
import zyn_util.util


_REMOTE_PATH_ROOT = '/'


class ZynClientException(zyn_util.exception.ZynException):
    def __init__(self, description):
        super(ZynClientException, self).__init__(description)


def _split_path(path):  # todo: rename?
    path_1, path_2 = posixpath.split(path)
    if not path_1 or not path_2:
        raise ZynClientException('Path could not be split, path="{}"'.format(path))
    return path_1, path_2


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
        return zyn_util.util.join_paths([path_data_root, self._path_remote])

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

    def remove_local(self, path_data):
        path = self.path_to_local_file(path_data)
        try:
            os.rmdir(path)
        except OSError:
            print(traceback.format_exc())
            print('Directory not empty, skipping, path="{}"'.format(path))


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

    def remove_local(self, path_data):
        os.remove(self.path_to_local_file(path_data))

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
            zyn_util.util.check_server_response(open_rsp)
            open_rsp = open_rsp.as_open_rsp()

            data = bytearray()
            if open_rsp.size > 0:  # todo: check file size and handle large files
                rsp, data = connection.read_file(open_rsp.node_id, 0, open_rsp.size)
                zyn_util.util.check_server_response(rsp)

            self._checksum = self._calculate_checksum(data)
            with open(path_local, 'wb') as fp:
                fp.write(data)

        finally:
            if open_rsp is not None:
                close_rsp = connection.close_file(open_rsp.node_id)
                zyn_util.util.check_server_response(close_rsp)

        self._revision = open_rsp.revision
        return open_rsp, close_rsp

    def _sync_local_random_access_changes_to_remote(self, connection, path_local, logger):

        open_rsp = None
        try:
            open_rsp = connection.open_file_write(node_id=self._node_id)
            zyn_util.util.check_server_response(open_rsp)
            open_rsp = open_rsp.as_open_rsp()

            remote_data = bytearray()
            if open_rsp.size > 0:
                rsp, remote_data = connection.read_file(open_rsp.node_id, 0, open_rsp.size)
                zyn_util.util.check_server_response(rsp)

            local_data = open(path_local, 'rb').read()

            self._revision = zyn_util.util.edit_random_access_file(
                connection,
                self._node_id,
                self._revision,
                remote_data,
                local_data,
                logger
            )

            self._checksum = self._calculate_checksum(local_data)
        finally:
            if open_rsp is not None:
                close_rsp = connection.close_file(open_rsp.node_id)
                zyn_util.util.check_server_response(close_rsp)

    def push(self, connection, path_data_root):

        path_local = self.path_to_local_file(path_data_root)
        try:
            open_rsp = connection.open_file_write(path=self._path_remote)
            zyn_util.util.check_server_response(open_rsp)
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

                zyn_util.util.check_server_response(rsp)
                self._revision = rsp.as_insert_rsp().revision
                self._checksum = self._calculate_checksum(local_data)

        finally:
            if open_rsp is not None:
                close_rsp = connection.close_file(open_rsp.node_id)
                zyn_util.util.check_server_response(close_rsp)

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

    def sync(self, connection, path_data_root, discard_local_changes, logger):

        synchronized = False
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
            zyn_util.util.check_server_response(open_rsp)
            open_rsp = open_rsp.as_open_rsp()
        finally:
            if open_rsp is not None:
                close_rsp = connection.close_file(open_rsp.node_id)
                zyn_util.util.check_server_response(close_rsp)

        if open_rsp.revision == self._revision and not has_changes:
            pass

        elif open_rsp.revision == self._revision and has_changes:
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

            synchronized = True

        elif open_rsp.revision > self._revision and not has_changes:
            # If remote file is newer and file has no local changes,
            # fetch file from remote
            self._download_full_file(connection, path_local, node_id=self._node_id)
            synchronized = True

        elif open_rsp.revision > self._revision and has_changes:
            # If remote file is newer and local file has changes,
            # unless it is allowed to drop local changes, this will require some kind of merge
            if not discard_local_changes:
                raise NotImplementedError(
                    'Both remote and local file have changes, merge is not implemented'
                )
            self._download_full_file(connection, path_local, node_id=self._node_id)
            synchronized = True

        else:
            raise NotImplementedError()

        return synchronized


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
                'path-data-directory': os.path.normpath(path_data_directory),
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
        zyn_util.util.check_server_response(rsp)
        rsp = rsp.as_query_system_rsp()
        self._server_info.set(rsp.server_id, rsp.started_at)

    def is_connected_to_same_server(self):
        rsp = self._connection.query_system()
        zyn_util.util.check_server_response(rsp)
        rsp = rsp.as_query_system_rsp()
        return self._server_info.is_same_server(rsp.server_id, rsp.started_at)

    def server_info(self):
        return self._server_info

    def path_to_local_file(self, path_remote):
        return zyn_util.util.join_paths([self._path_data, path_remote])

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
        zyn_util.util.check_server_response(rsp)
        return rsp.as_create_rsp()

    def create_directory(self, path_in_remote):
        dirname, dir_name = _split_path(path_in_remote)

        self._log.debug('Creating directory: dirname="{}", dir_name="{}"'.format(
            dirname, dir_name))

        rsp = self._connection.create_folder(dir_name, parent_path=dirname)
        zyn_util.util.check_server_response(rsp)
        return rsp.as_create_rsp()

    def _fetch_file(self, path_remote, fs_query):
        file = LocalFile.from_filesystem_query(path_remote, fs_query)
        if file.exists_locally(self._path_data):
            raise ZynClientException('Local file already exists, path="{}"'.format(
                path_remote
            ))

        print(
            'Fetching file: path_remote={}, path_data={}, type_of_file={}'.format(
                path_remote,
                self._path_data,
                fs_query.type_of_file,
            ))

        file.fetch(self._connection, self._path_data)
        self._local_files[path_remote] = file

    def _fetch_directory(self, path_remote, fs_query):
        dir = LocalDirectory.from_filesystem_query(path_remote, fs_query)
        if dir.exists_locally(self._path_data):
            raise ZynClientException('Local directory already exists, path="{}"'.format(
                path_remote
            ))

        print(
            'Fetching directory: path_remote={}, path_data={}'.format(
                path_remote,
                self._path_data,
            ))

        dir.craete(self._path_data)
        self._local_files[path_remote] = dir

    def _query_element(self, path):
        rsp = self._connection.query_filesystem(path=path)
        zyn_util.util.check_server_response(rsp)
        return rsp.as_query_filesystem_rsp()

    def _query_list(self, path_remote_parent):
        rsp = self._connection.query_list(path=path_remote_parent)
        zyn_util.util.check_server_response(rsp)
        return rsp.as_query_list_rsp()

    def fetch(self, path_in_remote, stop_on_error):

        print("Fetching, path={}".format(path_in_remote))

        elements_fetched = 0
        query = self._query_element(path_in_remote)
        if query.is_file():
            if path_in_remote not in self._local_files:
                self._fetch_file(path_in_remote, query)
                elements_fetched += 1
        elif query.is_directory():
            dirs = [path_in_remote]
            if path_in_remote != _REMOTE_PATH_ROOT and path_in_remote not in self._local_files:
                self._fetch_directory(path_in_remote, query)
                elements_fetched += 1

            while True:
                if not dirs:
                    break
                dir = dirs.pop()
                query_list = self._query_list(dir)
                for element in query_list.elements:
                    path_remote_element = zyn_util.util.join_paths([dir, element.name])

                    print('Processing element "{}"'.format(
                        path_remote_element,
                    ))

                    try:
                        if element.is_file():
                            if path_remote_element in self._local_files:
                                continue

                            rsp = self._query_element(path_remote_element)
                            self._fetch_file(path_remote_element, rsp)
                            elements_fetched += 1
                        elif element.is_directory():
                            dirs.append(path_remote_element)
                            if path_remote_element in self._local_files:
                                continue
                            self._fetch_directory(path_remote_element, element)
                            elements_fetched += 1
                        else:
                            raise RuntimeError()
                    except RuntimeError:
                        raise
                    except Exception:
                        if stop_on_error:
                            raise
                        print('There was an exception while processing element, path="{}"'.format(
                            path_remote_element
                        ))
                        print(traceback.format_exc())
        else:
            raise RuntimeError()
        return elements_fetched

    def sync(self, path_in_remote, stop_on_error, discard_local_changes):

        print("Synchronizing, path={}".format(path_in_remote))

        elements_synchronized = 0
        path_local = zyn_util.util.join_paths([self._path_data, path_in_remote])
        path_local = os.path.normpath(path_local)

        if os.path.isfile(path_local):
            element = self._local_files[path_in_remote]
            if element.sync(self._connection, self._path_data, discard_local_changes, self._log):
                elements_synchronized += 1

        elif os.path.isdir(path_local):
            for root, dirs, files in os.walk(path_local):

                for path_file in files:
                    path_local_file = zyn_util.util.join_paths([root, path_file])
                    path_local_file = os.path.normpath(path_local_file)
                    path_remote_file = path_local_file.replace(self._path_data, '')
                    path_remote_file = zyn_util.util.to_remote_path(path_remote_file)

                    if path_remote_file not in self._local_files:
                        continue

                    element = self._local_files[path_remote_file]
                    try:
                        if element.sync(
                                self._connection,
                                self._path_data,
                                discard_local_changes,
                                self._log
                        ):
                            elements_synchronized += 1

                    except Exception:
                        if stop_on_error:
                            raise
                        print('There was an exception while processing file, path="{}"'.format(
                            path_remote_file
                        ))
                        print(traceback.format_exc())

        else:
            raise RuntimeError('Unknown filesystem element: "{}"'.format(path_in_remote))

        return elements_synchronized

    def list(self, path_parent):

        # todo: handle case where node id is used

        rsp = self._query_list(path_parent)
        local_files = [
            os.path.basename(p)
            for p in glob.glob(zyn_util.util.join_paths([self._path_data, path_parent, '*']))
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
            element_path = zyn_util.util.join_paths([path_parent, element.name])

            self._log.debug('Processing "{}"'.format(element_path))

            if element_path in self._local_files:

                self._log.debug('"{}" found in local elements'.format(element_path))

                local_element = self._local_files[element_path]
                if local_element.exists_locally(self._path_data):
                    exists_locally = True
                    tracked = True
                    local_files.remove(element.name)
            else:

                if element.name in local_files:
                    exists_locally = True
                    local_files.remove(element.name)

            elements.append(Element(element, Localfile(exists_locally, tracked)))

        return elements, [
            zyn_util.util.join_paths([path_parent, f])
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
        rsp = self._query_element(path_in_remote)
        dir = LocalDirectory.from_filesystem_query(path_in_remote, rsp)
        self._local_files[path_in_remote] = dir

    def add_file(self, path_in_remote, type_of_file):

        element = LocalFile(path_in_remote)
        self._add(element)
        path_local = element.path_to_local_file(self._path_data)
        if not os.path.isfile(path_local):
            raise ZynClientException('"{}" must be file'.format(element.path_remote()))

        self.create_file(path_in_remote, type_of_file)
        rsp = self._query_element(path_in_remote)
        file = LocalFile.from_filesystem_query(path_in_remote, rsp)
        file.push(self._connection, self._path_data)
        self._local_files[path_in_remote] = file

    def add_tracked_files_to_remote(self):
        paths = sorted(self._local_files.keys(), key=lambda x: len(x))
        for path_remote in paths:
            element = self._local_files[path_remote]
            if element.is_directory():
                self.add_directory(path_remote)
            elif element.is_file():
                self.add_file(path_remote, element.file_type())
            else:
                raise RuntimeError()

    def find_tracked_elements(self, path_in_remote):

        elements = []
        query_element = self._query_element(path_in_remote)
        if query_element.is_file():
            elements.append((path_in_remote, query_element.node_id))

        elif query_element.is_directory():
            query_list = self._query_list(path_in_remote)
            for element in query_list.elements:
                element_path = zyn_util.util.join_paths([path_in_remote, element.name])
                if element.is_file():
                    elements.append((element_path, element.node_id))

                elif element.is_directory():
                    elements += self.find_tracked_elements(element_path)
                else:
                    raise RuntimeError()
            elements.append((path_in_remote, query_element.node_id))
        else:
            raise RuntimeError()
        return elements

    def remove(self, path_remote, node_id, remove_local_file, remove_remote_file):

        if remove_remote_file:
            self._connection.delete(node_id=node_id)

        element = self._local_files[path_remote]
        if remove_local_file:
            element.remove_local(self._path_data)

        del self._local_files[path_remote]

    def remove_local_files(self):
        self._local_files = {}

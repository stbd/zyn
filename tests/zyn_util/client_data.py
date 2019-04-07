import os
import os.path
import traceback
import logging

import zyn_util.exception
import zyn_util.util
import zyn_util.connection


_REMOTE_PATH_ROOT = '/'


class ZynClientException(zyn_util.exception.ZynException):
    def __init__(self, description):
        super(ZynClientException, self).__init__(description)


class LocalFileSystemElement:
    def __init__(self, path_remote, fs, node_id=None):
        self._path_remote = zyn_util.util.normalized_remote_path(path_remote)
        self._fs = fs
        self._node_id = node_id
        self._node_id_parent = None

    def is_root(self):
        return self._path_remote == _REMOTE_PATH_ROOT

    def set_parent(self, parent):
        self._node_id_parent = parent.node_id()

    def node_id_parent(self):
        return self._node_id_parent

    def node_id(self):
        return self._node_id

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

    def path_local(self):
        return self._fs.local_path(self)

    def parent(self):
        return self._fs.local_element_from_node_id(self._node_id_parent)

    def local_element_exists(self):
        path = self.path_local()
        if self.is_directory():
            return os.path.isdir(path)
        elif self.is_file():
            return os.path.isfile(path)
        zyn_util.util.unhandled()

    def split_to_parent_filename(self):
        return zyn_util.util.split_remote_path(self._path_remote)

    def path_parent(self):
        return self.split_to_parent_filename()[0]

    def name(self):
        if self.is_root():
            return self._path_remote
        return self.split_to_parent_filename()[1]

    def create_on_remote(self):
        raise NotImplementedError()

    def remove_remote(self, connection):
        rsp = connection.delete(self._node_id)
        zyn_util.util.check_server_response(rsp)

    def is_attached_to_local_filesystem(self):
        return self._fs.is_tracked(path_remote=self._path_remote)


class LocalDirectory(LocalFileSystemElement):
    def __init__(self, path_in_remote, fs, node_id=None, node_id_children=None):
        super().__init__(path_in_remote, fs, node_id)
        if node_id_children is None:
            self._node_id_children = []
        else:
            self._node_id_children = node_id_children

    def add_child(self, child):
        if child.node_id() in self._node_id_children:
            raise RuntimeError()
        self._node_id_children.append(child.node_id())

    def remove_child(self, child):
        self._node_id_children.remove(child.node_id())

    def node_id_children(self):
        return self._node_id_children

    def is_file(self):
        return False

    def is_directory(self):
        return True

    def to_json(self):
        return {
            'data-format': 1,
            'node-id': self._node_id,
            'path-remote': self._path_remote,
            'children': self._node_id_children,
        }

    def from_json(data, fs):
        if data['data-format'] != 1:
            raise ZynClientException(
                "Trying to import LocalDirectory from unsupported version, version={}".format(
                    data['data-version'],
                ))

        dir = LocalDirectory(
            data['path-remote'],
            fs,
            node_id=data['node-id'],
            node_id_children=data['children']
        )
        return dir

    def remove_local(self):
        os.rmdir(self.path_local())

    def is_local_empty(self):
        return len(self.local_children()) == 0

    def children_local_untracked(self):
        elements = []
        for c in os.listdir(self.path_local()):
            path_remote = zyn_util.util.join_remote_paths([self._path_remote, c])
            if self._fs.is_tracked(path_remote=path_remote):
                continue

            path_local = zyn_util.util.join_remote_paths([self.path_local(), c])
            if os.path.isfile(path_local):
                elements.append(LocalFile(path_remote, None, self._fs))
            elif os.path.isdir(path_local):
                elements.append(LocalDirectory(path_remote, self._fs))
            else:
                zyn_util.util.unhandled()
        return elements

    def local_children(self):
        return os.listdir(self.path_local())

    def create_empty(path, fs):
        return LocalDirectory(path, fs)

    def create_on_remote(self, parent, connection):
        rsp = connection.create_directory(
            self.name(),
            parent_node_id=parent.node_id(),
        )
        zyn_util.util.check_server_response(rsp)
        rsp = rsp.as_create_rsp()
        self._node_id = rsp.node_id
        self._fs._log.debug('Directory "{}" created to remote with node id: {}'.format(
            self.path_remote(),
            self.node_id(),
        ))
        return rsp

    def fetch(self, connection, overwrite=False):
        rsp = self._fs.query_element(self.path_remote(), connection)
        self._node_id = rsp.node_id
        if os.path.exists(self.path_local()):
            if not overwrite:
                raise ZynClientException('Directory already exists, path: "{}"'.format(
                    self.path_remote(),
                ))
        else:
            os.mkdir(self.path_local())

    def sync(self, connection, child_filter=None, discard_local_changes=False):
        self._fs._log.debug(
            'Synchronizing directory: child_filter: "{}", path="{}"'.format(
                child_filter,
                self._path_remote,
            ))

        synchronized_elements = []
        rsp = self._fs.query_fs_children(self, connection)
        rsp_elements = {}
        for c in rsp.elements:
            rsp_elements[c.node_id] = None
            if not self._fs.is_tracked(node_id=c.node_id):
                continue

            if child_filter is not None and c.name != child_filter:
                continue

            element = self._fs.local_element_from_node_id(c.node_id)
            if c.is_file():
                try:
                    if element.synchronize(connection, c.revision, discard_local_changes):
                        synchronized_elements.append(element)

                except Exception:
                    self._fs._log.error('Failed to sync element "{}"'.format(element.path_remote()))
                    print(traceback.format_exc())
                    continue

            elif c.is_directory():
                synchronized_elements += element.sync(connection, child_filter)
            else:
                zyn_util.util.unhandled()

        for c in self._node_id_children:
            # todo: this needs more implementation when move is added to server
            if c not in rsp_elements:
                element = self._fs.local_element_from_node_id(c)
                self._fs._log.debug('Element "{}" removed on remote'.format(
                    element.name()
                ))
                self._fs.remove(element)

        self._fs._log.debug(
            'Synchronizing done: elements synchronized: {}, path="{}"'.format(
                len(synchronized_elements),
                self._path_remote,
            ))
        return synchronized_elements


class LocalFileMetadata():
    def __init__(self, local_file, edit_timestamp=None, size=None):
        self._edit_timestamp = edit_timestamp
        self._size = size
        self._local_file = local_file

    def has_changed(self):
        stat = os.stat(self._local_file.path_local())
        return (
            self._edit_timestamp != stat.st_mtime
            or self._size != stat.st_size
        )

    def update(self):
        stat = os.stat(self._local_file.path_local())
        self._edit_timestamp = stat.st_mtime
        self._size = stat.st_size

    def to_json(self):
        return {
            'edit-timestamp': self._edit_timestamp,
            'size': self._size,
        }

    def from_json(data, local_file):
        return LocalFileMetadata(
            local_file,
            data['edit-timestamp'],
            data['size'],
        )


class LocalFile(LocalFileSystemElement):
    def __init__(
            self,
            path_in_remote,
            type_of,
            fs,
            node_id=None,
            revision=None,
    ):
        super().__init__(path_in_remote, fs, node_id)
        self._file_type = type_of
        self._revision = revision
        self._local_file_metadata = LocalFileMetadata(self)

    def is_file(self):
        return True

    def is_directory(self):
        return False

    def file_type(self):
        return self._file_type

    def is_random_access(self):
        return self._file_type == zyn_util.connection.FILE_TYPE_RANDOM_ACCESS

    def is_blob(self):
        return self._file_type == zyn_util.connection.FILE_TYPE_BLOB

    def revision(self):
        return self._revision

    def is_empty_local(self):
        return os.stat(self.path_local()).st_size == 0

    def to_json(self):
        return {
            'data-format': 1,
            'revision': self._revision,
            'file-type': self._file_type,
            'node-id': self._node_id,
            'path-remote': self._path_remote,
            'local-file': self._local_file_metadata.to_json(),
        }

    def from_json(data, fs):
        if data['data-format'] != 1:
            raise ZynClientException(
                "Trying to import LocalFile from unsupported version, version={}".format(
                    data['data-version'],
                ))
        local_file = LocalFile(
            data['path-remote'],
            data['file-type'],
            fs,
            data['node-id'],
            data['revision'],
        )
        local_file._local_file_metadata = LocalFileMetadata.from_json(
            data['local-file'],
            local_file
        )
        return local_file

    def create_empty(path, type_of, fs):
        return LocalFile(path, type_of, fs)

    def local_data(self):
        return open(self.path_local(), 'rb').read()

    def create_on_remote(self, parent, connection):
        rsp = connection.create_file(
            self.name(),
            self._file_type,
            parent_node_id=parent.node_id(),
        )
        zyn_util.util.check_server_response(rsp)
        rsp = rsp.as_create_rsp()
        self._node_id = rsp.node_id
        self._revision = 0  # todo: create rsp should contain this
        return rsp

    def remove_local(self):
        os.remove(self.path_local())

    def push_to_remote(self, connection):
        rsp_open = self._fs.open_write(self, connection)
        if rsp_open.revision != self._revision:
            raise ZynClientException(
                'Local revision too old, local={}, remote={}'.format(
                    self._revision,
                    rsp_open.revision,
                ))

        try:
            if self.is_empty_local():
                pass
            else:
                if self.is_blob():
                    stream = zyn_util.connection.FileStream(self.path_local())
                    rsp = connection.blob_write_stream(
                        self.node_id(),
                        self._revision,
                        stream,
                        rsp_open.block_size,
                    )
                elif self.is_random_access():
                    data = self.local_data()
                    if rsp_open.size > len(data):
                        rsp = connection.ra_delete(
                            self.node_id(),
                            self._revision,
                            len(data),
                            rsp_open.size - len(data),
                        )
                        zyn_util.util.check_server_response(rsp)
                        self._revision = rsp.as_delete_rsp().revision

                    rsp = connection.ra_write(
                        self.node_id(),
                        self._revision,
                        0,
                        data,
                    )
                else:
                    zyn_util.util.unhandled()

                zyn_util.util.check_server_response(rsp)
                rsp = rsp.as_write_rsp()
                self._revision = rsp.revision

            self._local_file_metadata.update()

        finally:
            self._fs.close(self, connection)

    def fetch(self, connection, overwrite=False):
        path_local = self.path_local()
        if not overwrite and os.path.exists(path_local):
            raise ZynClientException('Local file already exists, path: "{}"'.format(
                self.path_remote(),
            ))

        open_rsp = self._fs.open_read(self._path_remote, connection)
        try:
            with open(self.path_local(), 'wb') as fp:
                stream = zyn_util.connection.InputFileStream(fp)
                connection.read_file_stream(
                    open_rsp.node_id,
                    0,
                    open_rsp.size,
                    open_rsp.block_size,
                    stream,
                )
                if stream.is_error():
                    zyn_util.util.check_server_response(stream.error_rsp())

            self._node_id = open_rsp.node_id
            self._revision = open_rsp.revision
            self._local_file_metadata.update()
        finally:
            self._fs.close(self, connection)

    def apply_notification(self, connection, notification, byte_buffer):
        if self._local_file_metadata.has_changed():
            raise RuntimeError('Both local file and remote changed, merging changes not supported')

        n = notification
        self._fs._log.debug('Processing notification: type={}, node_id={}'.format(
            n.notification_type(), self.node_id()
        ))

        if n.notification_type() == zyn_util.connection.Notification.TYPE_MODIFIED:
            rsp, new_bytes = connection.read_file(
                self.node_id(),
                n.block_offset,
                n.block_size
            )
            byte_buffer = \
                byte_buffer[0:n.block_offset] \
                + new_bytes \
                + byte_buffer[n.block_offset + n.block_size:]

        elif n.notification_type() == zyn_util.connection.Notification.TYPE_INSERTED:
            rsp, new_bytes = connection.read_file(
                self.node_id(),
                n.block_offset,
                n.block_size
            )
            byte_buffer = \
                byte_buffer[0:n.block_offset] \
                + new_bytes \
                + byte_buffer[n.block_offset:]

        elif n.notification_type() == zyn_util.connection.Notification.TYPE_DELETED:
            byte_buffer = \
                byte_buffer[0:n.block_offset] \
                + byte_buffer[n.block_offset + n.block_size:]

        else:
            raise RuntimeError()

        open(self.path_local(), 'wb').write(byte_buffer)
        self._revision = n.revision
        self._local_file_metadata.update()
        return byte_buffer

    def push_random_access_changes(self, connection, remote_data):
        if not self._local_file_metadata.has_changed():
            return remote_data

        local_data = open(self.path_local(), 'rb').read()
        self._revision = zyn_util.util.edit_random_access_file(
            connection,
            self._node_id,
            self._revision,
            remote_data,
            local_data,
            self._fs._log,
        )
        self._local_file_metadata.update()
        return local_data

    def synchronize(self, connection, remote_revision, discard_local_changes):
        has_changes_remote = remote_revision > self._revision
        has_changes_local = False

        if not discard_local_changes:
            has_changes_local = self._local_file_metadata.has_changed()

        self._fs._log.debug(
            'Synchronizing: revision: local: {}, remote: {}, changed: {}, discard: {}, path="{}"'
            .format(
                self._revision,
                remote_revision,
                has_changes_local,
                str(discard_local_changes),
                self._path_remote,
            ))

        if has_changes_remote and has_changes_local:
            raise NotImplementedError(
                'Both remote and local file have changes, merge is not implemented'
            )

        if not has_changes_remote and not has_changes_local:
            return False

        if has_changes_remote:
            self.fetch(connection, overwrite=True)

        elif has_changes_local:
            rsp_open = None
            try:
                rsp_open = self._fs.open_write(self, connection)
                if self.is_blob():
                    stream = zyn_util.connection.FileStream(self.path_local())
                    rsp = connection.blob_write_stream(
                        self.node_id(),
                        self._revision,
                        stream,
                        rsp_open.block_size,
                    )
                    zyn_util.util.check_server_response(rsp)
                    self._revision = rsp.as_write_rsp().revision
                elif self.is_random_access():
                    remote_data = bytearray()
                    if rsp_open.size > 0:
                        rsp, remote_data = connection.read_file(
                            rsp_open.node_id,
                            0,
                            rsp_open.size
                        )
                        zyn_util.util.check_server_response(rsp)

                    self.push_random_access_changes(connection, remote_data)
                else:
                    zyn_util.util.unhandled()

                self._local_file_metadata.update()
            finally:
                if rsp_open is not None:
                    self._fs.close(self, connection)
        return True


class Element:
    def __init__(self, remote_element, local_element=None):
        self._remote = remote_element
        self._local = local_element

    def root_element(local_root):
        class RemoteRoot():
            def __init__(self):
                self.name = '/'
                self.node_id = 0

            def is_file(self):
                return False

            def is_directory(self):
                return True

        return Element(RemoteRoot(), local_root)

    def is_file(self):
        return self._remote.is_file()

    def is_directory(self):
        return self._remote.is_directory()

    def is_random_access(self):
        return self._remote.is_random_access()

    def is_blob(self):
        return self._remote.is_blob()

    def is_local(self):
        return self._local is not None

    def name(self):
        return self._remote.name

    def node_id(self):
        return self._remote.node_id

    def remote_revision(self):
        return self._remote.revision

    def local_revision(self):
        return self._remote.revision

    def children_local_untracked(self):
        return self._local.children_local_untracked()


class OpenLocalFile():
    def __init__(self, local_file, fs, log,):
        self._local_file = local_file
        self._fs = fs
        self._log = log
        self._bytes = None

    def open_and_sync(self, connection):
        rsp = self._fs.open_write(self._local_file, connection)
        self._local_file.synchronize(connection, rsp.revision, discard_local_changes=False)
        self._bytes = self._local_file.local_data()

    def close(self, connection):
        self._fs.close(self._local_file, connection)

    def handle_notification(self, connection, notification):
        self._bytes = self._local_file.apply_notification(connection, notification, self._bytes)

    def push_local_changes(self, connection):
        self._bytes = self._local_file.push_random_access_changes(
            connection,
            self._bytes,
        )


class LocalFilesystemManager:
    def to_json(self):
        elements = []
        for e in self._elements.values():
            if e.is_file():
                elements.append({
                    'file': e.to_json()
                })
            elif e.is_directory():
                elements.append({
                    'directory': e.to_json()
                })
            else:
                zyn_util.util.unhandled()

        return {
            'local-data-root': self._path_root,
            'elements': elements,
        }

    def from_json(data):
        fs = LocalFilesystemManager(data['local-data-root'])
        for e in data['elements']:
            f = e.get('file', None)
            d = e.get('directory', None)
            element = None
            if f is not None:
                element = LocalFile.from_json(f, fs)
            elif d is not None:
                element = LocalDirectory.from_json(d, fs)
            else:
                zyn_util.util.unhandled()

            fs._elements[element.node_id()] = element
            fs._path_to_node_id[element.path_remote()] = element.node_id()
        return fs

    def __init__(self, path_local_root):
        self._path_root = path_local_root
        self._log = logging.getLogger(__name__)

        rootdir = LocalDirectory.create_empty(_REMOTE_PATH_ROOT, self)
        rootdir._node_id = 0
        self._elements = {
            0: rootdir,
        }
        self._path_to_node_id = {
            _REMOTE_PATH_ROOT: 0,
        }
        self._log.debug('Initialized, root="{}"'.format(self._path_root))

    def local_path(self, element):
        if isinstance(element, str):
            return zyn_util.util.join_remote_paths([self._path_root, element])
        elif isinstance(element, LocalFileSystemElement):
            return zyn_util.util.join_remote_paths([
                self._path_root,
                element.path_remote(),
            ])
        zyn_util.util.unhandled()

    def query_element(self, element, connection):
        if isinstance(element, str):
            rsp = connection.query_fs_element(path=element)
        if isinstance(element, LocalFileSystemElement):
            rsp = connection.query_fs_element(node_id=element.node_id())
        zyn_util.util.check_server_response(rsp)
        return rsp.as_query_fs_element_rsp()

    def query_fs_children(self, element, connection):
        if isinstance(element, str):
            rsp = connection.query_fs_children(path=element)
        elif isinstance(element, LocalFileSystemElement):
            rsp = connection.query_fs_children(node_id=element.node_id())
        zyn_util.util.check_server_response(rsp)
        return rsp.as_query_fs_children_rsp()

    def open_read(self, element, connection):
        if isinstance(element, str):
            rsp = connection.open_file_read(path=element)
        elif isinstance(element, LocalFileSystemElement):
            rsp = connection.open_file_read(node_id=element.node_id())
        zyn_util.util.check_server_response(rsp)
        return rsp.as_open_rsp()

    def open_write(self, element, connection):
        if isinstance(element, str):
            rsp = connection.open_file_write(path=element)
        if isinstance(element, LocalFileSystemElement):
            rsp = connection.open_file_write(node_id=element.node_id())
        zyn_util.util.check_server_response(rsp)
        return rsp.as_open_rsp()

    def close(self, element, connection):
        if isinstance(element, int):
            rsp = connection.close_file(element)
        elif isinstance(element, LocalFileSystemElement):
            rsp = connection.close_file(element.node_id())
        zyn_util.util.check_server_response(rsp)
        return rsp

    def size(self):
        return len(self._path_to_node_id)

    def create_local_file_element(self, path_remote, type_of):
        return LocalFile.create_empty(path_remote, type_of, self)

    def create_local_directory_element(self, path_remote):
        return LocalDirectory.create_empty(path_remote, self)

    def _add_element_to_filesystem(self, element, parent):
        self._log.info(
            'Adding element to local filesystem, element="{}", parent="{}"'.format(
                element.name(),
                parent.name(),
            ))
        element.set_parent(parent)
        parent.add_child(element)
        self._elements[element.node_id()] = element
        self._path_to_node_id[element.path_remote()] = element.node_id()

    def _remove_element_from_filesystem(self, element):
        parent = self.local_element_from_node_id(element.node_id_parent())
        self._log.info(
            'Removing element from local filesystem, element="{}", parent="{}"'.format(
                element.name(),
                parent.name(),
            ))
        parent.remove_child(element)
        del self._elements[element.node_id()]
        del self._path_to_node_id[element.path_remote()]

    def exists_in_filesystem(self, element):
        if isinstance(element, str):
            return element in self._path_to_node_id
        elif isinstance(element, LocalFileSystemElement):
            return element.path_remote() in self._path_to_node_id
        zyn_util.util.unhandled()

    def local_element_from_remote_path(self, path_remote):
        node_id = self._path_to_node_id.get(path_remote, None)
        if node_id is None:
            raise zyn_util.client_data.ZynClientException(
                'Element is not known for client, path="{}"'.format(
                    path_remote
                ))
        return self._elements[node_id]

    def local_element_from_node_id(self, node_id):
        return self._elements[node_id]

    def is_tracked(self, path_remote=None, node_id=None):
        if path_remote is not None:
            if not isinstance(path_remote, str):
                raise ValueError('Path should be str')
            return path_remote in self._path_to_node_id
        elif node_id is not None:
            return node_id in self._elements
        else:
            raise zyn_util.client_data.ZynClientException(
                'Must pass either Node Id or path'
            )

    def element(self, path_remote, connection):
        if path_remote == _REMOTE_PATH_ROOT:
            return Element.root_element(self._elements[0])

        path_parent, name = zyn_util.util.split_remote_path(path_remote)
        children = self.query_fs_children(path_parent, connection)
        element = None

        for c in children.elements:
            if c.name != name and element is None:
                continue

            local = None
            if self.is_tracked(node_id=c.node_id):
                local = self.local_element_from_node_id(c.node_id)
            return Element(c, local)

        if element is None:
            raise zyn_util.client_data.ZynClientException(
                'Element "{}" does not exist on remote'.format(path_remote)
            )
        return element

    def create_local_element_based_on_remote(self, connection, path_remote):
        if path_remote == _REMOTE_PATH_ROOT:
            return self.create_local_directory_element(path_remote)

        path_parent, name = zyn_util.util.split_remote_path(path_remote)
        children = self.query_fs_children(path_parent, connection)
        element = None

        for c in children.elements:
            if c.name != name and element is None:
                continue

            if c.is_file():
                element = self.create_local_file_element(path_remote, c.file_type)
            elif c.is_directory():
                element = self.create_local_directory_element(path_remote)
            else:
                zyn_util.util.unhandled()

        if element is None:
            raise zyn_util.client_data.ZynClientException(
                'Element "{}" does not exist on remote'.format(path_remote)
            )
        return element

    def add(self, element, connection):
        if self.exists_in_filesystem(element):
            raise ZynClientException(
                'Element "{}" already exists in local filesystem'.format(
                    element.path_remote()
                ))

        if not element.local_element_exists():
            raise ZynClientException(
                'Element "{}" does not exist in filesystem'.format(
                    element.path_remote()
                ))

        path_parent = element.path_parent()
        if path_parent not in self._path_to_node_id:
            raise ZynClientException(
                'Parent does not exists in client, parent="{}"'.format(
                    path_parent,
                ))
        parent = self._elements[self._path_to_node_id[path_parent]]
        try:
            element.create_on_remote(parent, connection)
        except zyn_util.exception.ZynServerException as create_error:
            self._log.warn('Creating element on remote failed, trying to cleanup')
            try:
                element.delete_on_remote(connection)
            except zyn_util.exception.ZynServerException as delete_error:
                self._log.warn('Cleanup failed, error="{}"'.format(delete_error))
            raise create_error

        self._add_element_to_filesystem(element, parent)
        if element.is_file():
            element.push_to_remote(connection)
        return element

    def remove(self, element):
        self._remove_element_from_filesystem(element)

    def fetch_element_and_add_to_tracked(self, connection, parent, element, overwrite=False):
        try:
            self._log.debug('Fetching element "{}", overwrite={}'.format(
                element.path_remote(), str(overwrite)
            ))
            element.fetch(connection, overwrite)
            self._add_element_to_filesystem(element, parent)
            return element
        except zyn_util.exception.ZynServerException as fetch_error:
            self._log.warn('Fetch failed, element={}'.format(
                element.name()
            ))
            raise fetch_error

    def fetch_children_and_add_to_tracked(self, connection, parent, overwrite=False):

        fetched_elements = []
        rsp = self.query_fs_children(parent, connection)
        for e in rsp.elements:
            path_remote = zyn_util.util.join_remote_paths([parent.path_remote(), e.name])
            if e.is_file():
                element = LocalFile.create_empty(path_remote, e.file_type, self)
            elif e.is_directory():
                element = LocalDirectory.create_empty(path_remote, self)
            else:
                zyn_util.util.unhandled()
            try:
                element = self.fetch_element_and_add_to_tracked(
                    connection,
                    parent,
                    element,
                    overwrite,
                )
            except ZynClientException:
                self._log.error('Failed to fetch element "{}"'.format(element.path_remote()))
                print(traceback.format_exc())
                continue

            fetched_elements.append(element)
            if element.is_directory():
                fetched_elements += self.fetch_children_and_add_to_tracked(connection, element)
        return fetched_elements

    def _initial_synchronization_for_directory(self, element, elements, connection, parent=None):
        self._log.debug('Initial synchronization for directory "{}"'.format(
            element.path_remote(),
        ))

        rsp = self.query_fs_children(element, connection)
        remote_elements = {}
        for e in rsp.elements:
            remote_elements[e.name] = e

        children = element._node_id_children
        element._node_id_children = []
        for n in children:
            c = elements[n]
            if c.name() not in remote_elements:
                self._log.debug('Element "{}" not found on remote'.format(c.name()))
                c.create_on_remote(element, connection)
                self._add_element_to_filesystem(c, element)
                if c.is_file():
                    c.push_to_remote(connection)
                elif c.is_directory():
                    self._initial_synchronization_for_directory(c, elements, connection, element)
                else:
                    zyn_util.util.unhandled()

    def initial_synchronization(self, connection):
        self._log.debug('Initial synchronization')
        elements = self._elements
        self._elements = {0: elements[0]}
        self._path_to_node_id = {'/': 0}
        self._initial_synchronization_for_directory(elements[0], elements, connection)

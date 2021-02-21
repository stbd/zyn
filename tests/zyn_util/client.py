import json
import logging
import os.path
import time

import zyn_util.errors
import zyn_util.exception
import zyn_util.util
import zyn_util.connection
from zyn_util.client_data import (
    Element,
    OpenLocalFile,
    LocalFilesystemManager,
    ZynClientException,
)


class ServerInfo:
    def __init__(
            self,
            username,
            address,
            port,
            server_id=None,
            server_started_at=None
    ):
        self.username = username
        self.address = address
        self.port = port
        self.server_id = server_id
        self.server_started_at = server_started_at
        self.connection = None

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

    def initialize(self):
        rsp = self.connection.query_system()
        zyn_util.util.check_server_response(rsp)
        rsp = rsp.as_query_system_rsp()
        self.set(rsp.server_id, rsp.started_at)

    def is_connected_to_same_server(self):
        rsp = self.connection.query_system()
        zyn_util.util.check_server_response(rsp)
        rsp = rsp.as_query_system_rsp()
        return self.is_same_server(rsp.server_id, rsp.started_at)

    def create_connection(
            self,
            remote_hostname,
            path_cert,
            debug_protocol,
    ):
        if path_cert is None:
            socket = zyn_util.connection.ZynSocket.create(self.address, self.port)
        else:
            socket = zyn_util.connection.ZynSocket.create_with_custom_cert(
                self.address,
                self.port,
                path_cert,
                remote_hostname
            )

        self.connection = zyn_util.connection.ZynConnection(
            socket,
            debug_protocol,
        )

    def to_json(self):
        return {
            'data-format': 1,
            'username': self.username,
            'address': self.address,
            'port': self.port,
            'server_id': self.server_id,
            'server_started_at': self.server_started_at,
        }

    def from_json(data):
        if data['data-format'] != 1:
            raise ZynClientException(
                "Trying to import ServerInfo from unsupported version, version={}".format(
                    data['data-version'],
                ))

        return ServerInfo(
            data['username'],
            data['address'],
            data['port'],
            data['server_id'],
            data['server_started_at'],
        )


class ZynFilesystemClient:
    def init(
            path_state,
            path_data_directory,
            username,
            address,
            port,
    ):
        fs = LocalFilesystemManager(
            os.path.normpath(path_data_directory),
        )

        server = ServerInfo(
            username,
            address,
            port,
        )

        with open(path_state, 'w') as fp:
            json.dump({
                'server': server.to_json(),
                'local-filesystem': fs.to_json(),
            }, fp)

    def init_from_saved_state(path_state):
        logger = logging.getLogger(__name__)
        server = None
        path_data = None

        logger.info('Loading client state, path="{}"'.format(path_state))
        with open(path_state, 'r') as fp:
            content = json.load(fp)
            server = ServerInfo.from_json(content['server'])
            fs = LocalFilesystemManager.from_json(content['local-filesystem'])

        logger.info('Client loaded, number_of_filesystem_elements={}, path_data="{}"'.format(
            fs.size(), path_data
        ))
        return ZynFilesystemClient(
            server,
            path_data,
            fs,
            logger,
        )

    def store(self, path_state):
        self._log.info('Storing client state, path="{}", fs.size={}'.format(
            path_state, self._fs.size()
        ))

        with open(path_state, 'w') as fp:
            json.dump({
                'server': self._server.to_json(),
                'local-filesystem': self._fs.to_json(),
            }, fp)

    def __init__(
            self,
            server,
            path_data,
            elements,
            logger,
    ):
        self._log = logger
        self._fs = elements
        self._path_data = path_data
        self._server = server

    def connection(self):
        return self._server.connection

    def connect_and_authenticate(
            self,
            password,
            remote_hostname=None,
            path_to_cert=None,
            debug_protocol=False,
    ):
        self._server.create_connection(
            remote_hostname,
            path_to_cert,
            debug_protocol,
        )
        rsp = self._server.connection.authenticate(self._server.username, password)
        zyn_util.util.check_server_response(rsp)

    def server_info(self):
        return self._server

    def is_empty(self):
        return self._fs.is_empty()

    def element(self, node_id=None, path_remote=None):
        if node_id is not None:
            return self._fs.local_element_from_node_id(node_id)
        elif path_remote is not None:
            return self._fs.local_element_from_remote_path(path_remote)
        else:
            zyn_util.util.unhandled()

    def create_directory(self, path_in_remote):
        dirname, dir_name = zyn_util.util.split_remote_path(path_in_remote)

        self._log.debug('Creating directory: dirname="{}", dir_name="{}"'.format(
            dirname, dir_name))
        rsp = self.connection().create_directory(dir_name, parent_path=dirname)
        zyn_util.util.check_server_response(rsp)
        return rsp.as_create_rsp()

    def create_file(self, path_in_remote, type_of_file):
        dirname, filename = zyn_util.util.split_remote_path(path_in_remote)

        self._log.debug(
            'Creating file: type_of_file={}, parent="{}", file="{}"'.format(
                type_of_file, dirname, filename)
        )

        rsp = self.connection().create_file(filename, file_type=type_of_file, parent_path=dirname)
        zyn_util.util.check_server_response(rsp)
        return rsp.as_create_rsp()

    def add(self, path_remote, file_type):
        path_remote = zyn_util.util.normalized_remote_path(path_remote)
        path_local = self._fs.local_path(path_remote)
        element = None

        if os.path.isfile(path_local):
            if file_type is None:
                raise zyn_util.client_data.ZynClientException(
                    'Please specify file type: either random access or blob'.format(
                    ))
            element = self._fs.create_local_file_element(path_remote, file_type)

        elif os.path.isdir(path_local):
            if file_type is not None:
                raise zyn_util.client.ZynClientException(
                    'Must not specify either random access or blob for direcotry'
                )
            element = self._fs.create_local_directory_element(path_remote)

        else:
            raise zyn_util.client.ZynClientException(
                '"{}" does not exist'.format(path_remote)
            )
        return [self._fs.add(element, self.connection())]

    def query_element(self, path_remote):
        path_remote = zyn_util.util.normalized_remote_path(path_remote)
        rsp = self._fs.query_element(path_remote, self.connection())
        local = None
        if self._fs.is_tracked(node_id=rsp.node_id):
            local = self._fs.local_element_from_node_id(rsp.node_id)
        return Element(rsp, local)

    def query_directory(self, path_remote):
        path_remote = zyn_util.util.normalized_remote_path(path_remote)
        element = self._fs.element(path_remote, self.connection())
        children = []

        rsp = self._fs.query_fs_children(path_remote, self.connection())
        for e in rsp.elements:
            local = None
            if self._fs.is_tracked(node_id=e.node_id):
                local = self._fs.local_element_from_node_id(e.node_id)

            children.append(Element(
                e,
                local,
            ))
        return element, children

    def fetch(self, path_remote, overwrite):

        fetched_elements = []
        path_remote = zyn_util.util.normalized_remote_path(path_remote)
        element = self._fs.create_local_element_based_on_remote(
            self.connection(),
            path_remote,
        )

        if element.is_file() and self._fs.is_tracked(element.path_remote()):
            raise ZynClientException(
                'File is already tracked by client, path_parent="{}"'.format(
                    element.path_remote(),
                ))

        if element.is_directory() and self._fs.is_tracked(element.path_remote()):
            element = self._fs.local_element_from_remote_path(element.path_remote())
        else:
            path_parent = element.path_parent()
            if not self._fs.is_tracked(path_remote=path_parent):
                raise ZynClientException(
                    'Parent is not tracked by client, path_parent="{}"'.format(
                        path_parent,
                    ))
            parent = self._fs.local_element_from_remote_path(path_parent)
            element = self._fs.fetch_element_and_add_to_tracked(
                self.connection(),
                parent,
                element,
                overwrite,
            )
            fetched_elements.append(element)

        if element.is_directory():
            fetched_elements += self._fs.fetch_children_and_add_to_tracked(
                self.connection(),
                element,
                overwrite,
            )
        return fetched_elements

    def sync(self, path_remote, discard_local_changes):

        path_remote = zyn_util.util.normalized_remote_path(path_remote)
        if not self._fs.is_tracked(path_remote):
            raise ZynClientException(
                'Element is not tracked by client, path_remote="{}"'.format(
                    path_remote,
                ))
        synchronized_elements = []
        element = self._fs.local_element_from_remote_path(path_remote)
        if element.is_file():
            parent = element.parent()
            synchronized_elements += parent.sync(
                self.connection(),
                child_filter=element.name(),
                discard_local_changes=discard_local_changes,
            )
        elif element.is_directory():
            synchronized_elements += element.sync(
                self.connection(),
                discard_local_changes=discard_local_changes,
            )
        else:
            zyn_util.util.unhandled()
        return synchronized_elements

    def remove(self, path_remote, delete_local, delete_remote):

        path_remote = zyn_util.util.normalized_remote_path(path_remote)
        if not self._fs.is_tracked(path_remote):
            raise ZynClientException(
                'Element is not tracked, path="{}"'.format(
                    path_remote
                ))

        element = self._fs.local_element_from_remote_path(path_remote)
        if element.is_file():
            if delete_local:
                element.remove_local()
            if delete_remote:
                element.remove_remote(self.connection())
            self._fs.remove(element)
        elif element.is_directory():
            if delete_local:
                if not element.is_local_empty():
                    raise ZynClientException(
                        'Local directory is not empty, path="{}"'.format(
                            element.path_remote()
                        ))
                element.remove_local()
            if delete_remote:
                element.remove_remote()
            self._fs.remove(element)
        else:
            zyn_util.util.unhandled()

    def synchronize_local_files_with_remote(self):
        return self._fs.initial_synchronization(self.connection())

    def reset_local_filesystem(self):
        self._fs.reset_data()

    def open(self, path_files, sleep_duration, number_of_iterations=None):

        files = {}
        for p in path_files:
            if not self._fs.is_tracked(p):
                raise ZynClientException(
                    'Element is not tracked, path="{}"'.format(
                        p
                    ))
            e = self._fs.local_element_from_remote_path(p)
            if not e.is_file() or not e.is_random_access():
                raise ZynClientException(
                    'Only random access files can be opened'
                    )
            files[e.node_id()] = OpenLocalFile(e, self._fs, self._log)

        try:
            for f in files.values():
                f.open_and_sync(self.connection())

            iteration = 1
            while True:
                self._log.debug('Synchronizing')

                while True:
                    n = self.connection().pop_notification()
                    if n is None:
                        break
                    if n.notification_type() == zyn_util.connection.Notification.TYPE_DISCONNECTED:
                        print('Connection to Zyn server lost: "{}"'.format(n.reason))
                    elif n.notification_type() in [
                        zyn_util.connection.Notification.TYPE_MODIFIED,
                        zyn_util.connection.Notification.TYPE_DELETED,
                        zyn_util.connection.Notification.TYPE_INSERTED,
                    ]:
                        print('Read notification from remote')
                        if n.node_id in files:
                            files[n.node_id].handle_notification(
                                self.connection(),
                                n,
                            )

                for f in files.values():
                    f.push_local_changes(self.connection())

                if number_of_iterations is not None and iteration >= number_of_iterations:
                    raise KeyboardInterrupt()

                iteration += 1
                time.sleep(sleep_duration)

        except KeyboardInterrupt:
            pass

        finally:
            for f in files.values():
                f.close(self.connection())

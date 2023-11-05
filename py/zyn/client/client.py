import json
import logging
import os.path
import time

import zyn.errors
import zyn.exception
import zyn.util
import zyn.messages
from zyn.client.data import (
    Element,
    OpenLocalFile,
    LocalFilesystemManager,
    ZynClientException,
)


class State:
    def __init__(
            self,
            username,
            address,
            port,
            filesystem,
            server_id=None,
            server_started_at=None,
    ):
        self.username = username
        self.address = address
        self.port = port
        self.fs = filesystem
        self.server_id = server_id
        self.server_started_at = server_started_at

    def to_dict(self):
        return {
            'data-format': 1,
            'username': self.username,
            'address': self.address,
            'port': self.port,
            'server_id': self.server_id,
            'server_started_at': self.server_started_at,
            'filesystem': self.fs.to_dict(),
        }

    def to_file(self, path_state):
        with open(path_state, 'w') as fp:
            json.dump(self.to_dict(), fp)

    def from_dict(data, log):
        if data['data-format'] != 1:
            raise ZynClientException(
                "Trying to import State from unsupported version, version={}".format(
                    data['data-version'],
                ))

        return State(
            username=data['username'],
            address=data['address'],
            port=data['port'],
            filesystem=LocalFilesystemManager.from_dict(data['filesystem'], log),
            server_id=data['server_id'],
            server_started_at=data['server_started_at'],
        )

    def from_file(path_state, log):
        with open(path_state, 'r') as fp:
            content = json.load(fp)
            return State.from_dict(content, log)



class ZynFilesystemClient:
    def __init__(
            self,
            connection,
            state,
            logger,
    ):

        self._connection = connection
        self._state = state
        self._log = logger

    def connection(self):
        return self._connection

    def has_remote_info(self):
        return not (
            self._state.server_id is None
            or self._state.server_started_at is None
        )

    def set_remote_info(self, server_id, server_started_at):
        self._state.server_id = server_id
        self._state.server_started_at = server_started_at

    def _is_same_server(self, server_id, server_started_at):
        return \
            self._state.server_id == server_id \
            and self._state.server_started_at == server_started_at

    def update_remote_info(self):
        rsp = self._connection.query_system()
        zyn.util.check_server_response(rsp)
        rsp = rsp.as_query_system_rsp()
        self.set_remote_info(rsp.server_id, rsp.started_at)

    def validate_remote_matches_expected(self):
        rsp = self._connection.query_system()
        zyn.util.check_server_response(rsp)
        rsp = rsp.as_query_system_rsp()
        return (
            self._is_same_server(rsp.server_id, rsp.started_at),
            rsp.server_id,
            rsp.started_at,
        )

    def is_empty(self):
        return self._state.fs.is_empty()

    def element(self, node_id=None, path_remote=None):
        if node_id is not None:
            return self._state.fs.local_element_from_node_id(node_id)
        elif path_remote is not None:
            return self._state.fs.local_element_from_remote_path(path_remote)
        else:
            zyn.util.unhandled()

    def create_directory(self, path_in_remote):
        dirname, dir_name = zyn.util.split_remote_path(path_in_remote)

        self._log.debug('Creating directory: dirname="{}", dir_name="{}"'.format(
            dirname, dir_name))
        rsp = self._connection.create_directory(dir_name, parent_path=dirname)
        zyn.util.check_server_response(rsp)
        return rsp.as_create_rsp()

    def create_file(self, path_in_remote, type_of_file):
        dirname, filename = zyn.util.split_remote_path(path_in_remote)

        self._log.debug(
            'Creating file: type_of_file={}, parent="{}", file="{}"'.format(
                type_of_file, dirname, filename)
        )

        rsp = self._connection.create_file(filename, file_type=type_of_file, parent_path=dirname)
        zyn.util.check_server_response(rsp)
        return rsp.as_create_rsp()

    def add(self, path_remote, file_type):
        path_remote = zyn.util.normalized_remote_path(path_remote)
        path_local = self._state.fs.local_path(path_remote)
        element = None

        if os.path.isfile(path_local):
            if file_type is None:
                raise ZynClientException(
                    'Please specify file type: either random access or blob'.format(
                    ))
            element = self._state.fs.create_local_file_element(path_remote, file_type)

        elif os.path.isdir(path_local):
            if file_type is not None:
                raise ZynClientException(
                    'Must not specify either random access or blob for direcotry'
                )
            element = self._state.fs.create_local_directory_element(path_remote)

        else:
            raise ZynClientException(
                '"{}" does not exist'.format(path_remote)
            )
        return [self._state.fs.add(element, self._connection)]

    def query_element(self, path_remote):
        path_remote = zyn.util.normalized_remote_path(path_remote)
        rsp = self._state.fs.query_element(path_remote, self._connection)
        local = None
        if self._state.fs.is_tracked(node_id=rsp.node_id):
            local = self._state.fs.local_element_from_node_id(rsp.node_id)
        return Element(rsp, local)

    def query_directory(self, path_remote):
        path_remote = zyn.util.normalized_remote_path(path_remote)
        element = self._state.fs.element(path_remote, self._connection)
        children = []

        rsp = self._state.fs.query_fs_children(path_remote, self._connection)
        for e in rsp.elements:
            local = None
            if self._state.fs.is_tracked(node_id=e.node_id):
                local = self._state.fs.local_element_from_node_id(e.node_id)

            children.append(Element(
                e,
                local,
            ))
        return element, children

    def fetch(self, path_remote, overwrite):

        fetched_elements = []
        path_remote = zyn.util.normalized_remote_path(path_remote)
        element = self._state.fs.create_local_element_based_on_remote(
            self._connection,
            path_remote,
        )

        if element.is_file() and self._state.fs.is_tracked(element.path_remote()):
            raise ZynClientException(
                'File is already tracked by client, path_parent="{}"'.format(
                    element.path_remote(),
                ))

        if element.is_directory() and self._state.fs.is_tracked(element.path_remote()):
            element = self._state.fs.local_element_from_remote_path(element.path_remote())
        else:
            path_parent = element.path_parent()
            if not self._state.fs.is_tracked(path_remote=path_parent):
                raise ZynClientException(
                    'Parent is not tracked by client, path_parent="{}"'.format(
                        path_parent,
                    ))
            parent = self._state.fs.local_element_from_remote_path(path_parent)
            element = self._state.fs.fetch_element_and_add_to_tracked(
                self._connection,
                parent,
                element,
                overwrite,
            )
            fetched_elements.append(element)

        if element.is_directory():
            fetched_elements += self._state.fs.fetch_children_and_add_to_tracked(
                self._connection,
                element,
                overwrite,
            )
        return fetched_elements

    def sync(self, path_remote, discard_local_changes):

        path_remote = zyn.util.normalized_remote_path(path_remote)
        if not self._state.fs.is_tracked(path_remote):
            raise ZynClientException(
                'Element is not tracked by client, path_remote="{}"'.format(
                    path_remote,
                ))
        synchronized_elements = []
        element = self._state.fs.local_element_from_remote_path(path_remote)
        if element.is_file():
            parent = element.parent()
            synchronized_elements += parent.sync(
                self._connection,
                child_filter=element.name(),
                discard_local_changes=discard_local_changes,
            )
        elif element.is_directory():
            synchronized_elements += element.sync(
                self._connection,
                discard_local_changes=discard_local_changes,
            )
        else:
            zyn.util.unhandled()
        return synchronized_elements

    def remove(self, path_remote, delete_local, delete_remote):

        path_remote = zyn.util.normalized_remote_path(path_remote)
        if not self._state.fs.is_tracked(path_remote):
            raise ZynClientException(
                'Element is not tracked, path="{}"'.format(
                    path_remote
                ))

        element = self._state.fs.local_element_from_remote_path(path_remote)
        if element.is_file():
            if delete_local:
                element.remove_local()
            if delete_remote:
                element.remove_remote(self._connection)
            self._state.fs.remove(element)
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
            self._state.fs.remove(element)
        else:
            zyn.util.unhandled()

    def synchronize_local_files_with_remote(self):
        return self._state.fs.initial_synchronization(self._connection)

    def reset_local_filesystem(self):
        self._state.fs.reset_data()

    def open(self, path_files, sleep_duration, number_of_iterations=None):

        files = {}
        for p in path_files:
            if not self._state.fs.is_tracked(p):
                raise ZynClientException(
                    'Element is not tracked, path="{}"'.format(
                        p
                    ))
            e = self._state.fs.local_element_from_remote_path(p)
            if not e.is_file() or not e.is_random_access():
                raise ZynClientException(
                    'Only random access files can be opened'
                    )
            files[e.node_id()] = OpenLocalFile(e, self._state.fs, self._log)

        try:
            for f in files.values():
                f.open_and_sync(self._connection)

            iteration = 1
            while True:
                self._log.debug('Synchronizing')

                while True:
                    n = self._connection.pop_notification()
                    if n is None:
                        break
                    if n.notification_type() == zyn.messages.Notification.TYPE_DISCONNECTED:
                        print('Connection to Zyn server lost: "{}"'.format(n.reason))
                    elif n.notification_type() in [
                        zyn.messages.Notification.TYPE_MODIFIED,
                        zyn.messages.Notification.TYPE_DELETED,
                        zyn.messages.Notification.TYPE_INSERTED,
                    ]:
                        print('Read notification from remote')
                        if n.node_id in files:
                            files[n.node_id].handle_notification(
                                self._connection,
                                n,
                            )

                for f in files.values():
                    f.push_local_changes(self._connection)

                if number_of_iterations is not None and iteration >= number_of_iterations:
                    raise KeyboardInterrupt()

                iteration += 1
                time.sleep(sleep_duration)

        except KeyboardInterrupt:
            pass

        finally:
            for f in files.values():
                f.close(self._connection)

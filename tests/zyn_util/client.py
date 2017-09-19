import difflib
import json
import logging
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


class LocalFile:
    def __init__(self, revision, file_type):
        self.revision = revision
        self.file_type = file_type


class ZynFilesystemClient:
    def __init__(
            self,
            connection,
            data_dir,
            work_dir
    ):
        self._connection = connection
        self._data_dir = data_dir
        self._work_dir = work_dir
        self._local_files = {}
        self._log = logging.getLogger(__name__)
        self._serialized_filename = self._work_dir + '/.zyn-fs-client'

        if os.path.exists(self._serialized_filename):
            self._load()

    def _load(self):
        self._log.info('Loading client state, path="{}"'.format(self._serialized_filename))
        with open(self._serialized_filename, 'r') as fp:
            content = json.load(fp)
            for key, value in content.items():
                self._local_files[key] = LocalFile(
                    value['revision'],
                    value['file_type'],
                )

    def store(self):
        self._log.info('Storing client state, path="{}", number_of_files={}'.format(
            self._serialized_filename, len(self._local_files)))

        with open(self._serialized_filename, 'w') as fp:
            content = {
                key: {
                    'revision': desc.revision,
                    'file_type': desc.file_type,
                }
                for key, desc in self._local_files.items()}
            json.dump(content, fp)

    def disconnect(self):
        self._connection.disconnect()

    def _check_error(self, rsp):
        if rsp.is_error():
            desc = zyn_util.errors.error_to_string(rsp.error_code())
            raise ZynServerException(rsp.error_code(), desc)

    def authenticate(self, username, password):
        rsp = self._connection.authenticate(username, password)
        self._check_error(rsp)

    def create_random_access_file(self, path_in_remote):
        dirname, filename = os.path.split(path_in_remote)

        if not dirname or not filename:
            raise ZynClientException('Invalid path')

        self._log.debug('Creating random access file: parent="{}", file="{}"'.format(
            dirname, filename))

        rsp = self._connection.create_file_random_access(filename, parent_path=dirname)
        self._check_error(rsp)
        return rsp.as_create_rsp()

    def create_folder(self, path_in_remote):
        dirname, folder_name = os.path.split(path_in_remote)

        if not dirname or not folder_name:
            raise ZynClientException('Invalid path')

        self._log.debug('Creating folder: parent="{}", folder_name="{}"'.format(
            dirname, folder_name))

        rsp = self._connection.create_folder(folder_name, parent_path=dirname)
        self._check_error(rsp)
        return rsp.as_create_rsp()

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

    def fetch(self, path):
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

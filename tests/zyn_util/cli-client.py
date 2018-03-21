import argparse
import cmd
import getpass
import logging
import os.path
import sys

import zyn_util.connection
import zyn_util.client


class ZynCliClient(cmd.Cmd):
    intro = 'Zyn CLI client, type "help" for help'
    prompt = ' '

    def __init__(self, client, pwd='/'):
        super(ZynCliClient, self).__init__()
        self._pwd = pwd
        self._client = client
        self._set_prompt(self._pwd)
        self._log = logging.getLogger(__name__)

    def _parse_args(self, args_str):
        args = args_str.split()
        return [a.strip() for a in args]

    def _set_prompt(self, path):
        if not path.startswith('/'):
            raise RuntimeError('Invalid path')
        last = path.split('/')[-1]
        if last:
            current_folder = './' + last
        else:
            current_folder = path

        self.prompt = '{}:{} {}$ '.format("127.0.0.1", "1234", current_folder)

    def _to_absolute_remote_path(self, filename):
        if os.path.isabs(filename):
            return filename

        if self._pwd == '/':
            return '/' + filename
        return self._pwd + '/' + filename

    def _file_type_to_string(self, file_type):
        if file_type == zyn_util.connection.FILE_TYPE_RANDOM_ACCESS:
            return 'RandomAccess'
        else:
            raise ValueError()

    def do_pwd(self, args):
        'Print current folder'
        print(self._pwd)

    def do_cd(self, args):
        'Change current directory, [String: path]'
        args = self._parse_args(args)
        if len(args) != 1:
            print('Invalid arguments')
            return

        path = self._to_absolute_remote_path(args[0])
        try:
            desc = self._client.query_filesystem(path)
            if desc['type'] == zyn_util.connection.FILE_TYPE_FOLDER:
                self._pwd = path
            else:
                print('Path must be folder')
        except zyn_util.client.ZynServerException as e:
            if e.zyn_error_code == zyn_util.errors.InvalidPath:
                print('Invalid path')
            else:
                print(e)
                return
        except zyn_util.client.ZynClientException as e:
            print(e)
            return

    def do_create_random_access_file(self, args):
        'Create file: [String: filename]'

        args = self._parse_args(args)
        if len(args) != 1:
            print('Invalid arguments: expected filename')
            return

        path = self._to_absolute_remote_path(args[0])
        self._log.debug('Creating random access file, path="{}"'.format(path))

        try:
            node_id = self._client.create_random_access_file(path)
            print('File "{}" created successfully with Node Id {}'.format(path, node_id))
        except zyn_util.client.ZynClientException as e:
            print(e)
            return

    def do_create_folder(self, args):
        'Create folder: [String: name]'

        args = self._parse_args(args)
        if len(args) != 1:
            print('Invalid arguments: expected folder name')
            return

        path = self._to_absolute_remote_path(args[0])
        try:
            node_id = self._client.create_folder(path)
            print('Folder "{}" created successfully with Node Id {}'.format(path, node_id))
        except zyn_util.client.ZynClientException as e:
            print(e)
            return

    def do_fetch(self, args):
        'Fetch remote file to local machine: [String: filename]'

        args = self._parse_args(args)
        if len(args) != 1:
            print('Invalid arguments: expected filename')
            return

        path = self._to_absolute_remote_path(args[0])
        print('Fetching file, path={}'.format(path))

        try:
            node_id, revision, size, file_type, path_in_data = self._client.fetch(path)
            print('File "{}" fetched to "{}" successfully'.format(path, path_in_data))
            print('Node Id: {}, revision: {}, size: {}, type: {}'.format(
                node_id, revision, size, self._file_type_to_string(file_type)))

        except zyn_util.client.ZynClientException as e:
            print(e)
            return

    def do_sync(self, args):
        'Synchronize local changes to remote: [String: filename]'

        args = self._parse_args(args)
        if len(args) != 1:
            print('Invalid arguments: expected filename')
            return

        path = self._to_absolute_remote_path(args[0])
        print('Synchronizing, path={}'.format(path))

        try:
            revision = self._client.sync(path)
            print('File {} synchronized to revision {}'.format(path, revision))

        except zyn_util.client.ZynClientException as e:
            print(e)
            return

    def do_list(self, args):
        'List folder content: [String: path, default=pwd]'

        args = self._parse_args(args)
        if len(args) == 0:
            path = self._pwd
        elif len(args) == 1:
            path = self._to_absolute_remote_path(args[0])
        else:
            print('Invalid arguments: expected filename')
            return

        try:
            for element in self._client.query_list(path):
                print(element)  # todo: format
        except zyn_util.client.ZynClientException as e:
            print(e)
            return

    def do_delete_local_file(self, args):
        'Delete file from local filesystem: [String: path]'

        args = self._parse_args(args)
        if len(args) != 1:
            print('Invalid arguments: expected filename')
            return

        path = self._to_absolute_remote_path(args[0])
        try:
            self._client.remove_local_file(path)
        except zyn_util.client.ZynClientException as e:
            print(e)
            return

    def do_exit(self, _):
        'Close connection and exit client'
        self._client.disconnect()
        sys.exit(0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('username', help='Username')
    parser.add_argument('--password', '-p', help='Username')
    parser.add_argument('remote-address', help='')
    parser.add_argument('remote-port', help='', type=int)

    parser.add_argument('path-data-dir', help='')
    parser.add_argument('path-work-dir', help='')

    parser.add_argument('--path-to-cert', help='', default=None)
    parser.add_argument('--debug-protocol', help='', action='store_true')
    parser.add_argument('--verbose', '-v', action='count', default=0)

    args = vars(parser.parse_args())

    # print (args)

    logging.basicConfig(
        format='ZynClient %(asctime)-15s %(filename)s:%(lineno)s %(levelname)s: %(message)s',
        level=args['verbose'],
    )

    password = args['password']
    if password is None:
        password = getpass.getpass('Password: ')

    connection = zyn_util.connection.ZynConnection(
        args['path_to_cert'],
        args['debug_protocol'],
    )
    connection.load_default_certificate_bundle()
    connection.connect(
        args['remote-address'],
        args['remote-port'],
    )

    client = zyn_util.client.ZynFilesystemClient(
        connection,
        args['path-data-dir'],
        args['path-work-dir'],
    )

    try:
        client.authenticate(args['username'], password)
    except zyn_util.client.ZynClientException as e:
        print(e)
        sys.exit(1)

    print('Successfully connected and authenticated')

    cli = ZynCliClient(client)
    try:
        cli.cmdloop()
    finally:
        client.store()

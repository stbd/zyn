import argparse
import cmd
import getpass
import logging
import os.path
import sys
import datetime

import zyn_util.connection
import zyn_util.client


PATH_TO_DEFAULT_STATE_FILE = os.path.expanduser("~/.zyn-cli-client")


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

    def _to_absolute_remote_path(self, path):
        if os.path.isabs(path):
            return path

        if self._pwd == '/':
            return '/' + path
        return self._pwd + '/' + path

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
            rsp = self._client.create_random_access_file(path)
            print('"{}" created, Node Id: {}'.format(path, rsp.node_id))
        except zyn_util.client.ZynClientException as e:
            print(e)
            return

    def do_create_folder(self, args):
        'Create folder: [String: name]'

        args = self._parse_args(args)
        if len(args) != 1:
            print('Invalid arguments: expected folder name')
            return

        try:
            path = self._to_absolute_remote_path(args[0])
            self._log.debug('Creating folder, path={}'.format(path))

            rsp = self._client.create_folder(path)
            print('"{}" created, Node Id: {}'.format(path, rsp.node_id))
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
            rsp = self._client.connection().query_list(path=path)
            zyn_util.client.check_rsp(rsp)
            rsp = rsp.as_query_list_rsp()
            print('{:6} {:7} {}'.format('Type', 'Node Id', 'Name'))
            for element in rsp.elements:
                type_of = 'file'
                name = element.name
                if element.type_of_element == zyn_util.connection.FILE_TYPE_FOLDER:
                    type_of = 'dir'
                    name += '/'
                print('{:<6} {:<7} {}'.format(type_of, element.node_id, name))

        except zyn_util.client.ZynClientException as e:
            print(e)

    def do_modify_user(self, args):
        '''Modify user: [string: username] [--expiration [String: datetime]]
[--expiration-format: [String: format, default='%d.%m.%Y']]
        '''
        parser = argparse.ArgumentParser()
        parser.add_argument('username', type=str)
        parser.add_argument('--expiration', type=str)
        parser.add_argument('--expiration-format', type=str, default='%d.%m.%Y')
        try:
            # print (args, type(args))
            args = vars(parser.parse_args(self._parse_args(args)))
        except SystemExit as e:
            print(e)
            return

        # print (args)

        password = None
        expiration = args['expiration']
        username = args['username']

        if password is None and expiration is None:
            print('Please specify modified value')
            return

        if expiration is not None:
            expiration = int(datetime.datetime.strptime(
                expiration,
                args['expiration_format'],
            ).timestamp())

        rsp = self._client.connection().modify_user(
            username=username,
            expiration=expiration,
            password=password
        )
        zyn_util.client.check_rsp(rsp)
        print('Ok')

    def do_fetch(self, args):
        'Fetch remote file to local machine: [String: filename]'

        args = self._parse_args(args)
        if len(args) != 1:
            print('Invalid arguments: expected filename')
            return

        path = self._to_absolute_remote_path(args[0])
        print('Fetching file, path={}'.format(path))

        try:
            file = self._client.fetch(path)
            print('File "{}" fetched to "{}" successfully'.format(file._path_in_remote, 'FIXME'))

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
            self._client.sync(path)
            print('File {} synchronized to revision {}'.format(path, None))

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
    parser.add_argument('--init-data-directory-at', help='')
    parser.add_argument('--path-to-cert', help='', default=None)
    parser.add_argument('--path-to-client-file', help='', default=PATH_TO_DEFAULT_STATE_FILE)
    parser.add_argument('--debug-protocol', help='', action='store_true')
    parser.add_argument('--verbose', '-v', action='count', default=0)
    parser.add_argument('--remote-hostname', default=None)

    args = vars(parser.parse_args())

    # print (args)

    logging.basicConfig(
        format='ZynClient %(asctime)-15s %(filename)s:%(lineno)s %(levelname)s: %(message)s',
        level=args['verbose'],
    )

    path_data = args['init_data_directory_at']
    path_state_file = args['path_to_client_file']

    if path_data is not None and os.path.exists(path_state_file):
        raise NotImplementedError()
        sys.exit(1)

    if not os.path.exists(path_state_file):
        print('Zyn client file "{}" does not exist,'
              ' file will be created and client initialized'.format(
                  path_state_file
              ))

        if path_data is None:
            print('To initialize the client, please pass'
                  ' --init-data-directory-at [path-to-data-directory]')
            sys.exit(1)

        answer = input('yes/no? ')
        if answer.lower() == 'yes':
            zyn_util.client.ZynFilesystemClient.init_state_file(
                path_state_file,
                path_data,
            )
        else:
            print('Aborting initialization')
            sys.exit(1)

    password = args['password']
    if password is None:
        password = getpass.getpass('Password: ')

    connection = zyn_util.connection.ZynConnection(
        args['path_to_cert'],
        args['debug_protocol'],
    )

    if args['path_to_cert'] is None:
        connection.load_default_certificate_bundle()

    connection.connect(
        args['remote-address'],
        args['remote-port'],
        args['remote_hostname'],
    )

    client = zyn_util.client.ZynFilesystemClient(
        connection,
        args['path_to_client_file'],
    )

    try:
        rsp = client.connection().authenticate(args['username'], password)
        zyn_util.client.check_rsp(rsp)
    except zyn_util.client.ZynClientException as e:
        print(e)
        sys.exit(1)

    print('Successfully connected and authenticated')

    cli = ZynCliClient(client)
    try:
        cli.cmdloop()
    finally:
        client.store()

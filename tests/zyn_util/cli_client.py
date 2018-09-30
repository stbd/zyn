import argparse
import cmd
import datetime
import getpass
import logging
import os.path
import posixpath
import sys

import zyn_util.client
import zyn_util.connection
import zyn_util.util
import zyn_util.exception


PATH_TO_DEFAULT_STATE_FILE = os.path.expanduser("~/.zyn-cli-client")


def _command_completed():
    print('Command completed successfully')


def _normalise_path(path_original):
    elements = []

    path = path_original
    while True:
        path, e = posixpath.split(path)
        if e != '':
            elements.append(e)
        if e == '' or path == '':
            break

    if path != '':
        elements.append('/')
    elements.reverse()
    return zyn_util.util.join_paths(elements)


class ZynCliClient(cmd.Cmd):
    intro = 'Zyn CLI client, type "help" for help'
    prompt = ' '

    def __init__(self, client, pwd='/', remote_description=None):
        super(ZynCliClient, self).__init__()
        self._pwd = pwd
        self._client = client
        self._log = logging.getLogger(__name__)
        self._remote_description = remote_description
        self._set_prompt(self._pwd)

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

        self.prompt = '{} {}$ '.format(self._remote_description, current_folder)

    def _to_absolute_remote_path(self, path):
        path = _normalise_path(path)
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

    def emptyline(self):
        # Do nothing
        pass

    def do_pwd(self, _):
        'Print current folder'
        print(self._pwd)

    def get_pwd(self):
        return self._pwd

    def _parser_cd(self):
        parser = argparse.ArgumentParser(prog='cd')
        parser.add_argument('path', type=str)
        return parser

    def help_cd(self):
        print(self._parser_cd().format_help())

    def do_cd(self, args):
        parser = self._parser_cd()
        args = vars(parser.parse_args(self._parse_args(args)))
        path_remote = self._to_absolute_remote_path(args['path'])
        path_remote = posixpath.normpath(path_remote)

        # todo: maybe just check from local elements if target is known and folder
        if path_remote == '/':
            pass
        else:
            element = self._client.filesystem_element(path_remote)
            if not element.is_directory():
                raise zyn_util.client.ZynClientException(
                    'Target is not a folder, path="{}"'.format(path_remote)
                )

        self._pwd = path_remote
        self._set_prompt(self._pwd)

    def _parser_check_notifications(self):
        parser = argparse.ArgumentParser(prog='check_notifications')
        parser.add_argument('--timeout', type=int, default=0)
        return parser

    def help_check_notifications(self):
        print(self._parser_check_notifications().format_help())

    def do_check_notifications(self, args):
        parser = self._parser_check_notifications()
        args = vars(parser.parse_args(self._parse_args(args)))

        if self._client.connection().check_for_notifications():
            notification = self._client.connection().pop_notification()
            if notification.notification_type() == \
               zyn_util.connection.Notification.TYPE_DISCONNEDTED:
                print('Connection to Zyn server lost, reason: "{}"'.format(notification.reason))
            else:
                raise NotImplementedError()

        else:
            print('No notifications')

    def _parser_create_file(self):
        parser = argparse.ArgumentParser(prog='create_file')
        parser.add_argument('path', type=str)
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('-ra', '--random-access', action='store_true')
        group.add_argument('-b', '--blob', action='store_true')
        return parser

    def help_create_file(self):
        print(self._parser_create_file().format_help())

    def do_create_file(self, args):
        parser = self._parser_create_file()
        args = vars(parser.parse_args(self._parse_args(args)))

        path = self._to_absolute_remote_path(args['path'])
        if args['random_access']:
            file_type = zyn_util.connection.FILE_TYPE_RANDOM_ACCESS
        elif args['blob']:
            file_type = zyn_util.connection.FILE_TYPE_BLOB
        else:
            raise RuntimeError()

        print('Creating file, path="{}", file_type={}'.format(path, file_type))

        rsp = self._client.create_file(path, file_type)
        print('File "{}" created, Node Id: {}'.format(path, rsp.node_id))

    def _parser_create_directory(self):
        parser = argparse.ArgumentParser(prog='list')
        parser.add_argument('name', type=str)
        return parser

    def help_create_directory(self):
        print(self._parser_create_directory().format_help())

    def do_create_directory(self, args):
        parser = self._parser_create_directory()
        args = vars(parser.parse_args(self._parse_args(args)))

        path = self._to_absolute_remote_path(args['name'])
        rsp = self._client.create_directory(path)
        print('Directory "{}" created successfully with NodeId {}'.format(path, rsp.node_id))

    def _parser_list(self):
        parser = argparse.ArgumentParser(prog='list')
        parser.add_argument('-p', '--path', type=str)
        parser.add_argument('--hide-untracked-files', action='store_true')
        return parser

    def help_list(self):
        print(self._parser_list().format_help())

    def do_list(self, args):
        parser = self._parser_list()
        args = vars(parser.parse_args(self._parse_args(args)))

        path = args['path']
        if path is None:
            path_remote = self._pwd
        else:
            path_remote = self._to_absolute_remote_path(path)

        tracked_files, untracked_files = self._client.list(path_remote)

        print()
        print('{:6} {:10} {:8} {:14} {:9} {}'.format(
            'Type', 'File type', 'Node Id', 'Local element', 'Revision', 'Name'
        ))

        for f in sorted(tracked_files, key=lambda e: e.remote_file.is_file()):
            name = f.remote_file.name
            revision = '-'
            type_of_file = '-'
            if f.remote_file.is_file():
                type_of = 'file'
                revision = f.remote_file.revision
                if f.remote_file.is_random_access():
                    type_of_file = 'RA'
                elif f.remote_file.is_blob():
                    type_of_file = 'Blob'
                else:
                    raise NotImplementedError()

            elif f.remote_file.is_directory():
                name += '/'
                type_of = 'dir'
            else:
                raise NotImplementedError()

            state = None
            if f.local_file.exists_locally and f.local_file.tracked:
                state = 'Tracked'
            elif f.local_file.exists_locally and not f.local_file.tracked:
                state = 'Conflict'
            elif not f.local_file.exists_locally and f.local_file.tracked:
                state = 'Out of sync'
            elif not f.local_file.exists_locally and not f.local_file.tracked:
                state = 'Not fetched'
            else:
                raise NotImplementedError()

            if state is None:
                raise RuntimeError()

            print('{:<6} {:<10} {:<8} {:<14} {:<9} {}'.format(
                type_of, type_of_file, f.remote_file.node_id, state, revision, name
            ))

        if args['hide_untracked_files']:
            return

        if len(untracked_files) == 0:
            return

        print()
        print('Untracked elements:')
        for f in untracked_files:
            path_local = self._client.path_to_local_file(f)
            name = os.path.basename(f)
            if os.path.isdir(path_local):
                name += '/'
            print(name)

    def _parser_add(self):
        parser = argparse.ArgumentParser(prog='add')
        parser.add_argument('path', type=str)
        group = parser.add_mutually_exclusive_group(required=False)
        group.add_argument('-ra', '--random-access', action='store_true')
        group.add_argument('-b', '--blob', action='store_true')
        return parser

    def help_add(self):
        print(self._parser_add().format_help())

    def do_add(self, args):
        parser = self._parser_add()
        args = vars(parser.parse_args(self._parse_args(args)))
        path = args['path']
        path_remote = self._to_absolute_remote_path(path)
        path_local = zyn_util.util.join_paths([self._client._path_data, path_remote])

        if os.path.isfile(path_local):
            if args['random_access']:
                file_type = zyn_util.connection.FILE_TYPE_RANDOM_ACCESS
            elif args['blob']:
                file_type = zyn_util.connection.FILE_TYPE_BLOB
            else:
                raise zyn_util.client.ZynClientException(
                    'Must specify either file type, either random access or blob'
                )
            print('Adding file {}'.format(path_remote))
            self._client.add_file(path_remote, file_type)

        elif os.path.isdir(path_local):
            if args['random_access'] or args['blob']:
                raise zyn_util.client.ZynClientException(
                    'Must not specify either random access or blob for direcotry'
                )

            print('Adding directory {}'.format(path_remote))
            self._client.add_directory(path_remote)
        else:
            raise zyn_util.client.ZynClientException(
                '"{}" does not exist'.format(path_remote)
            )

        element = self._client.filesystem_element(path_remote)
        if element.is_file():
            print('File "{}" (Node Id: {}, revision: {}) pushed to remote successfully'.format(
                element.path_remote,
                element.node_id,
                element.revision,
            ))
        elif element.is_directory():
            print('Directory "{}" (Node Id: {}) pushed to remote successfully'.format(
                element.path_remote,
                element.node_id,
            ))
        else:
            raise NotImplementedError()

    def _parser_modify_user(self):
        parser = argparse.ArgumentParser(prog='modify_user')
        parser.add_argument('username', type=str)
        group = parser.add_mutually_exclusive_group(required=False)
        group.add_argument('--expiration', type=str)
        group.add_argument('-de', '--disable-expiration', action='store_true')
        parser.add_argument('--expiration-format', type=str, default='%d.%m.%Y')
        parser.add_argument('-p', '--password', action='store_true')
        return parser

    def help_modify_user(self):
        print(self._parser_modify_user().format_help())

    def do_modify_user(self, args):
        parser = self._parser_modify_user()
        args = vars(parser.parse_args(self._parse_args(args)))

        # print (args)

        password = None
        expiration = args['expiration']
        disable_expiration = args['disable_expiration']
        username = args['username']

        if (
                args['password'] is False
                and expiration is None
                and not disable_expiration
        ):
            print('Please specify modified value')
            return

        if expiration is not None:
            expiration = int(datetime.datetime.strptime(
                expiration,
                args['expiration_format'],
            ).timestamp())

        if disable_expiration:
            expiration = zyn_util.connection.EXPIRATION_NEVER_EXPIRE

        if args['password']:
            password = getpass.getpass('Password: ')

        rsp = self._client.connection().modify_user(
            username=username,
            expiration=expiration,
            password=password
        )
        zyn_util.util.check_server_response(rsp)
        _command_completed()

    def _parser_fetch(self):
        parser = argparse.ArgumentParser(prog='fetch')
        parser.add_argument('-p', '--path', type=str, default='/')
        parser.add_argument('-s', '--stop-on-error', action='store_true')
        return parser

    def help_fetch(self):
        print(self._parser_fetch().format_help())

    def do_fetch(self, args):
        parser = self._parser_fetch()
        args = vars(parser.parse_args(self._parse_args(args)))

        path = args['path']
        path_remote = self._to_absolute_remote_path(path)
        num_fetched = self._client.fetch(path_remote, args['stop_on_error'])
        print("Fetched {} filesystem elements".format(num_fetched))

    def _parser_sync(self):
        parser = argparse.ArgumentParser(prog='sync')
        parser.add_argument('-p', '--path', type=str, default='/')
        parser.add_argument('-s', '--stop-on-error', action='store_true')
        parser.add_argument('-dl', '--discard-local-changes', action='store_true')
        return parser

    def help_sync(self):
        print(self._parser_sync().format_help())

    def do_sync(self, args):
        parser = self._parser_sync()
        args = vars(parser.parse_args(self._parse_args(args)))
        path_remote = self._to_absolute_remote_path(args['path'])
        num_sync = self._client.sync(
            path_remote,
            args['stop_on_error'],
            args['discard_local_changes']
        )
        print('Done')
        print("Synchronized {} filesystem elements".format(num_sync))
        return num_sync

    def _parser_open(self):
        parser = argparse.ArgumentParser(prog='open')
        parser.add_argument('path-file', type=str, action='append')
        parser.add_argument('-s', '--poll-sleep', type=int, default=5)
        return parser

    def help_open(self):
        print(self._parser_open().format_help())

    def do_open(self, args):
        parser = self._parser_open()
        args = vars(parser.parse_args(self._parse_args(args)))
        path_files = [self._to_absolute_remote_path(f) for f in args['path-file']]
        self._client.open(path_files, args['poll_sleep'])

    def _parser_remove(self):
        parser = argparse.ArgumentParser(prog='remove')
        parser.add_argument('path', type=str)
        parser.add_argument('-dl', '--delete-local-file', action="store_true")
        parser.add_argument('-dr', '--delete-remote-file', action="store_true")
        return parser

    def help_remove(self):
        print(self._parser_remove().format_help())

    def do_remove(self, args):
        parser = self._parser_remove()
        args = vars(parser.parse_args(self._parse_args(args)))
        path_remote = self._to_absolute_remote_path(args['path'])
        delete_local_file = args['delete_local_file']
        delete_remote_file = args['delete_remote_file']

        elements = self._client.find_tracked_elements(path_remote)
        if not elements:
            raise zyn_util.client.ZynClientException(
                'No filesystem elements found, path="{}"'.format(path_remote)
            )

        print('Following filesystem elements will be deleted:')
        for e in elements:
            print('Node Id: {}, Path: "{}"'.format(e[1], e[0]))

        print('Delete local file: "{}", Delete remote file: "{}"'.format(
            delete_local_file,
            delete_remote_file
        ))

        answer = input('Is this ok? yes/no: ')
        if answer.strip().lower() != 'yes':
            print('Canceling')
            return

        for e in elements:
            self._client.remove(e[0], e[1], delete_local_file, delete_remote_file)


def main():
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
    )
    zyn_util.util.verbose_count_to_log_level(args['verbose'])

    path_data = args['init_data_directory_at']
    path_state_file = args['path_to_client_file']

    logging.getLogger(__name__).debug('Using client file from "{}"'.format(path_state_file))

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
        zyn_util.util.check_server_response(rsp)
    except zyn_util.client.ZynClientException as e:
        print(e)
        sys.exit(1)

    print('Successfully connected to Zyn server and authenticated')

    if not client.is_server_info_initialized():
        client.initialize_server_info()
    else:
        if not client.is_connected_to_same_server():
            print('It looks like the server you are connected is not the same server as before')
            server_info = client.server_info()
            print('Server has Id of {} and was started at {}'.format(
                server_info.server_id,
                server_info.server_started_at
            ))
            print('Are you sure this is safe')
            answer = input('yes/no? ')
            if answer.lower() != 'yes':
                sys.exit(0)

            client.initialize_server_info()

            print('Would you like to try add all tracked local files to remote')
            answer = input('yes/no? ')
            if answer.strip().lower() == 'yes':
                client.add_tracked_files_to_remote()
                print('Done')
            else:
                client.remove_local_files()
                print('Removing local files from tracked files')

    remote_description = '{}:{}'.format(args['remote-address'], args['remote-port'])
    cli = ZynCliClient(client, remote_description=remote_description)
    while True:
        try:
            cli.cmdloop()
        except zyn_util.exception.ZynException as e:
            print()
            print('Exception while processing command')
            print(e)
        except KeyboardInterrupt:
            break
        except SystemExit as e:
            print(e)
        finally:
            print()
            print('Storing Zyn state')
            client.store()


if __name__ == '__main__':
    main()

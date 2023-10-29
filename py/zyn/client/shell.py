import argparse
import cmd
import datetime
import getpass
import logging
import os.path
import sys
import traceback

import zyn.client.client
import zyn.client.data
import zyn.connection
import zyn.exception
import zyn.util


PATH_TO_DEFAULT_STATE_FILE = os.path.expanduser("~/.zyn-cli-client")


def _command_completed():
    print('Command completed successfully')


class ZynShell(cmd.Cmd):
    intro = 'Zyn CLI client, type "help" for help'
    prompt = ' '

    def __init__(self, client, log, pwd='/', remote_description=None):
        super(ZynShell, self).__init__()
        self._pwd = pwd
        self._client = client
        self._log = log
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
        path = zyn.util.normalized_remote_path(path)
        if os.path.isabs(path):
            return path
        return zyn.util.join_remote_paths([self._pwd, path])

    def emptyline(self):
        # Do nothing
        pass

    def do_pwd(self, _):
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

        if path_remote == '/':
            pass
        else:
            element = self._client.query_element(path_remote)
            if not element.is_directory():
                raise zyn.client.data.ZynClientException(
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
               zyn.connection.Notification.TYPE_DISCONNECTED:
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
            file_type = zyn.connection.FILE_TYPE_RANDOM_ACCESS
        elif args['blob']:
            file_type = zyn.connection.FILE_TYPE_BLOB
        else:
            zyn.util.unhandled()

        print('Creating file, path="{}", file_type={}'.format(path, file_type))
        rsp = self._client.create_file(path, file_type)
        print('File "{}" created, Node Id: {}'.format(path, rsp.node_id))

    def _parser_add(self):
        parser = argparse.ArgumentParser(prog='add')
        parser.add_argument('paths', type=str, nargs='+')
        group = parser.add_mutually_exclusive_group(required=False)
        group.add_argument('-ra', '--random-access', action='store_true')
        group.add_argument('-b', '--blob', action='store_true')
        return parser

    def help_add(self):
        print(self._parser_add().format_help())

    def do_add(self, args):
        parser = self._parser_add()
        args = vars(parser.parse_args(self._parse_args(args)))

        file_type = None
        if args['random_access']:
            file_type = zyn.connection.FILE_TYPE_RANDOM_ACCESS
        elif args['blob']:
            file_type = zyn.connection.FILE_TYPE_BLOB

        paths = args['paths']
        elements = []
        for path in paths:
            path_remote = self._to_absolute_remote_path(path)
            elements += self._client.add(path_remote, file_type)

        print('Added elements:')
        self._local_elements_header()
        for e in elements:
            self._print_local_element(e)
        _command_completed()

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

    def _parser_fetch(self):
        parser = argparse.ArgumentParser(prog='fetch')
        parser.add_argument('-p', '--path', type=str, default='/')
        parser.add_argument('-o', '--overwrite-local', action='store_true')
        return parser

    def help_fetch(self):
        print(self._parser_fetch().format_help())

    def do_fetch(self, args):
        parser = self._parser_fetch()
        args = vars(parser.parse_args(self._parse_args(args)))

        path = args['path']
        path_remote = self._to_absolute_remote_path(path)
        elements = self._client.fetch(path_remote, args['overwrite_local'])
        if elements:
            print('Fetched elements:')
            self._local_elements_header()
            for e in elements:
                self._print_local_element(e, full_remote_path=True)
        else:
            print('All elements fetched')
        _command_completed()

    def _parser_sync(self):
        parser = argparse.ArgumentParser(prog='sync')
        parser.add_argument('-p', '--path', type=str, default='/')
        parser.add_argument('-dl', '--discard-local-changes', action='store_true')
        return parser

    def help_sync(self):
        print(self._parser_sync().format_help())

    def do_sync(self, args):
        parser = self._parser_sync()
        args = vars(parser.parse_args(self._parse_args(args)))
        path_remote = self._to_absolute_remote_path(args['path'])
        elements = self._client.sync(
            path_remote,
            args['discard_local_changes']
        )
        if elements:
            print('Elements synchronized')
            self._local_elements_header()
            for e in elements:
                self._print_local_element(e, full_remote_path=True)
        else:
            print('All elements up-to-date')
        _command_completed()

    def _parser_remove(self):
        parser = argparse.ArgumentParser(prog='remove')
        parser.add_argument('path', type=str)
        parser.add_argument('-dl', '--delete-local', action="store_true")
        parser.add_argument('-dr', '--delete-remote', action="store_true")
        return parser

    def help_remove(self):
        print(self._parser_remove().format_help())

    def do_remove(self, args):
        parser = self._parser_remove()
        args = vars(parser.parse_args(self._parse_args(args)))
        path_remote = self._to_absolute_remote_path(args['path'])
        self._client.remove(
            path_remote,
            args['delete_local'],
            args['delete_remote'],
        )

    def _parser_list(self):
        parser = argparse.ArgumentParser(prog='list')
        parser.add_argument('-p', '--path', type=str)
        parser.add_argument('--hide-untracked-files', action='store_true')
        return parser

    def help_list(self):
        print(self._parser_list().format_help())

    def _local_elements_header(self):
        print('{:6} {:10} {:8} {:9} {}'.format(
            'Type', 'File type', 'Node Id', 'Revision', 'Name'
        ))

    def _print_local_element(self, element, full_remote_path=False):
        type_of_file = '-'
        revision = '-'
        type_of = 'dir'
        if full_remote_path:
            name = element.path_remote()
        else:
            name = element.name()

        if element.is_file():
            type_of = 'file'
            revision = element.revision()
            if element.is_random_access():
                type_of_file = 'RA'
            elif element.is_blob():
                type_of_file = 'Blob'
            else:
                zyn.util.unhandled()
        elif element.is_directory():
            name += '/'
        else:
            zyn.util.unhandled()

        print('{:<6} {:<10} {:<8} {:<9} {}'.format(
            type_of,
            type_of_file,
            element.node_id(),
            revision,
            name,
        ))

    def _elements_header(self):
        print('{:6} {:10} {:8} {:14} {:9} {}'.format(
            'Type', 'File type', 'Node Id', 'Local element', 'Revision', 'Name'
        ))

    def _print_element(self, element):
        type_of_file = '-'
        revision = '-'
        type_of = 'dir'
        local_element = '-'
        name = element.name()

        if element.is_local():
            local_element = 'tracked'
        else:
            local_element = '-'

        if element.is_file():
            type_of = 'file'
            revision = element.remote_revision()
            if element.is_random_access():
                type_of_file = 'RA'
            elif element.is_blob():
                type_of_file = 'Blob'
            else:
                zyn.util.unhandled()
        elif element.is_directory():
            name += '/'
        else:
            zyn.util.unhandled()

        print('{:<6} {:<10} {:<8} {:<14} {:<9} {}'.format(
            type_of,
            type_of_file,
            element.node_id(),
            local_element,
            revision,
            name,
        ))

    def do_list(self, args):
        parser = self._parser_list()
        args = vars(parser.parse_args(self._parse_args(args)))

        path_remote = args['path']
        if path_remote is None:
            path_remote = self._pwd
        else:
            path_remote = self._to_absolute_remote_path(path_remote)

        element, children = self._client.query_directory(path_remote=path_remote)

        self._elements_header()
        for c in children:
            self._print_element(c)
        if not children:
            print('-')

        print('\nUntracked local files:')
        untracked_printed = False
        if element.is_local():
            untracked = element.children_local_untracked()
            if untracked:
                untracked_printed = True
            for u in untracked:
                name = u.name()
                if u.is_directory():
                    name += '/'
                print(name)
        if not untracked_printed:
            print('-')

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
            expiration = zyn.connection.EXPIRATION_NEVER_EXPIRE

        if args['password']:
            password = getpass.getpass('Password: ')

        rsp = self._client.connection().modify_user(
            username=username,
            expiration=expiration,
            password=password
        )
        zyn.util.check_server_response(rsp)
        _command_completed()

    def _parser_open(self):
        parser = argparse.ArgumentParser(prog='open')
        parser.add_argument('path-file', type=str, action='append')
        parser.add_argument('-s', '--poll-sleep', type=int, default=5)
        parser.add_argument('-i', '--number-of-iterations', type=int)
        return parser

    def help_open(self):
        print(self._parser_open().format_help())

    def do_open(self, args):
        parser = self._parser_open()
        args = vars(parser.parse_args(self._parse_args(args)))
        path_files = [self._to_absolute_remote_path(f) for f in args['path-file']]
        self._client.open(path_files, args['poll_sleep'], args['number_of_iterations'])

    def _parser_show_counters(self):
        parser = argparse.ArgumentParser(prog='show_counters')
        return parser

    def help_show_counters(self):
        print(self._parser_show_counters().format_help())

    def do_show_counters(self, args):
        parser = self._parser_show_counters()
        args = vars(parser.parse_args(self._parse_args(args)))
        rsp = self._client.connection().query_counters().as_query_counters_rsp()
        if rsp.number_of_counters() != 3:
            print('Warning: not all counters are show')
        print('{}: {}'.format('active-connections', rsp.active_connections))
        print('{}: {}'.format('number-of-files', rsp.number_of_files))
        print('{}: {}'.format('number-of-open-files', rsp.number_of_open_files))

import logging
import argparse
import getpass
import os.path
import traceback
import sys

import zyn.socket
import zyn.connection
import zyn.client.shell
import zyn.client.client
import zyn.client.data
import zyn.client.web
import zyn.util


PATH_TO_DEFAULT_STATE_FILE = os.path.expanduser("~/.zyn-cli-client")


def _create_socket(address, port, no_tls=False):
    if no_tls:
        return zyn.socket.ZynSocket.create_no_tls(address, port)
    else:
        return zyn.socket.ZynSocket.create_tls(address, port)


def _create_connection(socket, debug_protocol):
    return zyn.connection.ZynConnection(socket, debug_protocol)


def get_logger(verbose_count):
    level = logging.WARNING
    if verbose_count == 1:
        level = logging.INFO
    elif verbose_count == 2:
        level = logging.DEBUG
    elif verbose_count > 2:
        logger.warn('Maximum number of verbose flags is 2, greater value is ignored')
    return zyn.util.get_logger('zyn', level)


def cli():
    parser = argparse.ArgumentParser()
    parser.add_argument('zyn-address')
    parser.add_argument('zyn-port', type=int)
    parser.add_argument('username')
    parser.add_argument('--no-tls', action='store_true')
    parser.add_argument('--debug-protocol', action='store_true')
    parser.add_argument('--verbose', '-v', action='count', default=0)

    subparsers = parser.add_subparsers(dest='cmd', required=True)

    subparsers.add_parser('test-connection')

    parser_list = subparsers.add_parser('list')
    parser_list.add_argument('path')

    subparsers.add_parser('change-password')

    args = vars(parser.parse_args())

    get_logger(args['verbose'])
    socket = _create_socket(args['zyn-address'], args['zyn-port'], args['no_tls'])
    connection = _create_connection(socket, args['debug_protocol'])
    password = getpass.getpass('Password: ')
    rsp = connection.authenticate(args['username'], password)
    if rsp.is_error():
        raise RuntimeError('Failed to login')

    cmd = args['cmd']
    if cmd == 'test-connection':
        print('Login successfull, connection ok')
    elif cmd == 'list':
        rsp = connection.query_fs_children(path=args['path'])
        zyn.util.check_server_response(rsp)
        rsp = rsp.as_query_fs_children_rsp()
        if rsp.number_of_elements() == 0:
            print('No elements found')
            return

        for e in rsp.elements:
            if e.is_file():
                print(f'File: {e.name}, {e.node_id}, {e.revision}, {e.size}')
            else:
                print(f'Dir: {e.name}, {e.node_id}')
    elif cmd == 'change-password':

        new_password = getpass.getpass('New Password: ')
        rsp = connection.modify_user(args['username'], password=new_password)
        zyn.util.check_server_response(rsp)
        print('Change completed')

    else:
        raise RuntimeError(f'Unknown command "$cmd"')

    connection.disconnect()


def shell():
    parser = argparse.ArgumentParser()
    parser.add_argument('--path-to-client-file', help='', default=PATH_TO_DEFAULT_STATE_FILE)
    parser.add_argument('--no-tls', action='store_true')
    parser.add_argument('--password')
    parser.add_argument('--debug-protocol', help='', action='store_true')
    parser.add_argument('--verbose', '-v', action='count', default=0)
    parser.add_argument('--hearbeat', action='store_true')

    subparsers = parser.add_subparsers(dest='cmd')
    parser_init = subparsers.add_parser('init')
    parser_init.add_argument('username')
    parser_init.add_argument('path-to-data')
    parser_init.add_argument('remote-address')
    parser_init.add_argument('remote-port', type=int)

    args = vars(parser.parse_args())
    log = get_logger(args['verbose'])
    path_client_conf = args['path_to_client_file']
    password = args['password']

    if args['cmd'] == 'init':
        if os.path.exists(path_client_conf):
            print('Zyn was started with "{}" even if state file "{}" exists'.format(
                args['cmd'],
                path_client_conf
            ))
            print('Zyn will replace the existing configuration, is this ok?')
            answer = input('yes/no? ')
            if answer != 'yes':
                print('Cannot initialize client without overriding state file, aborting')
                return

        username = args['username']
        path_local_data = os.path.abspath(args['path-to-data'])

        if not os.path.exists(path_local_data):
            print('Data location "{}" does not exist, aborting'.format(path_local_data))
            return

        state = zyn.client.client.State(
            username,
            args['remote-address'],
            args['remote-port'],
            zyn.client.data.LocalFilesystemManager(path_local_data, log),
        )
        state.to_file(path_client_conf)
        print('Configuration initialized')

    if not os.path.exists(path_client_conf):
        log.error('Cient configuration not found, please create it or point to right path')
        raise RuntimeError(f'Client configuration at path "{path_client_conf}" does not exists')

    client_state = zyn.client.client.State.from_file(path_client_conf, log)
    socket = _create_socket(client_state.address, client_state.port, args['no_tls'])
    connection = _create_connection(socket, args['debug_protocol'])

    if password is None:
        password = getpass.getpass('Password: ')

    rsp = connection.authenticate(client_state.username, password)
    zyn.util.check_server_response(rsp)

    print('Successfully connected and authenticated to remote')

    client = zyn.client.client.ZynFilesystemClient(connection, client_state, log)
    if not client.has_remote_info():
        log.debug('Setting remote info')
        client.update_remote_info()
    else:
        matches, server_id, started_at =  client.validate_remote_matches_expected()
        if matches:
            log.debug('Remote info matches expected')
        else:
            print('It looks like the server you are connected is not the same server as before')
            print('Server has Id of {} and was started at {}'.format(
                server_id,
                zyn.util.timestamp_to_datetime(started_at),
            ))
            print('Are you sure this is safe')
            answer = input('yes/no? ')
            if answer.lower() != 'yes':
                print('Aborting')
                sys.exit(0)

            client.update_remote_info()
            log.debug('Updating remote info')

            if not client.is_empty():
                print('Would you like to try add all tracked local files to remote?')
                print('Answering "no" will reset state of local client')
                answer = input('yes/no? ')
                if answer.strip().lower() == 'yes':
                    added, existed = client.synchronize_local_files_with_remote()
                    print('Done\n')
                    if added:
                        print()
                        print('Following files were pushed to remote:')
                        for a in added:
                            print('\tName: "{}", Node Id: {}'.format(a.path_remote(), a.node_id()))
                        print()

                    if existed:
                        print()
                        print('Note: Following files already existed on remote,')
                        print('they are assumed to be same as local files')
                        for e in existed:
                            print('\tName: "{}", Node Id: {}'.format(e.path_remote(), e.node_id()))
                        print()
                else:
                    client.reset_local_filesystem()
                    print('Local filesystem cleared')



    shell = zyn.client.shell.ZynShell(
        client,
        log=log,
        remote_description=f'{client_state.address}:{client_state.port}'
    )

    if args['hearbeat']:
        connection.start_heartbeat_thread()
    while True:
        try:
            shell.cmdloop()
        except zyn.exception.ZynException:
            print('Exception while processing command')
            traceback.print_exc()
        except KeyboardInterrupt:
            break
        except SystemExit:
            # This is thrown by argparse, it should have better type
            pass
        finally:
            client_state.to_file(path_client_conf)

    connection.disconnect()
    print()
    print('Exiting, saving client state')
    client_state.to_file(path_client_conf)


def webserver():

    parser = argparse.ArgumentParser()
    parser.add_argument('local-port', type=int, default=8080)
    parser.add_argument('zyn-server-ip')
    parser.add_argument('zyn-server-port', type=int)

    parser.add_argument('--no-tls', action='store_true')
    parser.add_argument('--zyn-server-path-to-cert', help='', default=None)
    parser.add_argument('--debug-protocol', action='store_true')
    parser.add_argument('--debug-tornado', action='store_true')
    parser.add_argument('--verbose', '-v', action='count', default=0)
    parser.add_argument('--remote-hostname', default=None)
    parser.add_argument('--server-websocket-address', default=None)

    args = vars(parser.parse_args())
    log = get_logger(args['verbose'])

    def create_connection_callback():
        s = _create_socket(args['zyn-server-ip'], args['zyn-server-port'], args['no_tls'])
        return _create_connection(s, args['debug_protocol'])

    # Try connection once to make sure it works
    try:
        c = create_connection_callback()
        c.disconnect()
    except Exception as e:
        print('Problem connection to Zyn backend')
        return

    server_websocket_address = args['server_websocket_address']
    if server_websocket_address is None:
        protocol = 'wss'
        if args['no_tls']:
            protocol = 'ws'

        server_websocket_address = f'{protocol}://{args["zyn-server-ip"]}:{args["zyn-server-port"]}'

    zyn.client.web.start_server(
        args['local-port'],
        server_websocket_address,
        create_connection_callback,
        log,
        args['debug_tornado'],
    )

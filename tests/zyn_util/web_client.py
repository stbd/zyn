import argparse
import base64
import datetime
import json
import logging
import os
import os.path
import ssl
import time
import uuid
import subprocess

import tornado.log
import tornado.web
import tornado.websocket

import zyn_util.connection
import zyn_util.errors
import zyn_util.util


PATH_STATIC_FILES = os.path.dirname(os.path.abspath(__file__)) + '/web-static-files'
PATH_TEMPLATES = os.path.dirname(os.path.abspath(__file__)) + '/web-templates'
COOKIE_NAME = 'zyn-cookie'
FILE_TYPE_RANDOM_ACCESS = 'random-access'
FILE_TYPE_BLOB = 'blob'
ELEMENT_TYPE_FILE = 'file'
ELEMENT_TYPE_DIRECTORY = 'dir'
OPEN_MODE_READ = 'read'
OPEN_MODE_WRITE = 'write'
NOTIFICATION_SOURCE_WEB_SERVER = 'web-server'
NOTIFICATION_SOURCE_ZYN_SERVER = 'zyn-server'
COOKIE_DURATION_DAYS = 30


def _file_type_to_string(file_type):
    if file_type == zyn_util.connection.FILE_TYPE_RANDOM_ACCESS:
        return FILE_TYPE_RANDOM_ACCESS
    elif file_type == zyn_util.connection.FILE_TYPE_BLOB:
        return FILE_TYPE_BLOB
    else:
        raise ValueError('Invalid file type: {}'.format(file_type))


class UserSessionWebSocket:
    def __init__(self, web_socket, connection):
        self.web_socket = web_socket
        self.connection = connection


class UserSession:
    INITIAL_CONNECTION = -1

    def __init__(self, username, password, initial_connection):
        self._username = username
        self._password = password
        self._websockets = {}
        self._ids = 0
        self._latest_successful_login = datetime.datetime.now()
        if initial_connection is not None:
            self._websockets = {UserSession.INITIAL_CONNECTION: initial_connection}

    def latest_login_duration(self):
        return datetime.datetime.now() - self._latest_successful_login

    def username(self):
        return self._username

    def size(self):
        return len(self._websockets)

    def add_websocket(self, socket):
        connection = None
        if UserSession.INITIAL_CONNECTION in self._websockets:
            connection = self._websockets[UserSession.INITIAL_CONNECTION]
            del self._websockets[UserSession.INITIAL_CONNECTION]
        else:
            connection = self.create_connect()

        self._ids += 1
        id_ = self._ids
        self._websockets[id_] = UserSessionWebSocket(socket, connection)
        return id_

    def remove_websocket(self, id_):
        try:
            del self._websockets[id_]
        except KeyError:
            pass

    def sockets(self, id_):
        return self._websockets[id_]

    def create_connect(self):
        global connection_factory
        connection = connection_factory.create_connection_and_connect()
        rsp = connection.authenticate(self._username, self._password)
        if rsp.is_error():
            raise ValueError(
                'Failed to login using credentials from session for user "{}"'.format(
                    self._username,
                )
            )
        self._latest_successful_login = datetime.datetime.now()
        return connection

    def reconnect(self, id_):
        sockets = self._websockets[id_]
        sockets.connection = self.create_connection()


class WebSocket(tornado.websocket.WebSocketHandler):
    def open(self):
        self._session = None
        self._tab_id = 0
        self._user_id = None
        self._log = logging.getLogger(__name__)
        self._log.info("New websocket connected")
        self._open_files = {}

    def _close_socket(self):
        self._log.info("Closing weboscket, tab_id={}".format(self._tab_id))
        self._session.remove_websocket(self._tab_id)
        self.close()

    def on_close(self):
        self._close_socket()

    def _connection(self):
        return self._session.sockets(self._tab_id).connection

    def on_message(self, message):
        msg = json.loads(message)
        msg_type = msg['type']
        user_id = int(msg['user-id'])
        tab_id = int(msg['tab-id'])

        if tab_id != self._tab_id:
            self._log.error("Closing web socket: tab ids do not match")
            self._close_socket()

        if self._user_id is not None and self._user_id != user_id:
            self._log.error("Closing web socket: user ids do not match")
            self._close_socket()

        max_number_of_trials = 2
        trial = 1
        while trial <= max_number_of_trials:
            try:
                return self._handle_message(msg_type, user_id, msg.get('content', None))
            except zyn_util.exception.ZynConnectionLost:
                self._log.info('Connection to Zyn server lost, trying to reconnect')
                self._send_notification(
                    NOTIFICATION_SOURCE_WEB_SERVER,
                    'reconnect',
                    {'trial': trial}
                )
                try:
                    self._session.reconnect(self._tab_id)
                except Exception:
                    time.sleep(1)
            trial += 1

    def _message_headers(self, msg_type):
        return {
            'type': msg_type,
            'user-id': self._user_id,
            'tab-id': self._tab_id,
        }

    def _send_response(self, msg_type, content=None):
        if content is None:
            content = ''

        msg = self._message_headers(msg_type + '-rsp')
        msg['content'] = content
        self.write_message(json.dumps(msg))

    def _send_error_response(
            self,
            msg_type,
            web_server_error_str,
            zyn_server_error_code
    ):
        msg = self._message_headers(msg_type + '-rsp')

        if zyn_server_error_code is not None:
            zyn_server_error_str = zyn_util.errors.error_to_string(zyn_server_error_code)

        msg['error'] = {
            'web-server-error': web_server_error_str,
            'zyn-server-error': zyn_server_error_str,
        }
        self.write_message(json.dumps(msg))

    def _send_notification(self, source, notification_type, content):
        msg = self._message_headers('notification')
        msg['notification'] = {
            'source': source,
            'type': notification_type,
            'content': content,
        }
        self.write_message(json.dumps(msg))

    def _handle_message(self, msg_type, user_id, content):

        if msg_type == 'register':

            self._log.debug('Register, user_id={}'.format(user_id))
            self._user_id = user_id
            self._session = user_sessions.find_session(self._user_id)
            self._tab_id = self._session.add_websocket(self)
            self._send_response(msg_type)

            self._log.info("Registered, user_id={}, tab_id={}, session.size()={}".format(
                self._user_id, self._tab_id, self._session.size()
            ))

        elif msg_type == 'log':

            level = content['level']
            msg = content['message']
            msg = 'Browser, user_id={}: {}'.format(user_id, msg)

            if level == 'debug':
                self._log.debug(msg)
            elif level == 'info':
                self._log.info(msg)
            else:
                raise RuntimeError()

        elif msg_type == 'list-directory-content':

            path = content['path']
            self._log.debug('{}: path={}'.format(msg_type, path))

            rsp = self._connection().query_fs_children(path=path)
            if rsp.is_error():
                self._send_error_response(msg_type, None, rsp.error_code())
                return

            elements = []
            rsp = rsp.as_query_fs_children_rsp()
            for element in rsp.elements:

                e = {
                    'name': element.name,
                    'node-id': element.node_id,
                }

                if element.is_file():
                    e['element-type'] = ELEMENT_TYPE_FILE
                    e['size'] = element.size
                    e['file-type'] = _file_type_to_string(element.file_type)

                elif element.is_directory():
                    e['element-type'] = ELEMENT_TYPE_DIRECTORY
                    # e['read'] = element.read
                    # e['write'] = element.write
                else:
                    raise RuntimeError()

                elements.append(e)

            self._send_response(msg_type, {
                'elements': elements,
            })

        elif msg_type == 'open-file':

            node_id = content['node-id']
            mode = content['mode']

            self._log.debug('{}: node_id={}, mode={}'.format(
                msg_type,
                node_id,
                mode,
            ))

            if mode == OPEN_MODE_READ:
                rsp = self._connection().open_file_read(node_id=node_id)
            elif mode == OPEN_MODE_WRITE:
                rsp = self._connection().open_file_write(node_id=node_id)
            else:
                raise RuntimeError()

            if rsp.is_error():
                self._send_error_response(msg_type, None, rsp.error_code())
                return

            rsp = rsp.as_open_rsp()
            self._open_files[node_id] = rsp.type_of_file

            self._send_response(msg_type, {
                'size': rsp.size,
                'revision': rsp.revision,
                'block-size': rsp.block_size,
                'file-type':  _file_type_to_string(rsp.type_of_file),
            })

        elif msg_type == 'close-file':

            node_id = content['node-id']

            self._log.debug('{}: node_id={}'.format(
                msg_type,
                node_id,
            ))

            try:
                del self._open_files[node_id]
            except:
                pass

            rsp = self._connection().close_file(node_id=node_id)
            if rsp.is_error():
                self._send_error_response(msg_type, None, rsp.error_code())
                return

            self._send_response(msg_type, None)

        elif msg_type == 'read-file':

            node_id = content['node-id']
            offset = content['offset']
            size = content['size']

            self._log.debug('{}: node_id={}, offset={}, size={}'.format(
                msg_type, node_id, offset, size,
            ))

            rsp, data = self._connection().read_file(node_id, offset, size)
            if rsp.is_error():
                self._send_error_response(msg_type, None, rsp.error_code())
                return

            rsp = rsp.as_read_rsp()
            self._send_response(msg_type, {
                'revision': rsp.revision,
                'offset': rsp.offset,
                'size': rsp.size,
                'bytes': str(base64.b64encode(data), 'ascii'),
            })

        elif msg_type == 'modify-file':

            node_id = content['node-id']
            modifications = content['modifications']
            revision = content['revision']

            self._log.debug('{}: node_id={}, len(modifications)={}'.format(
                msg_type, node_id, len(modifications),
            ))

            if len(modifications) == 0:
                raise RuntimeError('No modifications sent')

            for mod in modifications:

                type_of = mod['type']
                offset = mod['offset']

                if type_of == 'add':
                    rsp = self._connection().ra_insert(
                        node_id,
                        revision,
                        offset,
                        base64.b64decode(mod['bytes']),
                    )
                elif type_of == 'delete':
                    rsp = self._connection().ra_delete(
                        node_id,
                        revision,
                        offset,
                        mod['size'],
                    )
                else:
                    raise RuntimeError()

                if rsp.is_error():
                    self._send_error_response(msg_type, None, rsp.error_code())
                    return

                rsp = rsp.as_write_rsp()
                revision = rsp.revision

            self._send_response(msg_type, {
                'revision': rsp.revision,
            })

        elif msg_type == 'create-element':

            name = content['name']
            path_parent = content['path-parent']
            element_type = content['element-type']

            self._log.debug('{}: element_type={}, name={}, path_parent={}'.format(
                msg_type, element_type, name, path_parent,
            ))

            if element_type == ELEMENT_TYPE_DIRECTORY:

                rsp = self._connection().create_directory(
                    name,
                    parent_path=path_parent,
                )

            elif element_type == ELEMENT_TYPE_FILE:
                file_type = content['file-type']
                if file_type == FILE_TYPE_RANDOM_ACCESS:
                    rsp = self._connection().create_file_random_access(
                        name,
                        parent_path=path_parent,
                    )
                elif file_type == FILE_TYPE_BLOB:
                    rsp = self._connection().create_file_blob(
                        name,
                        parent_path=path_parent,
                    )
                else:
                    raise RuntimeError()
            else:
                raise RuntimeError()

            if rsp.is_error():
                self._send_error_response(msg_type, None, rsp.error_code())
                return

            rsp = rsp.as_create_rsp()
            self._send_response(msg_type, {
                'node-id': rsp.node_id,
            })

        elif msg_type == 'query-system':

            rsp = self._connection().query_system()
            if rsp.is_error():
                self._send_error_response(msg_type, None, rsp.error_code())
                return

            rsp = rsp.as_query_system_rsp()
            self._send_response(msg_type, {
                'zyn-server': {
                    'started-at': {
                        'timestamp': rsp.started_at,
                    },
                    'server-id': {
                        'string': rsp.server_id,
                    },
                },
                'web-server': {
                    'certificate-expiration': {
                        'string': certifacte_expiration,
                    },
                },
            })

            pass

        elif msg_type == 'delete-element':

            node_id = content['node-id']
            self._log.debug('{}: node_id={}'.format(
                msg_type, node_id,
            ))

            rsp = self._connection().delete(node_id=node_id)
            if rsp.is_error():
                self._send_error_response(msg_type, None, rsp.error_code())
                return

            self._send_response(msg_type, None)

        elif msg_type == 'check-notifications':

            while True:
                n = self._connection().pop_notification()
                if n is None:
                    break
                self._handle_notification(n)

        else:
            self._log.error("Closing socket: unexpected message: {}".format(msg_type))
            self._close_socket()

    def _handle_notification(self, notification):
        if isinstance(notification, zyn_util.connection.NotificationDisconnected):
            self._send_notification(
                NOTIFICATION_SOURCE_ZYN_SERVER,
                'disconnected',
                {
                    'reason': notification.reason,
                })

        elif isinstance(notification, zyn_util.connection.NotificationModified):

            if notification.node_id not in self._open_files:
                self._log.info('Discarding notification for not open file, node_id={}'.format(
                    notification.node_id
                ))
                return

            file_type = self._open_files[notification.node_id]
            if file_type == zyn_util.connection.FILE_TYPE_BLOB:
                self._send_notification(
                    NOTIFICATION_SOURCE_ZYN_SERVER,
                    'blob-modified',
                    {
                        'node-id': notification.node_id,
                        'revision': notification.revision,
                    })

            elif file_type == zyn_util.connection.FILE_TYPE_RANDOM_ACCESS:

                if (
                        notification.notification_type()
                        == zyn_util.connection.Notification.TYPE_MODIFIED
                ):
                    rsp, bytes = self._connection().read_file(
                        notification.node_id,
                        notification.block_offset,
                        notification.block_size
                    )

                    self._send_notification(
                        NOTIFICATION_SOURCE_ZYN_SERVER,
                        'random-access-modification',
                        {
                            'node-id': notification.node_id,
                            'revision': notification.revision,
                            'offset': notification.block_offset,
                            'bytes': str(base64.b64encode(bytes), 'ascii'),
                        })

                elif (
                    notification.notification_type()
                    == zyn_util.connection.Notification.TYPE_INSERTED
                ):
                    rsp, bytes = self._connection().read_file(
                        notification.node_id,
                        notification.block_offset,
                        notification.block_size
                    )

                    self._send_notification(
                        NOTIFICATION_SOURCE_ZYN_SERVER,
                        'random-access-insert',
                        {
                            'node-id': notification.node_id,
                            'revision': notification.revision,
                            'offset': notification.block_offset,
                            'bytes': str(base64.b64encode(bytes), 'ascii'),
                        })

                elif (
                    notification.notification_type()
                    == zyn_util.connection.Notification.TYPE_DELETED
                ):
                    self._send_notification(
                        NOTIFICATION_SOURCE_ZYN_SERVER,
                        'random-access-delete',
                        {
                            'node-id': notification.node_id,
                            'revision': notification.revision,
                            'offset': notification.block_offset,
                            'size': notification.block_size,
                        })
                else:
                    raise RuntimeError()
            else:
                raise RuntimeError()


class UserSessions:
    def __init__(self):
        self._sessions = {}

    def data(self):
        return self._sessions

    def find_session(self, user_id):
        try:
            return self._sessions[user_id]
        except KeyError:
            logging.getLogger(__name__).warn('user session "{}" not found'.format(user_id))
            return None

    def add_session(self, session):
        id_ = uuid.uuid4().int
        self._sessions[id_] = session
        return id_

    def remove(self, id_):
        del self._sessions[id_]


class MainHandler(tornado.web.RequestHandler):
    HANDLER_URL = '/fs'

    def post(self, path_file):
        username = self.get_body_argument("username")
        password = self.get_body_argument("password")

        if len(path_file) == 0:
            path_file = '/'

        log = logging.getLogger(__name__)
        log.info('Login, username="{}", path_file="{}"'.format(username, path_file))

        global connection_factory
        connection = connection_factory.create_connection_and_connect()
        rsp = connection.authenticate(username, password)

        if not rsp.is_error():
            user_id = user_sessions.add_session(UserSession(username, password, connection))
            log.info('New user session created for "{}"'.format(username))
            self.set_secure_cookie(COOKIE_NAME, str(user_id), expires_days=COOKIE_DURATION_DAYS)
        else:
            log.info('Failed to login, username="%s", error="%d"' % (username, rsp.error_code()))

        self.redirect(path_file)

    def get(self, path):
        global user_sessions
        log = logging.getLogger(__name__)
        is_logged_in = False
        user_id = 0
        cookie_user_id = None

        try:
            cookie_user_id = int(self.get_secure_cookie(
                COOKIE_NAME,
                max_age_days=COOKIE_DURATION_DAYS
            ))
        except ValueError:
            pass
        except TypeError:
            pass

        if cookie_user_id is not None:
            session = user_sessions.find_session(cookie_user_id)
            if session is not None:
                user_id = cookie_user_id
                is_logged_in = True
                log.info('Existing session found, username="{}"'.format(session.username()))

        if is_logged_in:
            if len(path) > 1:
                path = zyn_util.util.normalized_remote_path('/' + path)
                path_parent, name = zyn_util.util.split_remote_path(path)
            else:
                path_parent, name = ('/', '')

            log.info('get, path_parent="{}", name="{}"'.format(
                path_parent, name,
            ))

            self.render(
                "main.html",
                zyn_user_id=str(user_id),
                root_url=self.HANDLER_URL,
                path_parent=path_parent,
                name=name,
            )
        else:
            log.info('Unauthenticated user')
            self.render("login.html")


class RawHandler(tornado.web.RequestHandler):
    def get(self, path):
        log = logging.getLogger(__name__)
        cookie_user_id = None

        try:
            cookie_user_id = int(self.get_secure_cookie(
                COOKIE_NAME,
                max_age_days=COOKIE_DURATION_DAYS
            ))
        except ValueError:
            pass
        except TypeError:
            pass

        self.clear()
        if cookie_user_id is None:
            self.set_status(403)
            return

        path = zyn_util.util.normalized_remote_path('/' + path)
        _, filename = zyn_util.util.split_remote_path(path)
        session = user_sessions.find_session(cookie_user_id)
        socket_id = None
        open_rsp = None

        log.debug('Raw file requested, path="{}", cookie="{}"'.format(
            path, cookie_user_id,
        ))

        try:
            socket_id = session.add_websocket(None)
            connection = session.sockets(socket_id).connection
            rsp = connection.open_file_read(path=path)
            if rsp.is_error():
                self.set_status(400)
                raise RuntimeError(zyn_util.errors.error_to_string(rsp.error_code()))

            open_rsp = rsp.as_open_rsp()
            if open_rsp.type_of_file == zyn_util.connection.FILE_TYPE_RANDOM_ACCESS:
                rsp, data = connection.read_file(open_rsp.node_id, 0, open_rsp.size)
                if rsp.is_error():
                    self.set_status(500)
                    raise RuntimeError(zyn_util.errors.error_to_string(rsp.error_code()))
                self.write(data)
            elif open_rsp.type_of_file == zyn_util.connection.FILE_TYPE_BLOB:
                if open_rsp.size > open_rsp.block_size:
                    # For large files, server should sent the content is blocks
                    self.set_status(500)
                    raise RuntimeError('Large file download not implemented')

                rsp, data = connection.read_file(open_rsp.node_id, 0, open_rsp.size)
                if rsp.is_error():
                    self.set_status(500)
                    raise RuntimeError(zyn_util.errors.error_to_string(rsp.error_code()))
                self.write(data)
            else:
                self.set_status(500)
                raise RuntimeError()

            self.set_header('Content-Disposition', 'attachment; filename=' + filename)

        except RuntimeError as e:
            self.write(str(e))

        finally:
            if open_rsp is not None:
                connection.close_file(open_rsp.node_id)
            if socket_id is not None:
                session.remove_websocket(socket_id)


class RootHandler(tornado.web.RequestHandler):
    def get(self, path):
        self.redirect('/fs/', permanent=True)


class ZynConnectionFactory:
    def __init__(
            self,
            path_cert,
            server_ip,
            server_port,
            remote_hostname=None,
            debug_protocol=False,
    ):
        self._path_cert = path_cert
        self._server_ip = server_ip
        self._server_port = server_port
        self._debug_protocol = debug_protocol
        self._remote_hostname = remote_hostname

    def create_connection_and_connect(self):
        connection = zyn_util.connection.ZynConnection(
            self._path_cert,
            self._debug_protocol
        )

        if self._path_cert is None:
            connection.load_default_certificate_bundle()

        connection.connect(
            self._server_ip,
            self._server_port,
            self._remote_hostname,
        )
        return connection


def _timer_callback():
    global user_sessions
    expiration_duration_secs = COOKIE_DURATION_DAYS * 24 * 60 * 60
    expired_sessions = []

    for id_, session in user_sessions.data().items():
        d = session.latest_login_duration()
        if d.total_seconds() > expiration_duration_secs:
            logging.getLogger(__name__).info(
                'Cleaning up expired session: id="{}"'.format(id_)
            )
            expired_sessions.append(id_)

    for id_ in expired_sessions:
        user_sessions.remove(id_)


def _certificate_expiration(path):
    p = subprocess.Popen(
        [
            'openssl',
            'x509',
            '-enddate',
            '-noout',
            '-in',
            path,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    (data_stdout, data_stderr) = p.communicate()
    if p.returncode != 0:
        print(data_stderr)
        print(data_stdout)
        raise RuntimeError('Reading certificate expiration failed')

    parsed = data_stdout.decode('utf8').strip().split('=')
    return parsed[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('local-port', type=int, default=8080, help='')
    parser.add_argument('zyn-server-ip', help='')
    parser.add_argument('zyn-server-port', help='', type=int)

    parser.add_argument('--ssl-path-to-cert', help='')
    parser.add_argument('--ssl-path-to-key', help='')
    parser.add_argument('--zyn-server-path-to-cert', help='', default=None)
    parser.add_argument('--debug-protocol', action='store_true', help='')
    parser.add_argument('--debug-tornado', action='store_true', help='')
    parser.add_argument('--verbose', '-v', action='count', default=0)
    parser.add_argument('--remote-hostname', default=None)

    args = vars(parser.parse_args())

    ssl_path_cert = args['ssl_path_to_cert']
    ssl_path_key = args['ssl_path_to_key']
    ssl_context = None

    if ssl_path_cert is not None or ssl_path_key is not None:
        if ssl_path_cert is None or ssl_path_key is None:
            print('When using SSL, both key and certificate need to be passed')
            return

        ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        ssl_context.load_cert_chain(ssl_path_cert, ssl_path_key)

    global connection_factory
    connection_factory = ZynConnectionFactory(
        args['zyn_server_path_to_cert'],
        args['zyn-server-ip'],
        args['zyn-server-port'],
        args['remote_hostname'],
        args['debug_protocol'],
    )

    try:
        connection = connection_factory.create_connection_and_connect()
        connection.disconnect()
    except ConnectionRefusedError as e:
        print(e)
        print()
        print('Failed to connect to Zyn server')
        return

    timer = tornado.ioloop.PeriodicCallback(
        _timer_callback,
        1000 * 60 * 60,
    )

    app = tornado.web.Application(
        [
            (r'/static/(.*)', tornado.web.StaticFileHandler, {"path": PATH_STATIC_FILES}),
            (r'/websocket', WebSocket),
            (r'/raw/(.*)', RawHandler),
            (r'/fs(.*)', MainHandler),
            (r'/(.*)', RootHandler),
        ],
        cookie_secret=base64.b64encode(os.urandom(50)).decode('utf8'),
        static_path=PATH_STATIC_FILES,
        template_path=PATH_TEMPLATES,
        debug=args['debug_tornado'],
    )

    tornado.log.enable_pretty_logging()
    zyn_util.util.verbose_count_to_log_level(args['verbose'])

    global user_sessions
    user_sessions = UserSessions()

    global certifacte_expiration
    certifacte_expiration = None
    if args['zyn_server_path_to_cert']:
        certifacte_expiration = _certificate_expiration(args['zyn_server_path_to_cert'])

    app.listen(args['local-port'], ssl_options=ssl_context)
    timer.start()
    tornado.ioloop.IOLoop.current().start()


if __name__ == '__main__':
    main()

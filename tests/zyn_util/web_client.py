import argparse
import base64
import datetime
import logging
import os
import os.path
import ssl
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
COOKIE_DURATION_DAYS = 30


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
            connection = self.create_connection()

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

    def create_connection(self):
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
        global connection_factory
        username = self.get_body_argument("username")
        password = self.get_body_argument("password")

        if len(path_file) == 0:
            path_file = '/'

        log = logging.getLogger(__name__)
        log.info('Login, username="{}", path_file="{}"'.format(username, path_file))

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
        global connection_factory
        global server_address
        log = logging.getLogger(__name__)
        cookie_user_id = None
        session = None

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
            connection = session.create_connection()
            rsp = connection.allocate_authentication_token()
            connection.disconnect()

            if rsp.is_error():
                log.error('Failed to allocate login token for user "{}", code: {}'.format(
                    session.username(),
                    rsp.error_code(),
                ))
                raise RuntimeError()

            auth_token = rsp.as_allocate_auth_token_response().token
            log.info('Existing session found authentication token allocated, username="{}"'.format(
                session.username()
            ))

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
                zyn_user_id=str(cookie_user_id),
                root_url=self.HANDLER_URL,
                path_parent=path_parent,
                name=name,
                authentication_token=auth_token,
                server_address=server_address,
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

    def ip(self):
        return self._server_ip

    def port(self):
        return self._server_port

    def create_connection_and_connect(self):

        if self._path_cert is None:
            socket = zyn_util.connection.ZynSocket.create(self._server_ip, self._server_port)
        else:
            socket = zyn_util.connection.ZynSocket.create_with_custom_cert(
                self._server_ip,
                self._server_port,
                self._path_cert,
                self._remote_hostname,
            )
        return zyn_util.connection.ZynConnection(
            socket,
            self._debug_protocol
        )


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

    expiration_string = data_stdout.decode('utf8').strip().split('=')[1]

    p = subprocess.Popen(
        [
            'date',
            '-d',
            expiration_string,
            '+%s'
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    (data_stdout, data_stderr) = p.communicate()
    if p.returncode != 0:
        print(data_stderr)
        print(data_stdout)
        raise RuntimeError('Converting certificate expiration to timestamp failed')

    ts = data_stdout.strip().decode('utf8')
    return ts


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
    parser.add_argument('--server-websocket-address', default=None)

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

    global server_address
    server_address = args['server_websocket_address']
    if server_address is None:
        server_address = 'wss://{}:{}'.format(args['zyn-server-ip'], args['zyn-server-port'])

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

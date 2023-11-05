import argparse
import base64
import datetime
import logging
import os
import os.path
import ssl
import uuid
import subprocess
import sys
import time

import tornado.log
import tornado.web
import tornado.websocket

import zyn.connection
import zyn.errors
import zyn.util



PATH_STATIC_FILES = sys.prefix + '/zyn-web-static'
PATH_TEMPLATES = sys.prefix + '/zyn-web-templates'

if not os.path.isdir(PATH_STATIC_FILES) or not os.path.isdir(PATH_TEMPLATES):
    print('Asumming development install for web server paths')
    PATH_STATIC_FILES = os.path.dirname(os.path.abspath(__file__)) + '/zyn-web-static'
    PATH_TEMPLATES = os.path.dirname(os.path.abspath(__file__)) + '/zyn-web-templates'

COOKIE_NAME = 'zyn-cookie'
FILE_TYPE_RANDOM_ACCESS = 'random-access'
FILE_TYPE_BLOB = 'blob'
COOKIE_DURATION_DAYS = 30
create_zyn_connection = None


class UserSessionWebSocket:
    def __init__(self, web_socket, connection):
        self.web_socket = web_socket
        self.connection = connection


class UserSession:
    def __init__(self, username, password, initial_connection):
        self._username = username
        self._password = password
        self._connection = initial_connection
        self._created_timestamp = time.time()

    def _renew_connection(self):
        global create_zyn_connection
        self._connection = create_zyn_connection()
        rsp = self._connection.authenticate(self._username, self._password)
        if rsp.is_error():
            raise RuntimeError('Failed to authenticate for user')

    def session_age(self):
        return time.time() - self._created_timestamp

    def get_connection(self):
        if self._connection is None:
            self._renew_connection()

        return self._connection

    def allocate_auth_token(self):
        log = logging.getLogger(__name__)

        if self._connection is None:
            self._renew_connection()

        rsp = self._connection.allocate_authentication_token()
        self._connection.disconnect()
        self._connection = None

        if rsp.is_error():
            log.error('Failed to allocate login token for user "{}", code: {}'.format(
                self._username(),
                rsp.error_code(),
            ))
            raise RuntimeError()

        log.info('Authentication token allocated, username="{}"'.format(
            self._username
        ))
        return rsp.as_allocate_auth_token_response().token


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
        global create_zyn_connection

        username = self.get_body_argument("username")
        password = self.get_body_argument("password")

        if len(path_file) == 0:
            path_file = '/'

        log = logging.getLogger(__name__)
        log.info('Login, username="{}", path_file="{}"'.format(username, path_file))

        connection = create_zyn_connection()
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
        global server_address
        global create_zyn_connection

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

            token = session.allocate_auth_token()

            if len(path) > 1:
                path = zyn.util.normalized_remote_path('/' + path)
                path_parent, name = zyn.util.split_remote_path(path)
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
                authentication_token=token,
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

        path = zyn.util.normalized_remote_path('/' + path)
        _, filename = zyn.util.split_remote_path(path)
        session = user_sessions.find_session(cookie_user_id)
        socket_id = None
        open_rsp = None

        log.debug('Raw file requested, path="{}", cookie="{}"'.format(
            path, cookie_user_id,
        ))

        try:
            connection = session.get_connection()
            rsp = connection.open_file_read(path=path)
            if rsp.is_error():
                self.set_status(400)
                raise RuntimeError(zyn.errors.error_to_string(rsp.error_code()))

            open_rsp = rsp.as_open_rsp()
            if open_rsp.type_of_file == zyn.connection.FILE_TYPE_RANDOM_ACCESS:
                rsp, data = connection.read_file(open_rsp.node_id, 0, open_rsp.size)
                if rsp.is_error():
                    self.set_status(500)
                    raise RuntimeError(zyn.errors.error_to_string(rsp.error_code()))
                self.write(data)
            elif open_rsp.type_of_file == zyn.connection.FILE_TYPE_BLOB:
                if open_rsp.size > open_rsp.block_size:
                    # For large files, server should sent the content is blocks
                    self.set_status(500)
                    raise RuntimeError('Large file download not implemented')

                rsp, data = connection.read_file(open_rsp.node_id, 0, open_rsp.size)
                if rsp.is_error():
                    self.set_status(500)
                    raise RuntimeError(zyn.errors.error_to_string(rsp.error_code()))
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



def _timer_callback():
    global user_sessions
    expiration_duration_secs = COOKIE_DURATION_DAYS * 24 * 60 * 60
    expired_sessions = []

    for id_, session in user_sessions.data().items():
        d = session.session_age()
        if d > expiration_duration_secs:
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


def start_server(
        local_port,
        websocket_address,
        create_zyn_connection_callback,
        debug_tornado=False,

):
    global server_address
    global create_zyn_connection

    server_address = websocket_address
    create_zyn_connection = create_zyn_connection_callback

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
        debug=debug_tornado,
    )

    tornado.log.enable_pretty_logging()

    global user_sessions
    user_sessions = UserSessions()

    app.listen(local_port)
    timer.start()
    tornado.ioloop.IOLoop.current().start()

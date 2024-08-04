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
log = None
URL_LOGIN = '/login'
URL_FS = '/fs'
QUERY_PARAN_NAME_PATH = 'path'
QUERY_PARAN_NAME_MODE = 'mode'
QUERY_PARAN_NAME_ERROR = 'error'


def _get_client_cookie(handler):
    cookie_user_id = None
    try:
        cookie_user_id = int(handler.get_signed_cookie(
            COOKIE_NAME,
            max_age_days=COOKIE_DURATION_DAYS
        ))
    except ValueError:
        pass
    except TypeError:
        pass

    return cookie_user_id


def _logout_user(handler):
    handler.clear_cookie(COOKIE_NAME)
    handler.redirect(URL_LOGIN)


def _generate_url(base_url, file_path=None, mode=None, error=None):
    url = base_url
    if file_path is not None or mode is not None:
        url += '?'

    if file_path is not None:
        url += f'{QUERY_PARAN_NAME_PATH}={file_path}'

    if mode is not None:
        if not url.endswith('?'):
            url += "&"
        url += f'{QUERY_PARAN_NAME_MODE}={mode}'

    if error is not None:
        if not url.endswith('?'):
            url += "&"
        url += f'{QUERY_PARAN_NAME_ERROR}={error}'

    return url


class UserSession:
    def __init__(self, username, password):
        self._username = username
        self._password = password
        self._connection = None
        self._created_timestamp = time.time()

    def session_duration_sec(self):
        return time.time() - self._created_timestamp

    def connect(self):
        self._connection = create_zyn_connection()

    def release_connection(self):
        self._connection.disconnect()
        self._connection = None

    def create_logged_in_connection(self):
        c = create_zyn_connection()
        rsp = c.authenticate(self._username, self._password)
        if rsp.is_error():
            raise RuntimeError(f'Login failed, username="{username}", error="{rsp.error_code()}')
        return c

    def _reconnect_if_needed(self):
        if self._connection is None:
            log.debug(f'Creating new connection for user {self._username}')
            self._connection = create_zyn_connection()
            self.authenticate()

    def authenticate(self):
        rsp = self._connection.authenticate(self._username, self._password)
        if rsp.is_error():
            if rsp.error_code() == zyn.errors.InvalidUsernamePassword:
                return False
            else:
                raise RuntimeError(f'Login failed, username="{username}", error="{rsp.error_code()}')
        return True

    def allocate_token(self):
        self._reconnect_if_needed()
        rsp = self._connection.allocate_authentication_token()
        if rsp.is_error():
            log.error('Failed to allocate login token for user "{}", code: {}'.format(
                self._username(),
                rsp.error_code(),
            ))
            raise RuntimeError()

        log.debug('Authentication token allocated, username="{}"'.format(
            self._username
        ))

        return rsp.as_allocate_auth_token_response().token


class UserSessions:
    def __init__(self):
        self._sessions = {}

    def session(self, user_id):
        try:
            return self._sessions[user_id]
        except KeyError:
            log.error(f'User session not found with id "{user_id}"')
            return None

    def has_session(self, user_id):
        return user_id in self._sessions

    def sessions(self):
        return self._sessions.items()

    def add(self, session):
        id_ = uuid.uuid4().int
        self._sessions[id_] = session
        return id_

    def remove(self, id_):
        del self._sessions[id_]


class RootHandler(tornado.web.RequestHandler):
    def get(self, args, kwargs=None):
        user_id = _get_client_cookie(self)
        log.info(f'Root handler, has token {user_id is None}')
        if user_id is None or not user_sessions.has_session(user_id):
            self.redirect(URL_LOGIN)
        else:
            self.redirect(URL_FS)


class MainHandler(tornado.web.RequestHandler):
    def get(self, args, kwargs=None):
        global user_sessions
        global server_address
        global create_zyn_connection

        path = self.get_argument(QUERY_PARAN_NAME_PATH, args or '/')
        user_id = _get_client_cookie(self)

        log.info(f'Main handler, path "{path}"')

        if user_id is None or not user_sessions.has_session(user_id):

            url = _generate_url(URL_LOGIN, path, self.get_argument(QUERY_PARAN_NAME_MODE, None))
            log.info(f'Request for "{path}" without token, redirecting to {url}')
            self.redirect(url)

        else:

            session = user_sessions.session(user_id)
            token = session.allocate_token()
            session.release_connection()

            path_dir, filename = os.path.split(os.path.normpath(path))
            log.info(f'Rendering web client with parent "{path_dir}" and filename "{filename}"')

            self.render(
                "main.html",
                zyn_user_id=str(user_id),
                root_url=URL_FS,
                path_parent=path_dir,
                file=filename,
                file_mode=self.get_argument(QUERY_PARAN_NAME_MODE, None),
                authentication_token=token,
                server_address=server_address,
            )


class RawHandler(tornado.web.RequestHandler):
    def get(self, path):
        global user_sessions
        user_id = _get_client_cookie(self)
        self.clear()

        if user_id is None or not user_sessions.has_session(user_id):
            self.set_status(403)
            return

        path_file = os.path.normpath('/' + path)
        filename = os.path.basename(path_file)
        open_rsp = None

        log.info(f'Requesting file "{filename}" from path "{path_file}"')

        try:
            session = user_sessions.session(user_id)
            connection = session.create_logged_in_connection()
            rsp = connection.open_file_read(path=path_file)
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

        except Excetion:
            log.exeption('Failed to read file')
        finally:
            if open_rsp is not None:
                connection.close_file(open_rsp.node_id)


class LoginHandler(tornado.web.RequestHandler):
    def get(self, args, kwargs=None):
        self.render("login.html")

    def post(self, args, kwargs=None):
        global user_sessions
        username = self.get_body_argument("username")
        password = self.get_body_argument("password")

        if not username or not password:

            url = _generate_url(
                URL_LOGIN,
                self.get_argument(QUERY_PARAN_NAME_PATH, None),
                self.get_argument(QUERY_PARAN_NAME_MODE, None),
                'credentials',
            )
            self.redirect(url)

        else:

            session = UserSession(username, password)
            session.connect()
            if session.authenticate():

                user_id = user_sessions.add(session)
                self.set_signed_cookie(
                    COOKIE_NAME,
                    str(user_id),
                    expires_days=COOKIE_DURATION_DAYS
                )

                path = self.get_argument(QUERY_PARAN_NAME_PATH, None)
                url = _generate_url(
                    os.path.normpath(f'{URL_FS}/{path or ""}'),
                    mode=self.get_argument(QUERY_PARAN_NAME_MODE, None),
                )
                log.debug(f'Login successfull for "{username}", redirecting "{url}"')
                self.redirect(url)

            else:

                url = _generate_url(
                    URL_LOGIN,
                    self.get_argument(QUERY_PARAN_NAME_PATH, None),
                    self.get_argument(QUERY_PARAN_NAME_MODE, None),
                    'credentials',
                )
                self.redirect(url)


class LogoutHandler(tornado.web.RequestHandler):
    def get(self, args, kwargs=None):
        _logout_user(self)


def _timer_callback():
    global user_sessions
    expiration_duration_secs = COOKIE_DURATION_DAYS * 24 * 60 * 60
    expired_sessions = []

    for id_, session in user_sessions.sessions():
        d = session.session_duration_sec()
        if d > expiration_duration_secs:
            log.info(f'Cleaning up session {id_}')
            expired_sessions.append(id_)

    for id_ in expired_sessions:
        user_sessions.remove(id_)


def start_server(
        local_port,
        websocket_address,
        create_zyn_connection_callback,
        logger,
        debug_tornado=False,

):
    global server_address
    global create_zyn_connection
    global log

    server_address = websocket_address
    create_zyn_connection = create_zyn_connection_callback
    log = logger

    timer = tornado.ioloop.PeriodicCallback(
        _timer_callback,
        1000 * 60 * 60,
    )

    app = tornado.web.Application(
        [
            (r'/raw/(.*)', RawHandler),
            (r'/fs(.*)', MainHandler),
            (r'/login(.*)', LoginHandler),
            (r'/logout(.*)', LogoutHandler),
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

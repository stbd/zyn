import argparse
import base64
import json
import logging
import os
import os.path
import uuid

import tornado.log
import tornado.web
import tornado.websocket

import zyn_util.connection
import zyn_util.util


PATH_STATIC_FILES = os.path.dirname(os.path.abspath(__file__)) + '/web-static-files'
PATH_TEMPLATES = os.path.dirname(os.path.abspath(__file__)) + '/web-templates'
COOKIE_NAME = 'zyn-cookie'


class ConnectionContainer:
    def __init__(self):
        self._connections = {}

    def find_connection(self, user_id):
        try:
            return self._connections[user_id]
        except KeyError:
            return None

    def add_connection(self, connection):
        id = uuid.uuid4().int
        self._connections[id] = connection
        return id


class Connection:
    def __init__(self, zyn_connection):
        self._zyn_connection = zyn_connection
        self._web_sockets = {}
        self._ids = 0

    def add_web_socket(self, socket):
        self._ids += 1
        id = self._ids
        if id in self._web_sockets:
            raise RuntimeError()
        self._web_sockets[id] = socket
        return id

    def remote_web_socket(self, id):
        try:
            del self._web_sockets[id]
        except KeyError:
            pass

    def zyn_connection(self):
        return self._zyn_connection


class WebSocket(tornado.websocket.WebSocketHandler):
    def open(self):
        self._connection = None
        self._tab_id = 0
        self._log = logging.getLogger(__name__)
        self._log.info("New websocket connected")

    def on_message(self, message):
        msg = json.loads(message)

        # print (message)

        msg_type = msg['type']
        user_id = int(msg['user-id'])
        tab_id = int(msg['tab-id'])

        if tab_id != self._tab_id:
            self._log.error("Closing socket: tab_ids do not match")
            self._close_socket()

        if msg_type == 'register':
            self._connection = connections.find_connection(user_id)
            self._tab_id = self._connection.add_web_socket(self)

            self.write_message(json.dumps({
                'type': 'register-rsp',
                'user-id': user_id,
                'tab-id': self._tab_id,
            }))

            self._log.info("Registered, tab_id=%d" % self._tab_id)

        elif msg_type == 'list-files':

            path = msg['content']['path']

            files = []
            rsp = self._connection.zyn_connection().query_list(path=path)

            if not rsp.is_error():
                rsp = rsp.as_query_list_rsp()
                for element in rsp.elements:
                    files.append({
                        'name': element.name,
                        'node-id': element.node_id,
                    })

            self.write_message(json.dumps({
                'type': msg_type + '-rsp',
                'user-id': user_id,
                'tab-id': self._tab_id,
                'files': files,
            }))

        elif msg_type == 'load-file':

            node_id = msg['content']['node-id']

            content = b''
            open_rsp = None
            try:
                rsp = self._connection.zyn_connection().open_file_read(node_id=node_id)
                if not rsp.is_error():
                    open_rsp = rsp.as_open_rsp()
                    if open_rsp.size > 0:
                        rsp, data = self._connection.zyn_connection().read_file(
                            node_id,
                            0,
                            open_rsp.size
                        )
                        if not rsp.is_error():
                            content = str(base64.b64encode(data), 'ascii')
            finally:
                if open_rsp is not None:
                    rsp = self._connection.zyn_connection().close_file(node_id=node_id)

            self.write_message(json.dumps({
                'type': msg_type + '-rsp',
                'user-id': user_id,
                'tab-id': self._tab_id,
                'content': content,
            }))

        else:
            self._log.error("Closing socket: unexpected message")
            self._close_socket()

    def _close_socket(self):
        self._log.info("Closing: tab_id=%i" % self._tab_id)
        self._connection.remote_web_socket(self._tab_id)
        self.close()

    def on_close(self):
        self._close_socket()


class MainHandler(tornado.web.RequestHandler):
    def post(self, path_file):
        username = self.get_body_argument("username")
        password = self.get_body_argument("password")

        log = logging.getLogger(__name__)
        log.info('Login, username="%s", path_file="%s"' % (username, path_file))

        global connection_factory
        zyn_connection = connection_factory.create_connection_and_connect()
        rsp = zyn_connection.authenticate(username, password)

        if rsp.is_error():
            log.info('Failed to login, username="%s", error="%d"' % (username, rsp.error_code()))
            return

        user_id = connections.add_connection(Connection(zyn_connection))

        log.info('Login successful, username="%s"' % username)

        self.set_secure_cookie(COOKIE_NAME, str(user_id))
        if len(path_file) > 0:
            self.redirect(path_file)
        else:
            self.redirect('/')

    def get(self, path_file):
        global connections
        is_logged_in = False
        user_id = 0

        cookie_user_id = None
        try:
            cookie_user_id = int(self.get_secure_cookie(COOKIE_NAME))
        except ValueError:
            pass
        except TypeError:
            pass

        if cookie_user_id is not None:
            connection = connections.find_connection(cookie_user_id)
            if connection is not None:
                user_id = cookie_user_id
                is_logged_in = True

        self.render(
            "files.html",
            is_logged_in=int(is_logged_in),
            user_id=str(user_id),
        )


class ZynConnectionFactory:
    def __init__(
            self,
            path_key,
            path_cert,
            server_ip,
            server_port,
            remote_hostname=None,
            debug_protocol=False,
    ):
        self._path_key = path_key
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
        connection.connect(
            self._server_ip,
            self._server_port,
            self._remote_hostname,
        )
        return connection


def run_tornado():
    parser = argparse.ArgumentParser()
    parser.add_argument('local-port', type=int, default=8080, help='')
    parser.add_argument('zyn-server-ip', help='')
    parser.add_argument('zyn-server-port', help='', type=int)
    parser.add_argument('zyn-server-path-to-key', help='')
    parser.add_argument('zyn-server-path-to-cert', help='')

    parser.add_argument('--debug-protocol', action='store_true', help='')
    parser.add_argument('--verbose', '-v', action='count', default=0)
    parser.add_argument('--remote-hostname', default=None)

    args = vars(parser.parse_args())

    global connection_factory
    connection_factory = ZynConnectionFactory(
        args['zyn-server-path-to-key'],
        args['zyn-server-path-to-cert'],
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

    app = tornado.web.Application(
        [
            (r'/static/(.*)', tornado.web.StaticFileHandler, {"path": PATH_STATIC_FILES}),
            (r'/websocket', WebSocket),
            (r'/(.*)', MainHandler),
        ],
        cookie_secret=base64.b64encode(os.urandom(50)).decode('utf8'),
        static_path=PATH_STATIC_FILES,
        template_path=PATH_TEMPLATES,
        debug=True,
    )

    tornado.log.enable_pretty_logging()
    zyn_util.util.verbose_count_to_log_level(args['verbose'])

    global connections
    connections = ConnectionContainer()

    app.listen(args['local-port'])
    tornado.ioloop.IOLoop.current().start()


if __name__ == '__main__':
    run_tornado()

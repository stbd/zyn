import argparse
import base64
import json
import logging
import os
import os.path
import ssl
import uuid

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
    def __init__(self, zyn_connection, username, password):
        self._zyn_connection = zyn_connection
        self._username = username
        self._password = password
        self._web_sockets = {}
        self._ids = 0

    def add_web_socket(self, socket):
        if self._zyn_connection is None:
            self.reconnect()

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

        if len(self._web_sockets) == 0:
            self._zyn_connection = None

    def zyn_connection(self):
        return self._zyn_connection

    def reconnect(self):
        global connection_factory
        self._zyn_connection = connection_factory.create_connection_and_connect()
        rsp = self._zyn_connection.authenticate(self._username, self._password)
        if rsp.is_error():
            raise ValueError('Failed to login after reconnect as {}'.format(self._username))

    def check_for_notifications(self):
        while self._zyn_connection.check_for_notifications():
            notification = self._zyn_connection.pop_notification()
            for socket in self._web_sockets.values():
                socket.handle_notification(notification)


class OpenFile:
    def __init__(self):
        self.clear()

    def clear(self):
        self.node_id = None
        self.file_type = None
        self.content = None
        self.open_mode = None

    def is_set(self):
        return self.node_id is not None

    def is_random_access(self):
        return self.file_type == zyn_util.connection.FILE_TYPE_RANDOM_ACCESS

    def is_blob(self):
        return self.file_type == zyn_util.connection.FILE_TYPE_BLOB

    def is_mode_read(self):
        return self.open_mode == OPEN_MODE_READ

    def is_mode_write(self):
        return self.open_mode == OPEN_MODE_WRITE

    def update_content(self, content, revision):
        self.content = content
        # todo: use revision

    def reset(self, node_id, file_type, open_mode, content):
        self.node_id = node_id
        self.file_type = file_type
        self.open_mode = open_mode

        if self.is_random_access():
            self.content = content
        else:
            self.content = None


class WebSocket(tornado.websocket.WebSocketHandler):
    def open(self):
        self._connection = None
        self._tab_id = 0
        self._user_id = None
        self._log = logging.getLogger(__name__)
        self._log.info("New websocket connected")
        self._open_file = OpenFile()

    def _close_socket(self):
        self._log.info("Closing: tab_id=%i" % self._tab_id)
        self._close_current_file()
        self._connection.remote_web_socket(self._tab_id)
        self.close()

    def on_close(self):
        self._close_socket()

    def on_message(self, message):
        msg = json.loads(message)

        # print (message)

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
                self._connection.reconnect()
                self._send_notification(
                    NOTIFICATION_SOURCE_WEB_SERVER,
                    'reconnect',
                    {'trial': trial}
                )
            trial += 1

    def handle_notification(self, notification):
        self._send_server_notification(notification)

    def _handle_message(self, msg_type, user_id, content):

        if msg_type == 'log':

            level = content['level']
            msg = content['message']
            msg = 'Browser, user_id={}: {}'.format(user_id, msg)

            if level == 'debug':
                self._log.debug(msg)
            elif level == 'info':
                self._log.info(msg)
            else:
                raise RuntimeError()

        elif msg_type == 'poll':

            self._connection.check_for_notifications()

        elif msg_type == 'register':

            self._log.debug('Register, user_id={}'.format(user_id))
            self._user_id = user_id
            self._connection = connections.find_connection(self._user_id)
            self._tab_id = self._connection.add_web_socket(self)
            self._send_response(msg_type)
            self._log.info("Registered, user_id={}, tab_id={}".format(self._user_id, self._tab_id))

        elif msg_type == 'create':

            element_type = content['type']
            element_name = content['name']
            parent = content['parent']
            self._log.debug('Create, type="{}", parent="{}"'.format(element_type, parent))

            if element_type == ELEMENT_TYPE_FILE:
                file_type = content['file-type']
                if file_type == FILE_TYPE_RANDOM_ACCESS:
                    rsp = self._connection.zyn_connection().create_file_random_access(
                        element_name,
                        parent_path=parent
                    )
                else:
                    raise NotImplementedError()
            elif element_type == ELEMENT_TYPE_DIRECTORY:
                rsp = self._connection.zyn_connection().create_folder(
                    element_name,
                    parent_path=parent
                )
            else:
                raise RuntimeError(element_type)

            self._send_response(msg_type, {})

        elif msg_type == 'delete':

            node_id = content['node-id']
            self._log.debug('Delete, node-id={}'.format(node_id))
            rsp = self._connection.zyn_connection().delete(
                node_id=node_id
            )
            self._send_response(msg_type, {})

        elif msg_type == 'query-filesystem-element':

            path = content['path']
            self._log.debug('{}: path={}'.format(msg_type, path))
            rsp = self._connection.zyn_connection().query_filesystem(path=path)

            desc = {}
            if not rsp.is_error():
                rsp = rsp.as_query_filesystem_rsp()
                desc['node-id'] = rsp.node_id
                desc['write-access'] = rsp.write_access
                desc['read-access'] = rsp.read_access
                desc['created'] = rsp.created
                desc['modified'] = rsp.modified
                if rsp.is_file():
                    desc['type-of-element'] = 'file'
                elif rsp.is_directory():
                    desc['type-of-element'] = 'directory'
                else:
                    raise RuntimeError()

            self._send_response(msg_type, {
                'path': path,
                'description': desc
            })

        elif msg_type == 'list-directory-content':

            path = content['path']
            self._log.debug('{}: path={}'.format(msg_type, path))

            rsp = self._connection.zyn_connection().query_list(path=path)
            if rsp.is_error():
                self._send_error_response(msg_type, None, rsp.error_code())
                return

            elements = []
            rsp = rsp.as_query_list_rsp()
            for element in rsp.elements:

                if element.is_file():
                    element_type = 'file'
                elif element.is_directory():
                    element_type = 'dir'
                else:
                    raise RuntimeError()

                elements.append({
                    'name': element.name,
                    'node-id': element.node_id,
                    'element-type': element_type,
                })

            self._send_response(msg_type, {
                'elements': elements,
            })

        elif msg_type == 'change-open-file-mode':

            node_id = content['node-id']
            mode = content['open-mode']

            self._log.debug('{}: node_id={}, mode={}, open_file.open_mode={}'.format(
                msg_type,
                node_id,
                mode,
                self._open_file.open_mode
            ))

            if not self._open_file.is_set():
                self._send_response(msg_type, {})
                return

            if mode == self._open_file.open_mode:
                self._send_response(msg_type, {})
                return

            # todo: Update to use some method that allows changing mode without closing
            rsp = self._connection.zyn_connection().close_file(node_id=self._open_file.node_id)

            if mode == OPEN_MODE_READ:
                rsp = self._connection.zyn_connection().open_file_read(node_id=node_id)
            elif mode == OPEN_MODE_WRITE:
                rsp = self._connection.zyn_connection().open_file_write(node_id=node_id)
            else:
                raise RuntimeError()

            self._send_response(msg_type, {})

        elif msg_type == 'load-file':

            node_id = content['node-id']
            filename = content['filename']
            mode = content['open-mode']

            self._log.debug('{}: node_id={}, filename="{}", mode={}'.format(
                msg_type, node_id, filename, mode
            ))

            file_content = b''
            file_type = ''
            open_rsp = None

            if mode == OPEN_MODE_READ:
                rsp = self._connection.zyn_connection().open_file_read(node_id=node_id)
            elif mode == OPEN_MODE_WRITE:
                rsp = self._connection.zyn_connection().open_file_write(node_id=node_id)
            else:
                raise RuntimeError()

            if rsp.is_error():
                self._send_error_response(msg_type, None, rsp.error_code())
                return

            open_rsp = rsp.as_open_rsp()
            if open_rsp.is_random_access():
                file_type = FILE_TYPE_RANDOM_ACCESS
            elif open_rsp.is_blob():
                file_type = FILE_TYPE_BLOB
            else:
                raise RuntimeError()

            if open_rsp.size > 0:
                rsp, data = self._connection.zyn_connection().read_file(
                    node_id,
                    0,
                    open_rsp.size
                )
                if rsp.is_error():
                    self._send_error_response(msg_type, None, rsp.error_code())
                    return
                file_content = data

            self._close_current_file()
            self._open_file.reset(node_id, open_rsp.type_of_file, mode, file_content)

            self._log.debug('{}: loaded {} bytes, node_id={}'.format(
                msg_type, len(file_content), node_id))

            self._send_response(msg_type, {
                'node-id': node_id,
                'revision': open_rsp.revision,
                'filename': filename,
                'file-type': file_type,
                'bytes': str(base64.b64encode(file_content), 'ascii'),
            })

        elif msg_type == 'edit-file':

            node_id = content['node-id']
            content_edited = base64.b64decode(content['content-edited'])
            node_id = content['node-id']
            file_type = content['type-of-file']
            revision = content['revision']

            self._log.debug('{}: node_id={}, revision={}'.format(msg_type, node_id, revision))

            if node_id != self._open_file.node_id:
                raise RuntimeError()

            if file_type == FILE_TYPE_RANDOM_ACCESS:
                if not self._open_file.is_random_access():
                    raise RuntimeError()

                revision = zyn_util.util.edit_random_access_file(
                    self._connection.zyn_connection(),
                    node_id,
                    revision,
                    self._open_file.content,
                    content_edited,
                    self._log
                )
                self._open_file.update_content(content_edited, revision)
            elif file_type == FILE_TYPE_BLOB:
                rsp = self._connection.zyn_connection().blob_write(
                    node_id,
                    revision,
                    content_edited,
                )
                if not rsp.is_error():
                    revision = rsp.as_insert_rsp().revision
            else:
                raise RuntimeError()

            self._send_response(msg_type, {
                'node-id': node_id,
                'revision': revision,
            },
            )

        else:
            self._log.error("Closing socket: unexpected message: {}".format(msg_type))
            self._close_socket()

    def _close_current_file(self):
        if self._open_file.is_set():
            self._log.info("Closing currently active file {}".format(self._open_file.node_id))
            self._connection.zyn_connection().close_file(node_id=self._open_file.node_id)
            self._open_file.clear()

    def _message_headers(self, msg_type):
        return {
            'type': msg_type,
            'user-id': self._user_id,
            'tab-id': self._tab_id,
        }

    def _send_error_response(self, msg_type, web_server_error_str, zyn_server_error_code):
        msg = self._message_headers(msg_type + '-rsp')
        if web_server_error_str is None:
            web_server_error_str = ''

        if zyn_server_error_code is not None:
            zyn_server_error_str = zyn_util.errors.error_to_string(zyn_server_error_code)
        else:
            zyn_server_error_str = ''

        msg['error'] = {
            'web-server-error': web_server_error_str,
            'zyn-server-error': zyn_server_error_str,
        }
        self.write_message(json.dumps(msg))

    def _send_response(self, msg_type, content=None):
        if content is None:
            content = ''

        msg = self._message_headers(msg_type + '-rsp')
        msg['content'] = content
        self.write_message(json.dumps(msg))

    def _send_server_notification(self, notification):
        if notification.notification_type() == zyn_util.connection.Notification.TYPE_DISCONNECTED:
            self._send_notification(
                NOTIFICATION_SOURCE_ZYN_SERVER,
                'disconnected',
                {
                    'reason': notification.reason,
                })
        elif notification.notification_type() in [
                    zyn_util.connection.Notification.TYPE_MODIFIED,
                    zyn_util.connection.Notification.TYPE_DELETED,
                    zyn_util.connection.Notification.TYPE_INSERTED,
        ]:
            if notification.node_id != self._open_file.node_id:
                self._log.info('Discarding notification for not open file, node_id={}'.format(
                    notification.node_id
                ))
                return
            if not self._open_file.is_random_access():
                self._send_notification(
                    NOTIFICATION_SOURCE_ZYN_SERVER,
                    'blob-modified',
                    {
                        'node-id': notification.node_id,
                        'revision': notification.revision,
                    })
                return

            if notification.notification_type() == zyn_util.connection.Notification.TYPE_MODIFIED:
                rsp, bytes = self._connection.zyn_connection().read_file(
                    self._open_file.node_id,
                    notification.block_offset,
                    notification.block_size
                )
                content = self._open_file.content
                content = \
                    content[0:notification.block_offset] \
                    + bytes \
                    + content[notification.block_offset + notification.block_size:]

                self._open_file.update_content(content, notification.revision)
                self._send_notification(
                    NOTIFICATION_SOURCE_ZYN_SERVER,
                    'random-access-modified',
                    {
                        'node-id': notification.node_id,
                        'revision': notification.revision,
                        'offset': notification.block_offset,
                        'bytes': str(base64.b64encode(bytes), 'ascii'),
                    })

            elif notification.notification_type() == zyn_util.connection.Notification.TYPE_DELETED:
                content = self._open_file.content
                content = \
                    content[0:notification.block_offset] \
                    + content[notification.block_offset + notification.block_size:]

                self._open_file.update_content(content, notification.revision)
                self._send_notification(
                    NOTIFICATION_SOURCE_ZYN_SERVER,
                    'random-access-deleted',
                    {
                        'node-id': notification.node_id,
                        'revision': notification.revision,
                        'offset': notification.block_offset,
                        'size': notification.block_size,
                    })

            elif notification.notification_type() == zyn_util.connection.Notification.TYPE_INSERTED:
                rsp, bytes = self._connection.zyn_connection().read_file(
                    self._open_file.node_id,
                    notification.block_offset,
                    notification.block_size
                )
                content = self._open_file.content
                content = \
                    content[0:notification.block_offset] \
                    + bytes \
                    + content[notification.block_offset:]

                self._open_file.update_content(content, notification.revision)
                self._send_notification(
                    NOTIFICATION_SOURCE_ZYN_SERVER,
                    'random-access-inserted',
                    {
                        'node-id': notification.node_id,
                        'revision': notification.revision,
                        'offset': notification.block_offset,
                        'size': notification.block_size,
                        'bytes': str(base64.b64encode(bytes), 'ascii'),
                    })
            else:
                raise NotImplementedError()
        else:
            raise NotImplementedError()

    def _send_notification(self, source, notification_type, content):
        msg = self._message_headers('notification')
        msg['notification'] = {
            'source': source,
            'type': notification_type,
            'content': content,
        }
        self.write_message(json.dumps(msg))


class MainHandler(tornado.web.RequestHandler):
    def post(self, path_file):
        username = self.get_body_argument("username")
        password = self.get_body_argument("password")

        if len(path_file) == 0:
            path_file = '/'

        log = logging.getLogger(__name__)
        log.info('Login, username="%s", path_file="%s"' % (username, path_file))

        global connection_factory
        zyn_connection = connection_factory.create_connection_and_connect()
        rsp = zyn_connection.authenticate(username, password)

        if not rsp.is_error():
            user_id = connections.add_connection(Connection(zyn_connection, username, password))
            log.info('Login successful, username="%s"' % username)
            self.set_secure_cookie(COOKIE_NAME, str(user_id))
        else:
            log.info('Failed to login, username="%s", error="%d"' % (username, rsp.error_code()))

        self.redirect(path_file)

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

    app = tornado.web.Application(
        [
            (r'/static/(.*)', tornado.web.StaticFileHandler, {"path": PATH_STATIC_FILES}),
            (r'/websocket', WebSocket),
            (r'/(.*)', MainHandler),
        ],
        cookie_secret=base64.b64encode(os.urandom(50)).decode('utf8'),
        static_path=PATH_STATIC_FILES,
        template_path=PATH_TEMPLATES,
        debug=args['debug_tornado'],
    )

    tornado.log.enable_pretty_logging()
    zyn_util.util.verbose_count_to_log_level(args['verbose'])

    global connections
    connections = ConnectionContainer()

    app.listen(args['local-port'], ssl_options=ssl_context)
    tornado.ioloop.IOLoop.current().start()


if __name__ == '__main__':
    main()

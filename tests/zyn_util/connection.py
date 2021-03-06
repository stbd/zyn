import logging
import socket
import ssl
import time
import os

import certifi

import zyn_util.exception


FILESYSTEM_ELEMENT_FILE = 0
FILESYSTEM_ELEMENT_DIRECTORY = 1
FILE_TYPE_RANDOM_ACCESS = 0
FILE_TYPE_BLOB = 1
TYPE_USER = 0
TYPE_GROUP = 1
EXPIRATION_NEVER_EXPIRE = 0
BATCH_EDIT_TYPE_DELETE = 1
BATCH_EDIT_TYPE_INSERT = 2
BATCH_EDIT_TYPE_WRITE = 3


class RandomAccessBatchEdit:
    def __init__(self, connection, node_id, revision, transaction_id):
        self.connection = connection
        self.node_id = node_id
        self.revision = revision
        self.transaction_id = transaction_id
        self.operations = []

    def number_of_operations(self):
        return len(self.operations)

    def delete(self, offset, size):
        self.operations.append((BATCH_EDIT_TYPE_DELETE, offset, size))

    def insert(self, offset, data):
        self.operations.append((BATCH_EDIT_TYPE_INSERT, offset, data))

    def write(self, offset, data):
        self.operations.append((BATCH_EDIT_TYPE_WRITE, offset, data))

    def commit(self):
        return self.connection._commit_ra_batch(self)


class ZynSocket:
    def __init__(self, context, socket, ssl_socket):
        self._context = context
        self._socket_ = socket
        self._socket = ssl_socket

    def _create(context, remote_address, remote_port, remote_hostname=None):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((remote_address, remote_port))
        ssl = context.wrap_socket(
            s,
            server_hostname=remote_hostname or remote_address,
        )
        return ZynSocket(context, socket, ssl)

    def create_with_custom_cert(remote_address, remote_port, path_cert, remote_hostname=None):
        context = ssl.create_default_context()
        context.load_verify_locations(path_cert)
        return ZynSocket._create(context, remote_address, remote_port, remote_hostname)

    def create(remote_address, remote_port):
        context = ssl.create_default_context()
        context.load_verify_locations(certifi.where())
        return ZynSocket._create(context, remote_address, remote_port)

    def settimeout(self, timeout):
        return self._socket.settimeout(timeout)

    def recv(self, size=None):
        if size is None:
            return self._socket.recv()
        else:
            return self._socket.recv(size)

    def sendall(self, data):
        return self._socket.sendall(data)

    def close(self):
        self._socket.shutdown(socket.SHUT_WR)
        self._socket.close()


class ZynConnection:

    def __init__(self, zyn_socket, debug_messages=False):
        self._socket = zyn_socket
        self._log = logging.getLogger(__name__)
        self._transaction_id = 1
        self._debug_messages = debug_messages
        self._input_buffer = b''
        self._notifications = []

    def disconnect(self):
        self._socket.close()

    def enable_debug_messages(self):
        self._debug_messages = True

    def _send_receive(self, req):
        self.write(req)
        return self.read_response()

    def _read_notification(self, timeout=0):
        msg = self.read_message(timeout=timeout)
        if msg is not None:
            if msg.type() != Message.NOTIFICATION:
                raise RuntimeError('Server sent an unexpected response')
            self._notifications.append(msg)
            return True
        return False

    def check_for_notifications(self, timeout=0):
        if self._notifications:
            return True
        return self._read_notification(timeout=timeout)

    def pop_notification(self, timeout=0):
        if self._notifications:
            return self._notifications.pop(0)
        if self._read_notification(timeout):
            return self._notifications.pop(0)
        return None

    def authenticate(self, username, password, transaction_id=None):
        req = \
            self.field_version() \
            + 'A:' \
            + self.field_transaction_id(transaction_id or self._consume_transaction_id()) \
            + 'L:' \
            + self.field_string(username) \
            + self.field_string(password) \
            + ';' \
            + ';' \
            + self.field_end_of_message() \

        return self._send_receive(req)

    def authenticate_with_token(self, token, transaction_id=None):
        req = \
            self.field_version() \
            + 'A:' \
            + self.field_transaction_id(transaction_id or self._consume_transaction_id()) \
            + 'TOKEN:' \
            + self.field_string(token) \
            + ';' \
            + ';' \
            + self.field_end_of_message() \

        return self._send_receive(req)

    def allocate_authentication_token(self, transaction_id=None):
        req = \
            self.field_version() \
            + 'ALLOCATE-AUTH-TOKEN:' \
            + self.field_transaction_id(transaction_id or self._consume_transaction_id()) \
            + ';' \
            + self.field_end_of_message() \

        return self._send_receive(req)

    def create_file(
            self,
            name,
            file_type=None,
            parent_node_id=None,
            parent_path=None,
            block_size=None,
            transaction_id=None
    ):
        parent = self.file_descriptor(parent_node_id, parent_path)
        req = \
            self.field_version() \
            + 'CREATE-FILE:' \
            + self.field_transaction_id(transaction_id or self._consume_transaction_id()) \
            + parent \
            + self.field_string(name) \
            + self.field_unsigned(file_type)

        if block_size is not None:
            req += self.field_unsigned(block_size)

        req = \
            req \
            + ';' \
            + self.field_end_of_message() \

        return self._send_receive(req)

    def create_file_random_access(
            self,
            name,
            parent_node_id=None,
            parent_path=None,
            block_size=None,
            transaction_id=None
    ):
        return self.create_file(
            name,
            FILE_TYPE_RANDOM_ACCESS,
            parent_node_id,
            parent_path,
            block_size,
            transaction_id
        )

    def create_file_blob(
            self,
            name,
            parent_node_id=None,
            parent_path=None,
            block_size=None,
            transaction_id=None
    ):
        return self.create_file(
            name,
            FILE_TYPE_BLOB,
            parent_node_id,
            parent_path,
            block_size,
            transaction_id
        )

    def create_directory(self, name, parent_node_id=None, parent_path=None, transaction_id=None):
        parent = self.file_descriptor(parent_node_id, parent_path)
        req = \
            self.field_version() \
            + 'CREATE-DIRECTORY:' \
            + self.field_transaction_id(transaction_id or self._consume_transaction_id()) \
            + parent \
            + self.field_string(name) \
            + ';' \
            + self.field_end_of_message() \

        return self._send_receive(req)

    def file_descriptor(self, node_id=None, path=None):
        if node_id is not None and path is None:
            return self.field_file_descriptor(self.field_node_id(node_id))
        elif node_id is None and path is not None:
            return self.field_file_descriptor(self.field_path(path))
        else:
            raise RuntimeError('File descriptor needs either node_id or path')

    def delete(self, node_id=None, path=None, transaction_id=None):
        req = \
            self.field_version() \
            + 'DELETE:' \
            + self.field_transaction_id(transaction_id or self._consume_transaction_id()) \
            + self.file_descriptor(node_id, path) \
            + ';' \
            + self.field_end_of_message() \

        return self._send_receive(req)

    def file_open(self, mode, node_id=None, path=None, transaction_id=None):

        req = \
            self.field_version() \
            + 'O:' \
            + self.field_transaction_id(transaction_id or self._consume_transaction_id()) \
            + self.file_descriptor(node_id, path) \
            + self.field_unsigned(mode) \
            + ';' \
            + self.field_end_of_message() \

        return self._send_receive(req)

    def open_file_read(self, node_id=None, path=None, transaction_id=None):
        return self.file_open(0, node_id, path, transaction_id)

    def open_file_write(self, node_id=None, path=None, transaction_id=None):
        return self.file_open(1, node_id, path, transaction_id)

    def close_file(self, node_id=None, transaction_id=None):

        req = \
            self.field_version() \
            + 'CLOSE:' \
            + self.field_transaction_id(transaction_id or self._consume_transaction_id()) \
            + self.field_node_id(node_id) \
            + ';' \
            + self.field_end_of_message() \

        return self._send_receive(req)

    def blob_write(self, node_id, revision, data, block_size=None, transaction_id=None):
        # todo: refactor to use stream version
        if block_size is None:
            block_size = len(data)

        req = \
            self.field_version() \
            + 'BLOB-W:' \
            + self.field_transaction_id(transaction_id or self._consume_transaction_id()) \
            + self.field_node_id(node_id) \
            + self.field_unsigned(revision) \
            + self.field_unsigned(len(data)) \
            + self.field_unsigned(block_size) \
            + ';' \
            + self.field_end_of_message() \

        rsp = self._send_receive(req)
        if rsp.is_error():
            return rsp

        index_start = 0
        self._socket.settimeout(60)
        while index_start < (len(data) - 1):
            index_end = index_start + block_size
            self._socket.sendall(data[index_start:index_end])
            index_start += block_size
            rsp = self.read_response(timeout=60*5)
            if rsp.is_error():
                return rsp

        return self.read_response()

    def ra_batch_edit(self, node_id, revision, transaction_id=None):
        return RandomAccessBatchEdit(
            self,
            node_id,
            revision,
            transaction_id,
        )

    def _commit_ra_batch(self, batch):

        req = \
            self.field_version() \
            + 'RA-BATCH-EDIT:' \
            + self.field_transaction_id(batch.transaction_id or self._consume_transaction_id()) \
            + self.field_node_id(batch.node_id) \
            + self.field_unsigned(batch.revision) \
            + self.field_unsigned(batch.number_of_operations()) \
            + ';' \
            + self.field_end_of_message() \

        rsp = self._send_receive(req)
        if rsp.is_error():
            return rsp

        for operation_type, offset, param in batch.operations:
            if operation_type == BATCH_EDIT_TYPE_DELETE:
                req = \
                    self.field_unsigned(operation_type) \
                    + self.field_block(offset, param) \
                    + self.field_end_of_message()
                self.write(req)

            elif operation_type == BATCH_EDIT_TYPE_INSERT:
                data = param
                req = \
                    self.field_unsigned(operation_type) \
                    + self.field_block(offset, len(data)) \
                    + self.field_end_of_message()

                self.write(req)
                self._socket.sendall(data)

            elif operation_type == BATCH_EDIT_TYPE_WRITE:
                data = param
                req = \
                    self.field_unsigned(operation_type) \
                    + self.field_block(offset, len(data)) \
                    + self.field_end_of_message()

                self.write(req)
                self._socket.sendall(data)

            else:
                raise RuntimeError()

            rsp = self.read_message(timeout=120)
            if rsp is not None:
                if rsp.is_error():
                    return rsp
            else:
                raise RuntimeError()

        return rsp  # Return latest rsp

    def blob_write_stream(self, node_id, revision, stream, block_size=None, transaction_id=None):
        # If block size is not set, try to use data length as size
        # also if block size is larger than actual data, use data length
        if block_size is None or block_size > stream.size():
            block_size = stream.size()

        size = stream.size()
        req = \
            self.field_version() \
            + 'BLOB-W:' \
            + self.field_transaction_id(transaction_id or self._consume_transaction_id()) \
            + self.field_node_id(node_id) \
            + self.field_unsigned(revision) \
            + self.field_unsigned(size) \
            + self.field_unsigned(block_size) \
            + ';' \
            + self.field_end_of_message() \

        rsp = self._send_receive(req)
        if rsp.is_error():
            return rsp

        bytes_send = 0
        self._socket.settimeout(60)
        while True:
            block = stream.get(block_size)
            if block is None:
                break
            self._socket.sendall(block)
            bytes_send += len(block)
            rsp = self.read_response(timeout=60*5)
            if rsp.is_error():
                return rsp

        if bytes_send != size:
            raise RuntimeError('Sent bytes does not match the size of ')
        return self.read_response(timeout=60*5)

    def ra_write(self, node_id, revision, offset, data, transaction_id=None):
        req = \
            self.field_version() \
            + 'RA-W:' \
            + self.field_transaction_id(transaction_id or self._consume_transaction_id()) \
            + self.field_node_id(node_id) \
            + self.field_unsigned(revision) \
            + self.field_block(offset, len(data)) \
            + ';' \
            + self.field_end_of_message() \

        rsp = self._send_receive(req)
        if rsp.is_error():
            return rsp
        self._socket.sendall(data)
        return self.read_response()

    def ra_insert(self, node_id, revision, offset, data, transaction_id=None):
        req = \
            self.field_version() \
            + 'RA-I:' \
            + self.field_transaction_id(transaction_id or self._consume_transaction_id()) \
            + self.field_node_id(node_id) \
            + self.field_unsigned(revision) \
            + self.field_block(offset, len(data)) \
            + ';' \
            + self.field_end_of_message() \

        rsp = self._send_receive(req)
        if rsp.is_error():
            return rsp
        self._socket.sendall(data)
        return self.read_response()

    def ra_delete(self, node_id, revision, offset, size, transaction_id=None):
        req = \
            self.field_version() \
            + 'RA-D:' \
            + self.field_transaction_id(transaction_id or self._consume_transaction_id()) \
            + self.field_node_id(node_id) \
            + self.field_unsigned(revision) \
            + self.field_block(offset, size) \
            + ';' \
            + self.field_end_of_message() \

        self.write(req)
        return self.read_response()

    def read_file(self, node_id, offset, size, transaction_id=None):
        if size == 0:
            return

        req = \
            self.field_version() \
            + 'R:' \
            + self.field_transaction_id(transaction_id or self._consume_transaction_id()) \
            + self.field_node_id(node_id) \
            + self.field_block(offset, size) \
            + ';' \
            + self.field_end_of_message() \

        self.write(req)
        rsp = self.read_response()
        if rsp.is_error():
            return rsp, None

        _, read_size = rsp.field(1).as_block()
        data = bytearray()
        if read_size > 0:
            data = self.read_data(read_size)
        return rsp, data

    def read_file_stream(self, node_id, offset, size, block_size, stream):
        offset_start = offset
        offset_block_start = offset_start
        offset_end = offset_start + size
        while True:
            if offset_block_start >= offset_end:
                break
            bytes_remaining = offset_end - offset_block_start
            block_size = min(block_size, bytes_remaining)
            rsp, d = self.read_file(
                node_id,
                offset_block_start,
                block_size,
                stream.transaction_id(),
            )
            if rsp.is_error():
                stream.handle_error(rsp)
                break
            stream.handle_data(offset_block_start, d)
            offset_block_start += len(d)

    def query_fs_children(self, node_id=None, path=None, transaction_id=None):
        req = \
            self.field_version() \
            + 'Q-FS-C:' \
            + self.field_transaction_id(transaction_id or self._consume_transaction_id()) \
            + self.file_descriptor(node_id, path) \
            + ';' \
            + self.field_end_of_message() \

        self.write(req)
        rsp = self.read_response()
        return rsp

    def query_fs_element_properties(
            self,
            node_id=None,
            path=None,
            parent_node_id=None,
            parent_path=None,
            transaction_id=None):
        req = \
            self.field_version() \
            + 'Q-FS-P:' \
            + self.field_transaction_id(transaction_id or self._consume_transaction_id()) \
            + self.file_descriptor(node_id, path) \
            + self.file_descriptor(parent_node_id, parent_path) \
            + ';' \
            + self.field_end_of_message() \

        self.write(req)
        rsp = self.read_response()
        return rsp

    def query_fs_element(self, node_id=None, path=None, transaction_id=None):
        req = \
            self.field_version() \
            + 'Q-FS-E:' \
            + self.field_transaction_id(transaction_id or self._consume_transaction_id()) \
            + self.file_descriptor(node_id, path) \
            + ';' \
            + self.field_end_of_message() \

        self.write(req)
        rsp = self.read_response()
        return rsp

    def query_counters(self, transaction_id=None):
        req = \
            self.field_version() \
            + 'Q-COUNTERS:' \
            + self.field_transaction_id(transaction_id or self._consume_transaction_id()) \
            + ';' \
            + self.field_end_of_message() \

        self.write(req)
        rsp = self.read_response()
        return rsp

    def query_system(self, transaction_id=None):
        req = \
            self.field_version() \
            + 'Q-SYSTEM:' \
            + self.field_transaction_id(transaction_id or self._consume_transaction_id()) \
            + ';' \
            + self.field_end_of_message() \

        self.write(req)
        rsp = self.read_response()
        return rsp

    def create_user_group(self, name, type_user_or_group, transaction_id=None):
        req = \
            self.field_version() \
            + 'ADD-USER-GROUP:' \
            + self.field_transaction_id(transaction_id or self._consume_transaction_id()) \
            + self.field_unsigned(type_user_or_group) \
            + self.field_string(name) \
            + ';' \
            + self.field_end_of_message() \

        self.write(req)
        rsp = self.read_response()
        return rsp

    def create_user(self, username, transaction_id=None):
        return self.create_user_group(username, TYPE_USER, transaction_id)

    def create_group(self, group_name, transaction_id=None):
        return self.create_user_group(group_name, TYPE_GROUP, transaction_id)

    def modify_user_group(self, name, type_user_or_group, key_values, transaction_id=None):
        if not key_values:
            raise ValueError('No values modified')

        req = \
            self.field_version() \
            + 'MOD-USER-GROUP:' \
            + self.field_transaction_id(transaction_id or self._consume_transaction_id()) \
            + self.field_unsigned(type_user_or_group) \
            + self.field_string(name) \
            + self.field_list(key_values) \
            + ';' \
            + self.field_end_of_message() \

        self.write(req)
        rsp = self.read_response()
        return rsp

    def modify_user(self, username, password=None, expiration=None, transaction_id=None):

        key_values = []
        if password is not None:
            key_values.append(
                self.field_key_value_pair('password', self.field_string(password))
            )

        if expiration is not None:
            key_values.append(
                self.field_key_value_pair('expiration', self.field_unsigned(expiration))
            )

        if not key_values:
            raise ValueError('No values modified')

        return self.modify_user_group(username, TYPE_USER, key_values, transaction_id)

    def modify_group(self, group_name, expiration=None, transaction_id=None):

        key_values = []
        if expiration is not None:
            key_values.append(
                self.field_key_value_pair('expiration', self.field_unsigned(expiration))
            )

        if not key_values:
            raise ValueError('No values modified')

        return self.modify_user_group(group_name, TYPE_GROUP, key_values, transaction_id)

    def write(self, data):
        if self._debug_messages:
            self._log.debug('Write: {}'.format(data))

        encoded = data.encode('utf-8')
        self._socket.sendall(encoded)

    def read_data(self, length, timeout=None):
        timeout = timeout or 60.
        self._socket.settimeout(timeout)
        buffer = b''
        if len(self._input_buffer) > 0:
            buffer = self._input_buffer[:length]
            self._input_buffer = self._input_buffer[length + 1:]

        while len(buffer) < length:
            bytes_needed = length - len(buffer)
            d = self._socket.recv(bytes_needed)
            if not d:
                time.sleep(.5)
            buffer += d
        return buffer

    def read_response(self, end_of_message_field=None, timeout=None):

        timeout = timeout or 10.
        while True:
            message = self.read_message(end_of_message_field, timeout)
            if message is None:
                raise TimeoutError('No response received from socket on time')
            if message.type() == Message.NOTIFICATION:
                self._notifications.append(message)
                continue
            return message

    def read_message(self, end_of_message_field=None, timeout=None):

        eom = end_of_message_field or self.field_end_of_message()
        eom = eom.encode('utf-8')
        self._socket.settimeout(timeout)
        message = ''

        while True:
            try:
                d = self._socket.recv()
            except ssl.SSLWantReadError:
                d = None
            except socket.timeout:
                d = None

            if d is None:
                return None

            if len(d) == 0:
                raise zyn_util.exception.ZynConnectionLost()

            self._input_buffer += d
            i = self._input_buffer.find(eom)
            if i != -1:
                message = self._input_buffer[:i + len(eom)].decode('utf-8')
                self._input_buffer = self._input_buffer[i + len(eom) + 1:]
                break

        if self._debug_messages:
            self._log.debug('Read message: {}'.format(message))

        parsed = self.parse_message(message)
        if parsed[1][0] == 'NOTIFICATION':
            return Notification.create(parsed)
        return Response(parsed)

    def _parse_transaction_completed(self, msg, expected_error_code, has_error_string=False):
        msg = self._consume_expected(msg, 'V:1;')
        msg = self._consume_expected(msg, 'TC:T:U:')
        msg, transaction_id = self._read_until_delimiter(msg)
        self.assertTrue(isinstance(int(transaction_id), int))
        msg = self._consume_expected(msg, 'U:{};'.format(expected_error_code))

    def transaction_id(self):
        return self._transaction_id

    def _consume_transaction_id(self):
        _id = self._transaction_id
        self._transaction_id += 1
        return _id

    @staticmethod
    def field_end_of_message():
        return 'E:;'

    @staticmethod
    def field_node_id(value):
        return 'N:{};'.format(ZynConnection.field_unsigned(value))

    @staticmethod
    def field_block(offset, size):
        return 'BL:{}{};'.format(
            ZynConnection.field_unsigned(offset),
            ZynConnection.field_unsigned(size)
        )

    @staticmethod
    def field_unsigned(value):
        return 'U:{};'.format(value)

    @staticmethod
    def field_path(content):
        return 'P:' + ZynConnection.field_string(content) + ";"

    @staticmethod
    def field_file_descriptor(content):
        return 'F:' + content + ";"

    @staticmethod
    def field_string(content):
        return 'S:{}B:{};;'.format(
            ZynConnection.field_unsigned(len(content)),
            content,
        )

    @staticmethod
    def field_key_value_pair(key, value_str):
        return 'KVP:{}{};'.format(
            ZynConnection.field_string(key),
            value_str
        )

    @staticmethod
    def field_list(content):
        elements = ''
        for c in content:
            elements += 'LE:{};'.format(c)

        return 'L:{}{};'.format(
            ZynConnection.field_unsigned(len(content)),
            elements
        )

    @staticmethod
    def field_version():
        return 'V:1;'

    @staticmethod
    def field_transaction_id(transaction_id):
        return 'T:U:{};;'.format(transaction_id)

    def parse_message(self, message):
        class Node:
            def __init__(self, parent=None):
                self._parent = parent
                self._children = []
                self._value = None
                self._tag = None

            @staticmethod
            def from_tag(name):
                n = Node()
                n._tag = name
                return n

            @staticmethod
            def from_value(name):
                n = Node()
                n._value = name
                return n

            def from_empty_value():
                n = Node()
                n._value = ""
                return n

            def str(self, spaces=0):
                s = ''
                s += ' ' * spaces
                if self._parent is None:
                    s += 'Root'

                if self._tag is not None:
                    s += 'Tag: "{}"'.format(self._tag)
                if self._value is not None:
                    s += 'Value: "{}"'.format(self._value)

                for c in self._children:
                    s += '\n{}'.format(c.str(spaces + 1))

                return s

            def is_key_value(self):
                return \
                    len(self._children) == 1 \
                    and self._children[0]._value is not None

            def is_list_of_elements(self):
                return \
                    len(self._children) > 0 \
                    and self._children[0]._value is None

            def add_child(self, child):
                child._parent = self
                self._children.append(child)
                if self.is_key_value() and self._tag in ['V', 'U']:
                    self._children[0]._value = int(self._children[0]._value)

            def to_list(self):
                if self._tag:
                    children = []
                    for c in self._children:
                        list_of = c.to_list()
                        if isinstance(list_of, list) and not list_of:
                            continue
                        children.append(list_of)
                    return [self._tag] + children
                elif self._value or isinstance(self._value, int):
                    return self._value
                else:
                    children = [c.to_list() for c in self._children]
                    return children

        def _parse_part(message, end_of_tag):
            part = message[0:end_of_tag]
            message = message[end_of_tag + 1:]
            return message, part

        def _parse(message, parent):

            end_of_tag = message.find(':')
            end_of_value = message.find(';')

            # Special handling for strings, as content may include
            # delimiter characters
            if parent._tag == 'B' and parent._parent._tag == 'S':
                string_size = parent._parent._children[0]._children[0]._value
                end_of_tag = string_size + message[string_size:].find(':')
                end_of_value = string_size + message[string_size:].find(';')

            if end_of_value == -1 and end_of_tag == -1:
                if message:
                    raise RuntimeError('Malformed message')
                return message

            if \
               (end_of_tag != -1 and end_of_value == -1) \
               or (end_of_tag != -1 and end_of_tag < end_of_value):

                message, tag = _parse_part(message, end_of_tag)
                node = Node.from_tag(tag)
                parent.add_child(node)

                while True:
                    message = _parse(message, node)
                    if node.is_key_value():
                        break
                    if node.is_list_of_elements() and message.startswith(';'):
                        message = message[1:]
                        break
                    if not message:
                        raise RuntimeError('Malformed message')

                return message

            elif \
                    (end_of_tag == -1 and end_of_value != -1) \
                    or (end_of_value != -1 and end_of_value < end_of_tag):

                message, value = _parse_part(message, end_of_value)
                if value:
                    parent.add_child(Node.from_value(value))
                else:
                    parent.add_child(Node.from_empty_value())
                return message
            else:
                return message

            raise RuntimeError('Malformed message')

        root = Node()
        while message:
            message = _parse(message, root)

        # print (root.str())

        return root.to_list()


class DataStream:
    def __init__(self, data):
        self._data = data
        self._index = 0

    def size(self):
        return len(self._data)

    def get(self, size):
        d = self._data[self._index:self._index + size]
        if len(d) == 0:
            return None
        self._index += len(d)
        return d


class FileStream:
    def __init__(self, path):
        self._fp = open(path, 'rb')

    def size(self):
        return os.stat(self._fp.name).st_size

    def get(self, size):
        d = self._fp.read(size)
        if len(d) == 0:
            return None
        return d


class InputFileStream():
    def __init__(self, fp):
        self._fp = fp
        self._rsp = None

    def is_error(self):
        return self._rsp is not None

    def error_rsp(self):
        return self._rsp

    def transaction_id(self):
        return None

    def handle_error(self, rsp):
        self._rsp = rsp

    def handle_data(self, _, data):
        self._fp.write(data)


TAG_RESPONSE = 'RSP'
TAG_BATCH_RESPONSE = 'RSP-BATCH'
TAG_NOTIFICATION = 'NOTIFICATION'
TAG_END_OF_MESSAGE = 'E'
TAG_UINT = 'U'
TAG_NODE_ID = 'N'
TAG_STRING = 'S'
TAG_BYTES = 'B'
TAG_BLOCK = 'BL'
TAG_LIST = 'L'
TAG_KEY_VALUE = 'KVP'
TAG_LIST_ELEMENT = 'LE'
TAG_TRANSACTION_ID = 'T'
TAG_TIMESTAMP = 'TS'
TAG_PROTOCOL_VERSION = 'V'
TAG_AUTHORITY = 'AUTHORITY'


class Authority:
    def __init__(self, type_of, name):
        _validate_authority_type(type_of)
        self.type = type_of
        self.name = name

    def is_group(self):
        return self.type == TYPE_GROUP

    def is_user(self):
        return self.type == TYPE_USER

    def __str__(self):
        if self.type == TYPE_GROUP:
            t = 'GROUP'
        elif self.type == TYPE_USER:
            t = 'USER'
        return '{}:{}'.format(t, self.name)


class Field:
    def __init__(self, content):
        self._content = content

    def as_uint(self):
        if self._content[0] != TAG_UINT:
            _malfomed_message()
        return int(self._content[1])

    def as_timestamp(self):
        if self._content[0] != TAG_TIMESTAMP:
            _malfomed_message()
        return int(self._content[1])

    def as_authority(self):
        if self._content[0] != TAG_AUTHORITY:
            _malfomed_message()
        authority_type = Field(self._content[1]).as_uint()
        name = Field(self._content[2]).as_string()
        return Authority(authority_type, name)

    def as_node_id(self):
        if self._content[0] != TAG_NODE_ID:
            _malfomed_message()
        return Field(self._content[1]).as_uint()

    def as_protocol_version(self):
        if self._content[0] != TAG_PROTOCOL_VERSION:
            _malfomed_message()
        return int(self._content[1])

    def as_transaction_id(self):
        if self._content[0] != TAG_TRANSACTION_ID:
            _malfomed_message()
        return Field(self._content[1]).as_uint()

    def as_block(self):
        if self._content[0] != TAG_BLOCK:
            _malfomed_message()
        return (Field(self._content[1]).as_uint(), Field(self._content[2]).as_uint())

    def as_string(self):
        if self._content[0] != TAG_STRING:
            _malfomed_message()
        length = Field(self._content[1]).as_uint()
        if self._content[2][0] != TAG_BYTES:
            _malfomed_message()
        content = self._content[2][1]
        if len(content) != length:
            _malfomed_message()
        return content

    def as_list(self):
        if self._content[0] != TAG_LIST:
            _malfomed_message()
        size = Field(self._content[1]).as_uint()
        content = []
        for element in self._content[2:]:
            if element[0] != TAG_LIST_ELEMENT:
                _malfomed_message()

            # List element always has at least tow fields, tag and content.
            # Content may it self be a list or single value, this is detected
            # by below if
            if len(element) == 2:
                content.append(Field(element[1]))
            else:
                content.append([Field(e) for e in element[1:]])

        if len(content) != size:
            _malfomed_message()
        return content

    def as_key_value(self):
        if self._content[0] != TAG_KEY_VALUE:
            _malfomed_message()
        key = Field(self._content[1]).as_string()
        value = Field(self._content[2])
        return (key, value)

    def key_value_list_to_dict(self):
        d = {}
        for e in self.as_list():
            key, value = e.as_key_value()
            d[key] = value
        return d

    def __getitem__(self, key):
        return self._content[key]


def _malfomed_message():
    raise RuntimeError('Malformed message')


def _validate_file_system_element_type(type_of_element):
    if type_of_element not in [FILESYSTEM_ELEMENT_FILE, FILESYSTEM_ELEMENT_DIRECTORY]:
        _malfomed_message()


def _validate_file_type(type_of_file):
    if type_of_file not in [FILE_TYPE_RANDOM_ACCESS, FILE_TYPE_BLOB]:
        _malfomed_message()


def _validate_authority_type(authority_type):
    if authority_type not in [TYPE_GROUP, TYPE_USER]:
        _malfomed_message()


class Message:
    NOTIFICATION = 1
    RESPONSE = 2

    def type(self):
        raise NotImplementedError()

    def __init__(self, rsp):
        self._rsp = rsp

    def transaction_id(self):
        return Field(self._rsp[1][1]).as_transaction_id()

    def protocol_version(self):
        return Field(self._rsp[0]).as_protocol_version()

    def is_error(self):
        return self.error_code() != 0

    def error_code(self):
        return Field(self._rsp[1][2]).as_uint()

    def size(self):
        return len(self._rsp)

    def __getitem__(self, key):
        return self._rsp[key]


class CreateResponse:
    def __init__(self, response):
        self._rsp = response
        if response.number_of_fields() == 1:
            self.node_id = response.field(0).as_node_id()
        elif response.number_of_fields() == 2:
            self.node_id = response.field(0).as_node_id()
            self.revision = response.field(1).as_uint()
        else:
            _malfomed_message()


class WriteResponse:
    def __init__(self, response):
        self._rsp = response
        if response.number_of_fields() != 1:
            _malfomed_message()
        self.revision = response.field(0).as_uint()


class DeleteResponse(WriteResponse):
    pass


class InsertResponse(WriteResponse):
    pass


class BatchEditErrordResponse:
    def __init__(self, response):
        self._rsp = response
        if response.number_of_fields() == 2:
            if response._rsp[1][0] != TAG_BATCH_RESPONSE:
                _malfomed_message()
            self.operation_index = response.field(0).as_uint()
            self.revision = response.field(1).as_uint()
        else:
            _malfomed_message()


class AllocateAuthTokenResponse:
    def __init__(self, response):
        self._rsp = response
        if response.number_of_fields() != 1:
            _malfomed_message()
        self.token = response.field(0).as_string()


class ReadResponse:
    def __init__(self, response):
        self._rsp = response
        if response.number_of_fields() != 2:
            _malfomed_message()
        self.revision = response.field(0).as_uint()
        self.offset, self.size = response.field(1).as_block()


class OpenResponse:
    def __init__(self, response):
        self._rsp = response
        if response.number_of_fields() != 5:
            _malfomed_message()

        self.node_id = response.field(0).as_node_id()
        self.revision = response.field(1).as_uint()
        self.size = response.field(2).as_uint()
        self.block_size = response.field(3).as_uint()
        self.type_of_file = response.field(4).as_uint()
        _validate_file_type(self.type_of_file)

    def is_random_access(self):
        return self.type_of_file == FILE_TYPE_RANDOM_ACCESS

    def is_blob(self):
        return self.type_of_file == FILE_TYPE_BLOB


class QueryElement:
    def __init__(self, msg):
        self.type_of_element = msg[0].as_uint()
        _validate_file_system_element_type(self.type_of_element)

        if self.is_file():
            self.name = msg[1].as_string()
            self.node_id = msg[2].as_node_id()
            self.revision = msg[3].as_uint()
            self.file_type = msg[4].as_uint()
            self.size = msg[5].as_uint()
            self.is_open = msg[6].as_uint() == 1

        elif self.is_directory():
            self.name = msg[1].as_string()
            self.node_id = msg[2].as_node_id()
            self.read = msg[3].as_authority()
            self.write = msg[4].as_authority()

    def is_file(self):
        return self.type_of_element == FILESYSTEM_ELEMENT_FILE

    def is_directory(self):
        return self.type_of_element == FILESYSTEM_ELEMENT_DIRECTORY

    def is_random_access(self):
        return self.file_type == FILE_TYPE_RANDOM_ACCESS

    def is_blob(self):
        return self.file_type == FILE_TYPE_BLOB


class QueryFilesystemChildrenResponse:
    def __init__(self, response):
        self._rsp = response
        if response.number_of_fields() != 1:
            _malfomed_message()

        self.elements = []
        for e in response.field(0).as_list():
            self.elements.append(QueryElement(e))

    def number_of_elements(self):
        return len(self.elements)


class QueryFilesystemElementPropertiesResponse:
    def is_file(self):
        return self.type_of_element == FILESYSTEM_ELEMENT_FILE

    def is_directory(self):
        return self.type_of_element == FILESYSTEM_ELEMENT_DIRECTORY

    def is_random_access_file(self):
        if self.type_of_element != FILESYSTEM_ELEMENT_FILE:
            raise RuntimeError('Element is not file')
        return self.type_of_file == FILE_TYPE_RANDOM_ACCESS

    def is_blob_file(self):
        if self.type_of_element != FILESYSTEM_ELEMENT_FILE:
            raise RuntimeError('Element is not file')
        return self.type_of_file == FILE_TYPE_BLOB

    def __init__(self, response):
        self.type_of_element = response.field(0).as_uint()
        _validate_file_system_element_type(self.type_of_element)

        if self.type_of_element == FILESYSTEM_ELEMENT_FILE:
            if response.number_of_fields() != 6:
                _malfomed_message()
            self.name = response.field(1).as_string()
            self.node_id = response.field(2).as_node_id()
            self.revision = response.field(3).as_uint()
            self.size = response.field(4).as_uint()
            self.type_of_file = response.field(5).as_uint()

        elif self.type_of_element == FILESYSTEM_ELEMENT_DIRECTORY:
            if response.number_of_fields() != 3:
                _malfomed_message()
            self.name = response.field(1).as_string()
            self.node_id = response.field(2).as_node_id()


class QueryFilesystemElementResponse:
    def is_file(self):
        return self.type_of_element == FILESYSTEM_ELEMENT_FILE

    def is_directory(self):
        return self.type_of_element == FILESYSTEM_ELEMENT_DIRECTORY

    def is_random_access_file(self):
        if self.type_of_element != FILESYSTEM_ELEMENT_FILE:
            raise RuntimeError('Element is not file')
        return self.type_of_file == FILE_TYPE_RANDOM_ACCESS

    def is_blob_file(self):
        if self.type_of_element != FILESYSTEM_ELEMENT_FILE:
            raise RuntimeError('Element is not file')
        return self.type_of_file == FILE_TYPE_BLOB

    def __init__(self, response):
        if response.number_of_fields() != 1:
            _malfomed_message()

        desc = response.field(0).key_value_list_to_dict()
        self.type_of_element = desc['type'].as_uint()
        _validate_file_system_element_type(self.type_of_element)

        self.node_id = desc['node-id'].as_uint()
        self.created = desc['created-at'].as_uint()
        self.modified = desc['modified-at'].as_uint()

        if self.type_of_element == FILESYSTEM_ELEMENT_FILE:
            if len(desc) != 12:
                print('Unhandled file fields in QueryFilesystemElementResponse')

            self.created_by = desc['created-by'].as_authority()
            self.modified_by = desc['modified-by'].as_authority()
            self.write_access = desc['parent-write-authority'].as_authority()
            self.read_access = desc['parent-read-authority'].as_authority()
            self.block_size = desc['page-size'].as_uint()
            self.size = desc['size'].as_uint()
            self.revision = desc['revision'].as_uint()
            self.type_of_file = desc['file-type'].as_uint()
            _validate_file_type(self.type_of_file)

        elif self.type_of_element == FILESYSTEM_ELEMENT_DIRECTORY:
            if len(desc) != 6:
                print('Unhandled file fields in QueryFilesystemElementResponse')

            self.write_access = desc['write-authority'].as_authority()
            self.read_access = desc['read-authority'].as_authority()


class QueryCountersResponse:
    def __init__(self, response):
        self._rsp = response
        if response.number_of_fields() != 1:
            _malfomed_message()

        desc = response.field(0).key_value_list_to_dict()
        self._number_of_counters = 3
        if len(desc) != self._number_of_counters:
            _malfomed_message()

        self.active_connections = desc['active-connections'].as_uint()
        self.number_of_files = desc['number-of-files'].as_uint()
        self.number_of_open_files = desc['number-of-open-files'].as_uint()

    def number_of_counters(self):
        return self._number_of_counters


class QuerySystemResponse:
    def __init__(self, response):
        self._rsp = response
        if response.number_of_fields() != 1:
            _malfomed_message()

        self.has_admin_information = False

        desc = response.field(0).key_value_list_to_dict()
        if len(desc) in [4, 5]:
            self.started_at = desc['started-at'].as_timestamp()
            self.server_id = desc['server-id'].as_uint()
            self.max_number_of_open_files_per_connection = (
                desc['max-number-of-open-files-per-connection'].as_uint()
            )
            self.number_of_open_files = desc['number-of-open-files'].as_uint()
        if len(desc) in [5]:
            self.has_admin_information = True
            self.expiration = desc['certification-expiration'].as_timestamp()
        else:
            _malfomed_message()


class Response(Message):
    def type(self):
        return Message.RESPONSE

    def __init__(self, rsp):
        super(Response, self).__init__(rsp)
        if self._rsp[1][0] not in [TAG_RESPONSE, TAG_BATCH_RESPONSE]:
            _malfomed_message()
        if self._rsp[-1][0] != TAG_END_OF_MESSAGE:
            _malfomed_message()

    def number_of_fields(self):
        return len(self._rsp) - 3  # Ignore protocol version, rsp, end

    def field(self, index):
        return Field(self._rsp[2 + index])

    def as_create_rsp(self):
        return CreateResponse(self)

    def as_open_rsp(self):
        return OpenResponse(self)

    def as_write_rsp(self):
        return WriteResponse(self)

    def as_insert_rsp(self):
        return InsertResponse(self)

    def as_delete_rsp(self):
        return DeleteResponse(self)

    def as_read_rsp(self):
        return ReadResponse(self)

    def as_query_fs_children_rsp(self):
        return QueryFilesystemChildrenResponse(self)

    def as_query_counters_rsp(self):
        return QueryCountersResponse(self)

    def as_query_fs_element_properties_rsp(self):
        return QueryFilesystemElementPropertiesResponse(self)

    def as_query_fs_element_rsp(self):
        return QueryFilesystemElementResponse(self)

    def as_query_system_rsp(self):
        return QuerySystemResponse(self)

    def as_batch_edit_response(self):
        return BatchEditErrordResponse(self)

    def as_allocate_auth_token_response(self):
        return AllocateAuthTokenResponse(self)


class Notification(Message):
    TYPE_DISCONNECTED = 1
    TYPE_MODIFIED = 2
    TYPE_INSERTED = 3
    TYPE_DELETED = 4

    # todo: rename to type_of_message
    def type(self):
        return Message.NOTIFICATION

    def create(msg):
        n = Notification(msg)
        if n.notification_type() == Notification.TYPE_DISCONNECTED:
            return NotificationDisconnected(msg)
        elif n.notification_type() == Notification.TYPE_MODIFIED:
            return NotificationModified(msg)
        elif n.notification_type() == Notification.TYPE_INSERTED:
            return NotificationModified(msg)
        elif n.notification_type() == Notification.TYPE_DELETED:
            return NotificationModified(msg)
        else:
            raise NotImplementedError()

    def __init__(self, msg):
        super(Notification, self).__init__(msg)
        if self._rsp[1][0] != TAG_NOTIFICATION:
            _malfomed_message()
        if self._rsp[-1][0] != TAG_END_OF_MESSAGE:
            _malfomed_message()
        if len(self._rsp) != 4:  # Version, message type, notification, and end
            _malfomed_message()

    def notification_type(self):
        t = self._rsp[2][0]
        if t == 'DISCONNECTED':
            return Notification.TYPE_DISCONNECTED
        elif t == 'F-MOD':
            return Notification.TYPE_MODIFIED
        elif t == 'F-INS':
            return Notification.TYPE_INSERTED
        elif t == 'F-DEL':
            return Notification.TYPE_DELETED
        raise NotImplementedError()

    def number_of_fields(self):
        return len(self._rsp[2]) - 1  # Ignore notification type

    def field(self, index):
        return Field(self._rsp[2][1 + index])


class NotificationDisconnected(Notification):
    def __init__(self, msg):
        super(Notification, self).__init__(msg)
        if self.number_of_fields() != 1:
            _malfomed_message()

        self.reason = self.field(0).as_string()


class NotificationModified(Notification):
    def __init__(self, msg):
        super(Notification, self).__init__(msg)
        if self.number_of_fields() != 3:
            _malfomed_message()

        self.node_id = self.field(0).as_node_id()
        self.revision = self.field(1).as_uint()
        self.block_offset, self.block_size = self.field(2).as_block()

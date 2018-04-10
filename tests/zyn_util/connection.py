import logging
import socket
import ssl
import time

import certifi


FILE_TYPE_FILE = 0  # todo: Rename FILESYSTEM_ELEMENT_FILE
FILE_TYPE_FOLDER = 1
FILE_TYPE_RANDOM_ACCESS = 0
FILE_TYPE_BLOB = 1
TYPE_USER = 0
TYPE_GROUP = 1


class ZynConnection:

    def __init__(self, path_cert=None, debug_messages=False):
        self._context = ssl.create_default_context()
        if path_cert is not None:
            self._context.load_verify_locations(path_cert)
        self._log = logging.getLogger(__name__)
        self._transaction_id = 1
        self._debug_messages = debug_messages
        self._input_buffer = b''
        self._notifications = []

    def load_default_certificate_bundle(self):
        self._context.load_verify_locations(certifi.where())

    def connect(self, remote_address, remote_port, remote_hostname=None):
        self._socket_ = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket_.connect((remote_address, remote_port))
        self._socket = self._context.wrap_socket(
            self._socket_,
            server_hostname=remote_hostname or remote_address,
        )

        self._log.debug("Connected to {}:{}".format(remote_address, remote_port))

    def disconnect(self):
        self._socket.shutdown(socket.SHUT_WR)
        self._socket.close()

    def enable_debug_messages(self):
        self._debug_messages = True

    def _send_receive(self, req):
        self.write(req)
        return self.read_response()

    def pop_notification(self):
        if self._notifications:
            return self._notifications.pop(0)
        return None

    def authenticate(self, username, password, transaction_id=None):
        req = \
            self.field_version() \
            + 'A:' \
            + self.field_transaction_id(transaction_id or self._consume_transaction_id()) \
            + self.field_string(username) \
            + self.field_string(password) \
            + ';' \
            + self.field_end_of_message() \

        return self._send_receive(req)

    def create_file(
            self,
            name,
            file_type=None,
            parent_node_id=None,
            parent_path=None,
            transaction_id=None
    ):
        parent = self.file_descriptor(parent_node_id, parent_path)
        req = \
            self.field_version() \
            + 'CREATE-FILE:' \
            + self.field_transaction_id(transaction_id or self._consume_transaction_id()) \
            + parent \
            + self.field_string(name) \
            + self.field_unsigned(file_type) \
            + ';' \
            + self.field_end_of_message() \

        return self._send_receive(req)

    def create_file_random_access(
            self,
            name,
            parent_node_id=None,
            parent_path=None,
            transaction_id=None
    ):
        return self.create_file(
            name,
            FILE_TYPE_RANDOM_ACCESS,
            parent_node_id,
            parent_path,
            transaction_id
        )

    def create_file_blob(
            self,
            name,
            parent_node_id=None,
            parent_path=None,
            transaction_id=None
    ):
        return self.create_file(name, FILE_TYPE_BLOB, parent_node_id, parent_path, transaction_id)

    def create_folder(self, name, parent_node_id=None, parent_path=None, transaction_id=None):
        parent = self.file_descriptor(parent_node_id, parent_path)
        req = \
            self.field_version() \
            + 'CREATE-FOLDER:' \
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
            self._socket.send(data[index_start:index_end])
            index_start += block_size
            rsp = self.read_response(timeout=60*5)
            if rsp.is_error():
                return rsp

        return self.read_response()

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
        self._socket.send(data)
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
        self._socket.send(data)
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

    def query_list(self, node_id=None, path=None, transaction_id=None):
        req = \
            self.field_version() \
            + 'Q-LIST:' \
            + self.field_transaction_id(transaction_id or self._consume_transaction_id()) \
            + self.file_descriptor(node_id, path) \
            + ';' \
            + self.field_end_of_message() \

        self.write(req)
        rsp = self.read_response()
        return rsp

    def query_filesystem(self, node_id=None, path=None, transaction_id=None):
        req = \
            self.field_version() \
            + 'Q-FILESYSTEM:' \
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
        while True:
            message = self.read_message(end_of_message_field, timeout)
            if message.type() == Message.NOTIFICATION:
                self._notifications.append(message)
            return message

    def read_message(self, end_of_message_field=None, timeout=None):
        eom = end_of_message_field or self.field_end_of_message()
        eom = eom.encode('utf-8')

        timeout = timeout or 10.
        self._socket.settimeout(timeout)

        message = ''
        while True:
            try:
                d = self._socket.recv()
            except ssl.SSLWantReadError:
                continue
            except Exception as e:
                print('Exception while reading message:', type(e), e)
                d = None
            if not d:
                raise TimeoutError('Socket disconnected')

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
            return Notification(parsed)
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
                        l = c.to_list()
                        if isinstance(l, list) and not l:
                            continue
                        children.append(l)
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


TAG_RESPONSE = 'RSP'
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
TAG_PROTOCOL_VERSION = 'V'


class Field:
    def __init__(self, content):
        self._content = content

    def as_uint(self):
        if self._content[0] != TAG_UINT:
            _malfomed_message()
        return int(self._content[1])

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
    if type_of_element not in [FILE_TYPE_FILE, FILE_TYPE_FOLDER]:
        _malfomed_message()


class Message:
    NOTIFICATION = 1
    RESPONSE = 2

    def type(self):
        raise NotImplemented()

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
        if response.number_of_fields() != 1:
            _malfomed_message()
        self.node_id = response.field(0).as_node_id()


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
        self.type_of_element = response.field(4).as_uint()
        _validate_file_system_element_type(self.type_of_element)

    def is_file(self):
        return self.type == FILE_TYPE_FILE

    def is_folder(self):
        return self.type == FILE_TYPE_FOLDER


class QueryElement:
    def __init__(self, name, node_id, type_of_element):
        self.name = name
        self.node_id = node_id
        self.type_of_element = type_of_element
        _validate_file_system_element_type(self.type_of_element)

    def is_file(self):
        return self.type_of_element == FILE_TYPE_FILE

    def is_directory(self):
        return self.type_of_element == FILE_TYPE_FOLDER


class QueryListResponse:
    def __init__(self, response):
        self._rsp = response
        if response.number_of_fields() != 1:
            _malfomed_message()

        self.elements = []

        for e in response.field(0).as_list():
            self.elements.append(
                QueryElement(
                    e[0].as_string(),
                    e[1].as_node_id(),
                    e[2].as_uint(),
                )
            )

    def number_of_elements(self):
        return len(self.elements)


class QueryFilesystemElementResponse:
    def __init__(self, response):
        if response.number_of_fields() != 1:
            _malfomed_message()

        desc = response.field(0).key_value_list_to_dict()
        self.type_of_element = desc['type'].as_uint()
        _validate_file_system_element_type(self.type_of_element)

        if self.type_of_element == FILE_TYPE_FILE and len(desc) == 7:
            self.block_size = desc['block-size'].as_uint()
        elif self.type_of_element == FILE_TYPE_FOLDER and len(desc) == 6:
            pass
        else:
            _malfomed_message()

        self.node_id = desc['node-id'].as_uint()
        self.write_access = desc['write-access'].as_string()
        self.read_access = desc['read-access'].as_string()
        self.created = desc['created'].as_uint()
        self.modified = desc['modified'].as_uint()


class QueryCountersResponse:
    def __init__(self, response):
        self._rsp = response
        if response.number_of_fields() != 1:
            _malfomed_message()

        desc = response.field(0).key_value_list_to_dict()
        if len(desc) != 1:
            _malfomed_message()

        self._number_of_counters = 1
        self.active_connections = desc['active-connections'].as_uint()

    def number_of_counters(self):
        return self._number_of_counters


class QuerySystemResponse:
    def __init__(self, response):
        self._rsp = response
        if response.number_of_fields() != 1:
            _malfomed_message()

        desc = response.field(0).key_value_list_to_dict()
        if len(desc) != 2:
            _malfomed_message()

        self.started_at = desc['started-at'].as_uint()
        self.server_id = desc['server-id'].as_uint()


class Response(Message):
    def type(self):
        return Message.RESPONSE

    def __init__(self, rsp):
        super(Response, self).__init__(rsp)
        if self._rsp[1][0] != TAG_RESPONSE:
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

    def as_query_list_rsp(self):
        return QueryListResponse(self)

    def as_query_counters_rsp(self):
        return QueryCountersResponse(self)

    def as_query_filesystem_rsp(self):
        return QueryFilesystemElementResponse(self)

    def as_query_system_rsp(self):
        return QuerySystemResponse(self)


class Notification(Message):
    def type(self):
        return Message.NOTIFICATION

    def __init__(self, rsp):
        super(Notification, self).__init__(rsp)
        if self._rsp[1][0] != TAG_NOTIFICATION:
            _malfomed_message()
        if self._rsp[-1][0] != TAG_END_OF_MESSAGE:
            _malfomed_message()

    def notification_type(self):
        return self._rsp[2][0]

    def number_of_fields(self):
        return len(self._rsp) - 3  # Ignore protocol version, rsp, notification type, and end

    def field(self, index):
        return Field(self._rsp[3 + index])

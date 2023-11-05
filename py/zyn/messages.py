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
            self.is_admin = desc['is-admin'].as_string()
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

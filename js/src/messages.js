const {
  ZynFileType,
} = require('./common')


const ELEMENT_TYPE_FILE = 0;
const ELEMENT_TYPE_DIRECTORY = 1;

const ELEMENT_FILE_TYPE_RANDOM_ACCESS = 0;
const ELEMENT_FILE_TYPE_BLOB = 1;

const AUTHORITY_TYPE_USER = 0;
const AUTHORITY_TYPE_GROUP = 1;

exports.Constants = Object.freeze({
    'ELEMENT_TYPE_FILE': ELEMENT_TYPE_FILE,
    'ELEMENT_TYPE_DIRECTORY': ELEMENT_TYPE_DIRECTORY,
})

class ZynByteBuffer {
    constructor(size) {
        this._data = new Uint8Array(size);
        this._current_size = 0;
    }

    complete_size() {
        return this._data.length;
    }

    current_size() {
        return this._current_size;
    }

    data() {
        return this._data;
    }

    is_complete() {
        return this._current_size === this._data.length;
    }

    add(data) {
        let size_after = this._current_size + data.length;
        if (size_after > this._data.length) {
            zyn_show_modal_error(
                ErrorLevel.error,
                'Buffer handling error',
                `Trying to add data of ${data.length} which would exceed buffer size ${this._data.length}`
            );
            throw '';
        }
        this._data.set(data, this._current_size);
        this._current_size = size_after;
    }
}

class Message {
    constructor() {}

    is_notification() { return false; }
}

class Notification extends Message {
    constructor() {
        super();
    }

    is_notification() { return true; }
    is_disconnect() { return false; }
    is_edit() { return false; }
}

exports.DisconnectNotification = class DisconnectNotification extends Notification {
    constructor(description) {
        super();
        this.description = description;
    }

    to_string() { return `Server disconeccted: ${this.description}`; }
    is_disconnect() { return true; }
}

exports.EditNotification = class EditNotification extends Notification {
    constructor(type_of, node_id, revision, offset, size) {
        super();
        this.type_of_edit = type_of;
        this.node_id = node_id;
        this.revision = revision;
        this.offset = offset;
        this.size = size;
    }
    is_edit() { return true; }
    to_string() { return `Edit modification: ${this.type_of_edit}(revision=${this.revision}, offset=${this.offset}, size=${this.size})`; }
}

class MessageRsp extends Message {
    constructor(namespace, transaction_id, error_code) {
        super();
        this.namespace = namespace;
        this.transaction_id = transaction_id;
        this.error_code = error_code;
    }

    is_error() { return this.error_code != 0; }
    is_data_message() { return false; }
}

exports.ListChildrenRsp = class ListChildrenRsp extends MessageRsp {
    constructor(namespace, transaction_id, error_code) {
        super(namespace, transaction_id, error_code);
        this.elements = []
    }

    add_element(element) {
        this.elements.push(element);
    }
}

class OpenRsp extends MessageRsp {
    constructor(namespace, transaction_id, error_code, node_id, revision, size, page_size, file_type) {
        super(namespace, transaction_id, error_code);
        this.node_id = node_id;
        this.revision = revision;
        this.size = size;
        this.page_size = page_size;
        if (file_type == ELEMENT_FILE_TYPE_RANDOM_ACCESS) {
            this.file_type = ZynFileType.ra;
        } else if (file_type == ELEMENT_FILE_TYPE_BLOB) {
            this.file_type = ZynFileType.blob;
        } else {
            zyn_unhandled();
        }
    }
}
exports.OpenRsp = OpenRsp;

class CreateFileRsp extends MessageRsp {
   constructor(namespace, transaction_id, error_code, node_id, revision) {
        super(namespace, transaction_id, error_code);
        this.node_id = node_id;
        this.revision = revision;
   }
}
exports.CreateFileRsp = CreateFileRsp;

class CreateDirectoryRsp extends MessageRsp {
   constructor(namespace, transaction_id, error_code, node_id) {
        super(namespace, transaction_id, error_code);
        this.node_id = node_id;
   }
}
exports.CreateDirectoryRsp = CreateDirectoryRsp;

exports.ReadRsp = class ReadRsp extends MessageRsp {
    constructor(namespace, transaction_id, error_code, revision, offset, size) {
        super(namespace, transaction_id, error_code);
        this.revision = revision;
        this.offset = offset;
        this._data = new ZynByteBuffer(size);
    }

    data() {
        return this._data.data();
    }

    is_data_message() { return true; }

    is_complete() {
        return this._data.is_complete();
    }

    add_data(data) {
        this._data.add(data);
    }
}

exports.EditRsp = class EditRsp extends MessageRsp {
    constructor(namespace, transaction_id, error_code, revision) {
        super(namespace, transaction_id, error_code);
        this.revision = revision;
    }
}

exports.BatchEditRsp = class BatchEditRsp extends MessageRsp {
    constructor(namespace, transaction_id, error_code, revision, operation_index) {
        super(namespace, transaction_id, error_code);
        this.revision = revision;
        this.operation_index = operation_index;
    }
}

class QueryRsp extends MessageRsp {
    constructor(namespace, transaction_id, error_code) {
        super(namespace, transaction_id, error_code);
        this.fields = {}
    }

    add_field(name, type_of, value) {
        this.fields[name] = {
            'type': type_of,
            'value': value,
        };
    }
}

exports.ExpectRsp = class ExpectRsp {
    constructor() {
        this.callback = null;
        this.expecte_rsp_type = null;
    }

    is_set() {
        return this.callback !== null;
    }

    reset() {
        let c = this.callback;
        this.callback = null;
        this.expecte_rsp_type = null;
        return c;
    }

    set_callback_for_rsp(rsp_type, callback) {
        this.callback = callback;
        this.expecte_rsp_type = rsp_type;
    }
}

exports.MessageRsp = MessageRsp;

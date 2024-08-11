const {
  MessageRsp,
  ExpectRsp,
  ListChildrenRsp,
  OpenRsp,
  ReadRsp,
  Constants,
  CreateFileRsp,
  CreateDirectoryRsp,
  EditRsp,
  BatchEditRsp,
  EditNotification,
  DisconnectNotification,
} = require('./messages')

const {
  FilesystemElementFile,
  FilesystemElementDirectory,
  Authority,
  OpenMode,
} = require('./common')

const MSG_TYPE_AUTH = 'A';
const MSG_TYPE_QUERY_FS_CHILDREN = 'Q-FS-C';
const MSG_TYPE_OPEN_FILE = 'O';
const MSG_TYPE_CLOSE_FILE = 'CLOSE';
const MSG_TYPE_READ = 'R';
const MSG_TYPE_CREATE_FILE = 'CREATE-FILE';
const MSG_TYPE_CREATE_DIRECTORY = 'CREATE-DIRECTORY';
const MSG_TYPE_DELETE_ELEMENT = 'DELETE';
const MSG_TYPE_QUERY_SYSTEM = 'Q-SYSTEM';

const MSG_HANDLER_EDIT = 'EDIT';
const MSG_HANDLER_BATCH_EDIT = 'EDIT-BATCH';
const MSG_HANDLER_EDIT_PREAMBLE = 'EDIT-PREAMBLE';


class ModificationState {
    constructor(node_id, revision, modifications, callback, connection) {
        this._node_id = node_id;
        this._revision = revision;
        this._modifications = modifications;
        this._callback = callback;
        this._connection = connection;
        this._operation_index = 0;
        this.apply();
    }

    apply() {
        let mod = this._modifications[this._operation_index];
        if (mod.type == 'add') {
            this._connection.edit_ra_file_preamble_insert(this._node_id, this._revision, mod.offset, mod.bytes.length, (rsp_preamble) => {
                if (!rsp_preamble.is_error()) {
                    this._connection._expected_msg.set_callback_for_rsp(MSG_HANDLER_EDIT, (rsp) => {
                        this._complete_operation(rsp);
                    });
                    this._connection._socket.send(mod.bytes);
                }
            })
        } else if (mod.type == 'delete') {
            this._connection.edit_ra_file_delete(this._node_id, this._revision, mod.offset, mod.size, (rsp) => {
                this._complete_operation(rsp);
            })
        } else {
            zyn_unhandled();
        }

    }

    _complete_operation(rsp) {
        this._operation_index += 1;
        this._revision = rsp.revision;
        if (this._operation_index == this._modifications.length) {
            this._callback(rsp);
        } else {
            this.apply();
        }
    }
}


class BatchModificationState {
  constructor(node_id, revision, modifications, callback, connection) {
    this._node_id = node_id;
    this._revision = revision;
    this._modifications = modifications;
    this._callback = callback;
    this._connection = connection;
    this._operation_index = 0;
    this._connection.edit_ra_file_preamble_batch_edit(this._node_id, this._revision, this._modifications.length, (rsp_preamble) => {
      if (rsp_preamble.is_error()) {
        return this._complete_operation(rsp_preamble);
      }
      this.apply();
    });
  }

  apply() {
    this._connection._expected_msg.set_callback_for_rsp(MSG_HANDLER_BATCH_EDIT, (rsp) => {
      return this.complete_operation(rsp);
    });

    //zyn_show_modal_loading(`Applying modification ${this._operation_index + 1} / ${this._modifications.length}`);

    let mod = this._modifications[this._operation_index];
    if (mod.type == 'add') {
      this._connection.edit_ra_batch_edit(2, mod.offset, mod.bytes.length);
      this._connection._socket.send(mod.bytes);
    } else if (mod.type == 'delete') {
      this._connection.edit_ra_batch_edit(1, mod.offset, mod.size);
    } else {
      zyn_unhandled();
    }
  }

  complete_operation(rsp) {
    this._revision = rsp.revision;
    this._operation_index += 1;
    if (rsp.is_error()) {
        this._callback(rsp);
    } else {
      if (this._operation_index == this._modifications.length) {
        this._callback(rsp);
      } else {
        this.apply();
      }
    }
  }
}


class Connection {
  constructor(server_address, on_connected, on_error, on_close) {
    this._text_encoder = new TextEncoder();
    this._text_decoder = new TextDecoder();

    this._socket = new WebSocket(server_address);
    this._socket.onopen = on_connected;
    this._socket.onerror = on_error;
    this._socket.onclose = on_close;
    this._socket.onmessage = (event) => this._handle_socket_message(event);
    this._socket.binaryType = "arraybuffer";
    this._transaction_id = 1;

    this._expected_msg = new ExpectRsp();
    this._msg = null;
    this._tag_end = this._text_encoder.encode('E:;');
  }

  handle_notification(msg) {
    // todo
    throw('Unhandled')
  }

  close() {
    this._socket.close();
  }

  is_ok() {
    return this._socket.readyState in [WebSocket.CONNECTING, WebSocket.OPEN];
  }

  encode_to_bytes(text) {
    return this._text_encoder.encode(text);
  }

  decode_from_bytes(bytes) {
    return this._text_decoder.decode(bytes);
  }

  _handle_socket_message(event) {
    let event_data = new Uint8Array(event.data);

    if (this._msg !== null) {
      this._msg.add_data(event_data);
      if (this._msg.is_complete()) {
        let msg = this._msg;
        this._msg = null;
        let c = this._expected_msg.reset();
        c(msg);
      }
    } else {
      let msg = this._parse_message(event_data);

      if (msg.is_notification()) {
        this._handle_notification(msg);
      } else if (msg.is_data_message() && !msg.is_complete()) {
        this._msg = msg;
      } else {
        let c = this._expected_msg.reset();
        c(msg);
      }
    }
  }

  _peek(msg, expected) {
    let s = msg.substr(0, expected.length);
    return s === expected;
  }

  _parse_advance(msg, expected) {
    let s = msg.substr(0, expected.length);
    if (s != expected) {
      throw `Malformed message: expected "${expected}", received "${msg}"`;
    }
    return msg.substr(expected.length);
  }

    _parse_unsigned(msg) {
        msg = this._parse_advance(msg, 'U:');
        let end = msg.search(';');
        let number = msg.substr(0, end);
        if (isNaN(number)) {
            throw `Failed to parse number from ${msg}, read ${number}`;
        }
        return [msg.substr(end + 1), Number(number)];
    }

    _parse_timestamp(msg) {
        msg = this._parse_advance(msg, 'TS:');
        let end = msg.search(';');
        let number = msg.substr(0, end);
        if (isNaN(number)) {
            throw `Failed to parse number from ${msg}, read ${number}`;
        }
        return [msg.substr(end + 1), Number(number)];
    }

    _parse_string(msg) {
        let l = 0;
        msg = this._parse_advance(msg, 'S:');
        [msg, l] = this._parse_unsigned(msg);
        msg = this._parse_advance(msg, 'B:');
        let v = msg.substr(0, l);
        msg = msg.substr(l);
        msg = this._parse_advance(msg, ';;');
        return [msg, v];
    }

    _parse_node_id(msg) {
        let v = 0;
        msg = this._parse_advance(msg, 'N:');
        [msg, v] = this._parse_unsigned(msg);
        msg = this._parse_advance(msg, ';');
        return [msg, v];
    }

    _parse_authority(msg) {
        let type_of = 0;
        let name = 0;
        msg = this._parse_advance(msg, 'AUTHORITY:');
        [msg, type_of] = this._parse_unsigned(msg);
        [msg, name] = this._parse_string(msg);
        msg = this._parse_advance(msg, ';');
        return [msg, new Authority(type_of, name)];
    }

    _parse_block(msg) {
        let offset = 0;
        let size = 0;
        msg = this._parse_advance(msg, 'BL:');
        [msg, offset] = this._parse_unsigned(msg);
        [msg, size] = this._parse_unsigned(msg);
        msg = this._parse_advance(msg, ';');
        return [msg, offset, size]
    }

    _parse_key_value(msg) {
        let name = null, value =null, type_of = null;
        msg = this._parse_advance(msg, 'KVP:');
        [msg, name] = this._parse_string(msg);

        try {
            [msg, value] = this._parse_unsigned(msg);
            type_of = 'unsigned';
        } catch (_) {}
        if (value === null) {
            try {
                [msg, value] = this._parse_timestamp(msg);
                type_of = 'timestamp';
            } catch (_) {}
        }
        if (value === null) {
            zyn_unhandled();
        }

        msg = this._parse_advance(msg, ';');
        return [msg, name, type_of, value]
    }

    _parse_tag(msg) {
        let index = msg.search(':');
        let tag = msg.substr(0, index);
        msg = msg.substr(index + 1);
        return [msg, tag];
    }

    _parse_message(array) {
        let msg = this._find_message(array);
        let namespace = null;

        // console.log(msg);

        while (msg.length > 0) {
            let tag = null;
            [msg, tag] = this._parse_tag(msg);

            if (tag == 'V') {

                let end = msg.search(';');
                namespace = msg.substr(2, end - 2);
                msg = msg.substr(end + 1);

            } else if (tag == 'NOTIFICATION') {

                let notification_type = null;
                msg = this._parse_advance(msg, ';');
                [msg, notification_type] = this._parse_tag(msg);

                if (notification_type === 'DISCONNECTED') {

                    let desc = null;
                    [msg, desc] = this._parse_string(msg);
                    return new DisconnectNotification(desc);

                } else if (notification_type === 'F-INS') {

                    let node_id = null, revision = null, offset = null, size = null;
                    [msg, node_id] = this._parse_node_id(msg);
                    [msg, revision] = this._parse_unsigned(msg);
                    [msg, offset, size] = this._parse_block(msg);
                    return new EditNotification('insert', node_id, revision, offset, size)

                } else if (notification_type === 'F-MOD') {

                    let node_id = null, revision = null, offset = null, size = null;
                    [msg, node_id] = this._parse_node_id(msg);
                    [msg, revision] = this._parse_unsigned(msg);
                    [msg, offset, size] = this._parse_block(msg);
                    return new EditNotification('modify', node_id, revision, offset, size)

                } else if (notification_type === 'F-DEL') {

                    let node_id = null, revision = null, offset = null, size = null;
                    [msg, node_id] = this._parse_node_id(msg);
                    [msg, revision] = this._parse_unsigned(msg);
                    [msg, offset, size] = this._parse_block(msg);
                    return new EditNotification('delete', node_id, revision, offset, size)

                } else {
                    zyn_unhandled();
                }

                return n;

            } else if (tag === 'RSP' || tag === 'RSP-BATCH') {

                if (!this._expected_msg.is_set()) {
                    throw `Unexpect RSP`;
                }
                let transaction_id = null, error_code = null;
                msg = this._parse_advance(msg, 'T:');
                [msg, transaction_id] = this._parse_unsigned(msg);
                msg = this._parse_advance(msg, ';');
                [msg, error_code] = this._parse_unsigned(msg);
                msg = this._parse_advance(msg, ';');

                if (this._peek(msg, "E:;")) {

                    msg = this._parse_advance(msg, 'E:;');
                    return new MessageRsp(namespace, transaction_id, error_code)

                } else if (this._expected_msg.expecte_rsp_type == MSG_HANDLER_EDIT_PREAMBLE) {

                    msg = this._parse_advance(msg, 'E:;');
                    return new MessageRsp(namespace, transaction_id, error_code)

                } else if (this._expected_msg.expecte_rsp_type == MSG_TYPE_DELETE_ELEMENT) {

                    msg = this._parse_advance(msg, 'E:;');
                    return new MessageRsp(namespace, transaction_id, error_code)

                } else if (this._expected_msg.expecte_rsp_type == MSG_TYPE_AUTH) {

                    msg = this._parse_advance(msg, 'E:;');
                    return new MessageRsp(namespace, transaction_id, error_code)

                } else if (this._expected_msg.expecte_rsp_type == MSG_TYPE_CLOSE_FILE) {

                    msg = this._parse_advance(msg, 'E:;');
                    return new MessageRsp(namespace, transaction_id, error_code)

                } else if (this._expected_msg.expecte_rsp_type == MSG_TYPE_QUERY_SYSTEM) {

                    let rsp = new QueryRsp(namespace, transaction_id, error_code);
                    let number_of_elements = 0;
                    msg = this._parse_advance(msg, 'L:');
                    [msg, number_of_elements] = this._parse_unsigned(msg);
                    for (let i = 0; i < number_of_elements; i++) {
                        let name = null, value = null, type_of;
                        msg = this._parse_advance(msg, 'LE:');
                        [msg, name, type_of, value] = this._parse_key_value(msg);
                        msg = this._parse_advance(msg, ';');
                        rsp.add_field(name, type_of, value);
                    }
                    return rsp;

                } else if (this._expected_msg.expecte_rsp_type == MSG_TYPE_CREATE_FILE) {

                    let node_id = null, revision = null;;
                    [msg, node_id] = this._parse_node_id(msg);
                    [msg, revision] = this._parse_unsigned(msg);
                    return new CreateFileRsp(namespace, transaction_id, error_code, node_id, revision);

                } else if (this._expected_msg.expecte_rsp_type == MSG_TYPE_CREATE_DIRECTORY) {

                    let node_id = null, revision = null;;
                    [msg, node_id] = this._parse_node_id(msg);
                    return new CreateDirectoryRsp(namespace, transaction_id, error_code, node_id, revision);

                } else if (this._expected_msg.expecte_rsp_type == MSG_TYPE_OPEN_FILE) {

                    let node_id = null, revision = null, size = null, page_size = null, file_type = null;
                    [msg, node_id] = this._parse_node_id(msg);
                    [msg, revision] = this._parse_unsigned(msg);
                    [msg, size] = this._parse_unsigned(msg);
                    [msg, page_size] = this._parse_unsigned(msg);
                    [msg, file_type] = this._parse_unsigned(msg);
                    msg = this._parse_advance(msg, 'E:;');
                    return new OpenRsp(namespace, transaction_id, error_code, node_id, revision, size, page_size, file_type);

                } else if (this._expected_msg.expecte_rsp_type == MSG_TYPE_READ) {

                    let revision = null, offset = null, size = null, data = null;
                    [msg, revision] = this._parse_unsigned(msg);
                    [msg, offset, size] = this._parse_block(msg);
                    msg = this._parse_advance(msg, 'E:;');
                    return new ReadRsp(namespace, transaction_id, error_code, revision, offset, size);

                } else if (this._expected_msg.expecte_rsp_type == MSG_HANDLER_EDIT) {

                    let revision = null;
                    [msg, revision] = this._parse_unsigned(msg);
                    msg = this._parse_advance(msg, 'E:;');
                    return new EditRsp(namespace, transaction_id, error_code, revision);

                } else if (this._expected_msg.expecte_rsp_type == MSG_HANDLER_BATCH_EDIT) {

                    let revision = null, operation_index = null;
                    [msg, operation_index] = this._parse_unsigned(msg);
                    [msg, revision] = this._parse_unsigned(msg);
                    msg = this._parse_advance(msg, 'E:;');
                    return new BatchEditRsp(namespace, transaction_id, error_code, revision, operation_index);

                } else if (this._expected_msg.expecte_rsp_type == MSG_TYPE_QUERY_FS_CHILDREN) {

                    msg = this._parse_advance(msg, 'L:');
                    let n = 0;
                    let rsp = new ListChildrenRsp(namespace, transaction_id, error_code);
                    [msg, n] = this._parse_unsigned(msg);

                    for (let i = 0; i < n; i++) {
                        let type_of = null, name = null, node_id = null;

                        msg = this._parse_advance(msg, 'LE:');
                        [msg, type_of] = this._parse_unsigned(msg);
                        [msg, name] = this._parse_string(msg);
                        [msg, node_id] = this._parse_node_id(msg);
                        if (type_of == Constants.ELEMENT_TYPE_FILE) {

                            let revision = null, file_type = null, size = null, is_open = null;
                            [msg, revision] = this._parse_unsigned(msg);
                            [msg, file_type] = this._parse_unsigned(msg);
                            [msg, size] = this._parse_unsigned(msg);
                            [msg, is_open] = this._parse_unsigned(msg);
                            rsp.add_element(new FilesystemElementFile(name, node_id, revision, file_type, size, is_open));

                        } else if (type_of == Constants.ELEMENT_TYPE_DIRECTORY) {
                            let auth_read = null;
                            let auth_write = null;
                            [msg, auth_read] = this._parse_authority(msg);
                            [msg, auth_write] = this._parse_authority(msg);
                            rsp.add_element(new FilesystemElementDirectory(name, node_id, auth_read, auth_write));
                        }
                        msg = this._parse_advance(msg, ';');


                    }
                    msg = this._parse_advance(msg, ';E:;');
                    return rsp;
                } else {
                    zyn_unhandled();
                }
            } else {
                break;
            }
        }
    }

    _find_message(array) {
        let last_char = this._tag_end[this._tag_end.length - 1];

        if (array.length < 2) {
            return null;
        }

        for (let i = 2; i < array.length; i++) {
            if (array[i] === last_char) {
                let found = true;

                for (let tag_i = 1; tag_i < this._tag_end.length; tag_i++) {
                    // console.log(`-- ${tag_i} ${array[i - tag_i]} ${this._tag_end[this._tag_end.length - 1 - tag_i]}`)
                    if (array[i - tag_i] != this._tag_end[this._tag_end.length - 1 - tag_i]) {
                        found = false;
                        break
                    }
                }
                if (found) {
                    return this._text_decoder.decode(array.subarray(0, i + 1));
                }
            }
        }
        return null;
    }

    _format_string(value) {
        return `S:U:${value.length};B:${value};;`;
    }

    _format_uint(value) {
        return `U:${value};`;
    }

    _format_fd_path(path) {
        return `F:P:${this._format_string(path)};;`;
    }

    _format_fd_node_id(node_id) {
        return `F:N:${this._format_uint(node_id)};;`;
    }

    _format_node_id(node_id) {
        return `N:${this._format_uint(node_id)};`;
    }

    _format_block(offset, size) {
        return `BL:${this._format_uint(offset)}${this._format_uint(size)};`;
    }

    _format_transaction_id() {
        let id = this._transaction_id;
        this._transaction_id++;
        return `T:U:${id};;`
    }

    authenticate_with_token(token, callback) {
        this._expected_msg.set_callback_for_rsp(MSG_TYPE_AUTH, callback);

        let msg = this._text_encoder.encode(
            `V:1;${MSG_TYPE_AUTH}:${this._format_transaction_id()}TOKEN:${this._format_string(token)};;E:;`);
        this._socket.send(msg.buffer);
    }

    query_element_children(path, callback) {
        this._expected_msg.set_callback_for_rsp(MSG_TYPE_QUERY_FS_CHILDREN, callback);

        let msg = this._text_encoder.encode(
            `V:1;${MSG_TYPE_QUERY_FS_CHILDREN}:${this._format_transaction_id()}${this._format_fd_path(path)};E:;`);
        this._socket.send(msg.buffer);
    }

    open_file(node_id, open_mode, callback) {
        this._expected_msg.set_callback_for_rsp(MSG_TYPE_OPEN_FILE, callback);

        let mode = null;
        if (open_mode === OpenMode.read) {
            mode = 0;
        } else if (open_mode === OpenMode.edit) {
            mode = 1;
        } else {
            zyn_unhandled();
        }

        let msg = this._text_encoder.encode(
            `V:1;${MSG_TYPE_OPEN_FILE}:${this._format_transaction_id()}${this._format_fd_node_id(node_id)}${this._format_uint(mode)};E:;`);
        this._socket.send(msg.buffer);
    }

    query_system(callback) {
        this._expected_msg.set_callback_for_rsp(MSG_TYPE_QUERY_SYSTEM, callback);

        let msg = this._text_encoder.encode(
            `V:1;${MSG_TYPE_QUERY_SYSTEM}:${this._format_transaction_id()};E:;`);
        this._socket.send(msg.buffer);
    }

    close_file(node_id, callback) {
        this._expected_msg.set_callback_for_rsp(MSG_TYPE_CLOSE_FILE, callback);

        let msg = this._text_encoder.encode(
            `V:1;${MSG_TYPE_CLOSE_FILE}:${this._format_transaction_id()}${this._format_node_id(node_id)};E:;`);
        this._socket.send(msg.buffer);
    }

    delete_element(node_id, callback) {
        this._expected_msg.set_callback_for_rsp(MSG_TYPE_DELETE_ELEMENT, callback);

        let msg = this._text_encoder.encode(
            `V:1;${MSG_TYPE_DELETE_ELEMENT}:${this._format_transaction_id()}${this._format_fd_node_id(node_id)};E:;`);
        this._socket.send(msg.buffer);
    }

    create_file_ra(name, parent_dir, callback) {
        this._expected_msg.set_callback_for_rsp(MSG_TYPE_CREATE_FILE, callback);

        let msg = this._text_encoder.encode(
            `V:1;${MSG_TYPE_CREATE_FILE}:${this._format_transaction_id()}${this._format_fd_path(parent_dir)}`
            + `${this._format_string(name)}${this._format_uint(0)};E:;`);
        this._socket.send(msg.buffer);
    }

    create_directory(name, parent_dir, callback) {
        this._expected_msg.set_callback_for_rsp(MSG_TYPE_CREATE_DIRECTORY, callback);

        let msg = this._text_encoder.encode(
            `V:1;${MSG_TYPE_CREATE_DIRECTORY}:${this._format_transaction_id()}${this._format_fd_path(parent_dir)}`
            + `${this._format_string(name)};E:;`);
        this._socket.send(msg.buffer);
    }

    read_file(node_id, offset, size, callback) {
        this._expected_msg.set_callback_for_rsp(MSG_TYPE_READ, callback);

        let msg = this._text_encoder.encode(
            `V:1;${MSG_TYPE_READ}:${this._format_transaction_id()}${this._format_node_id(node_id)}${this._format_block(offset, size)};E:;`);
        this._socket.send(msg.buffer);
    }

    apply_modifications(node_id, revision, modifications, callback) {
        if (modifications.length > 1) {
            new BatchModificationState(node_id, revision, modifications, callback, this);
        } else {
            new ModificationState(node_id, revision, modifications, callback, this);
        }
    }

    edit_ra_file_preamble_insert(node_id, revision, offset, size, callback) {
        this._expected_msg.set_callback_for_rsp(MSG_HANDLER_EDIT_PREAMBLE, callback);

        let msg = this._text_encoder.encode(
            `V:1;RA-I:${this._format_transaction_id()}${this._format_node_id(node_id)}`
            + `${this._format_uint(revision)}${this._format_block(offset, size)};E:;`);
        this._socket.send(msg.buffer);
    }

    edit_ra_file_delete(node_id, revision, offset, size, callback) {
        this._expected_msg.set_callback_for_rsp(MSG_HANDLER_EDIT, callback);

        let msg = this._text_encoder.encode(
            `V:1;RA-D:${this._format_transaction_id()}${this._format_node_id(node_id)}`
            + `${this._format_uint(revision)}${this._format_block(offset, size)};E:;`);
        this._socket.send(msg.buffer);
    }

    edit_ra_file_preamble_batch_edit(node_id, revision, number_of_operation, callback) {
        this._expected_msg.set_callback_for_rsp(MSG_HANDLER_EDIT_PREAMBLE, callback);

        let msg = this._text_encoder.encode(
            `V:1;RA-BATCH-EDIT:${this._format_transaction_id()}${this._format_node_id(node_id)}`
            + `${this._format_uint(revision)}${this._format_uint(number_of_operation)};E:;`);
        this._socket.send(msg.buffer);
    }

    edit_ra_batch_edit(operation_type, offset, size) {
        let msg = this._text_encoder.encode(`${this._format_uint(operation_type)}${this._format_block(offset, size)}E:;`);
        this._socket.send(msg.buffer);
    }
}

module.exports = Connection

var _socket = null;
var _user_id = 0;
var _tab_id = 0;
var _current_transaction = null;
var _notification_callback = null;
var _disconnected_callback = null;

var ZYN_ERROR_CODE_TRANSACTION_ALREADY_IN_PROGRESS = 1;
var ZYN_ERROR_CODE_SERVER_RESPONDED_WITH_UEXPECTED_MESSAGE = 2;
var ZYN_ERROR_CODE_NOT_INITIALIZED = 2;
var ZYN_ERROR_CODE_ALREADY_INITIALIZED = 2;


class Path {
    constructor() {
        this._path = [];
    }

    to_str() {
        if (this._path.length == 0) {
            return '/';
        }
        return '/' + this._path.join('/');
    }

    is_equal(other) {

        console.log(this._path + ' == ' + other._path);

        if (this._path.length != other._path.length) {
            return false;
        }

        for (var i = 0; i < this._path.length; i++) {
            if (this._path[i] != other._path[i]) {
                return false;
            }
        }
        return true;
    }

    parent() {
        this._path.pop();
    }

    append(name) {
        this._path.push(name);
    }

    clone() {
        var copy = new Path();
        copy._path = this._path.slice();
        return copy;
    }
}

function zyn_edit_file_blob(
    node_id,
    revision,
    type_of_file,
    content_edited,
    transaction,
) {
    if (_start_transaction(transaction) === false) {
        return ;
    }

    _socket.onmessage = _parse_response_and_forward;
    _socket.send(_to_json_message(
        'edit-file',
        {
            'node-id': node_id,
            'revision': revision,
            'type-of-file': type_of_file,
            'content-original': null,
            'content-edited': btoa(content_edited),
        }
    ));
}

function zyn_poll_server()
{
    if (_current_transaction !== null) {
        return ;
    }
    _socket.onmessage = _parse_response_and_forward;
    _socket.send(_to_json_message('poll', {}));
}

function zyn_edit_file_random_access(
    node_id,
    revision,
    type_of_file,
    content_original,
    content_edited,
    transaction,
) {
    if (_start_transaction(transaction) === false) {
        return ;
    }

    _socket.onmessage = _parse_response_and_forward;
    _socket.send(_to_json_message(
        'edit-file',
        {
            'node-id': node_id,
            'revision': revision,
            'type-of-file': type_of_file,
            'content-original': btoa(content_original),
            'content-edited': btoa(content_edited),
        }
    ));
}

function zyn_load_file(node_id, filename, transaction)
{
    if (_start_transaction(transaction) === false) {
        return ;
    }

    _socket.onmessage = _parse_response_and_forward;
    _socket.send(_to_json_message(
        'load-file',
        {
            'node-id': node_id,
            'filename': filename,
        }
    ));
}

function zyn_query_filesystem_element(path, transaction)
{
    if (_start_transaction(transaction) === false) {
        return ;
    }

    _socket.onmessage = _parse_response_and_forward;
    _socket.send(_to_json_message(
        'query-filesystem-element',
        {
            'path': path.to_str(),
        }
    ));

}

function zyn_load_folder_contents(path, transaction)
{
    if (_start_transaction(transaction) === false) {
        return ;
    }

    _socket.onmessage = _parse_response_and_forward;
    _socket.send(_to_json_message(
        'list-directory-content',
        {
            'path': path.to_str(),
        }
    ));
}

function _handle_register_response(websocket_msg)
{
    var msg = JSON.parse(websocket_msg.data);
    if (_handle_notification_msg(msg)) {
        return ;
    }
    var transaction = _reset_current_transaction();

    if (msg['type'] != 'register-rsp') {
        transaction.on_error(ZYN_ERROR_CODE_SERVER_RESPONDED_WITH_UEXPECTED_MESSAGE, null, null);
        return ;
    }

    if (_handle_error_rsp(msg, transaction)) {
        return ;
    }

    _tab_id = msg['tab-id'];
    transaction.on_success(msg['content']);
}

function zyn_init(user_id, notification_callback, disconnected_callback, transaction)
{
    if (_user_id === null) {
        transaction.on_error(ZYN_ERROR_CODE_ALREADY_INITIALIZED, null, null);
        return ;
    }

    _user_id = user_id;
    _notification_callback = notification_callback;
    _disconnected_callback = disconnected_callback;
    _current_transaction = transaction;

    var url = new URL(window.location.href)
    var protocol = "ws";
    if (location.protocol === 'https:') {
        protocol = "wss";
    }
    var websocket_url = protocol + '://' + url.hostname + ":" + url.port + "/websocket";

    _socket = new WebSocket(websocket_url);
    _socket.onmessage = _handle_register_response;
    _socket.onclose = _handle_on_close;
    _socket.onopen = function () {
        _socket.send(_to_json_message('register', null));
    };
}

function zyn_log_on_server(message, level='debug')
{
    var supported_levels = ['debug', 'info']
    if (supported_levels.indexOf(level) == -1) {
        console.log('Invalid log level: ' + level);
        return
    }

    _socket.send(_to_json_message(
        'log',
        {
            'level': level,
            'message': message,
        }
    ));
}

function _handle_notification_msg(msg) {
    if (msg['type'] === 'notification') {
        _notification_callback(msg['notification']);
        return true;
    }
    return false;
}


function _handle_error_rsp(msg, transaction) {
    if ('error' in msg) {
        web = msg['error']['web-server-error'];
        zyn = msg['error']['zyn-server-error'];

        if (web === '') {
            web = null;
        }

        if (zyn === '') {
            zyn = null;
        }

        transaction.on_error(null, web, zyn);
        return true;
    }
    return false;
}

function _to_json_message(msg_type, content)
{
    var msg = {
        'user-id': _user_id,
        'tab-id': _tab_id,
        'type': msg_type,
    };

    if (content != null) {
        msg['content'] = content;
    }

    return JSON.stringify(msg);
}

function _start_transaction(transaction)
{
    if (_socket == null) {
        transaction.on_error(ZYN_ERROR_CODE_NOT_INITIALIZED, null, null);
        return false;
    }

    if (_current_transaction != null) {
        transaction.on_error(ZYN_ERROR_CODE_TRANSACTION_ALREADY_IN_PROGRESS, null, null);
        return false;
    }

    _current_transaction = transaction;
    return true;
}

function _reset_current_transaction()
{
    var transaction = _current_transaction;
    _current_transaction = null;
    return transaction;
}

function _parse_response_and_forward(websocket_msg)
{
    var msg = JSON.parse(websocket_msg.data);
    if (_handle_notification_msg(msg)) {
        return ;
    }
    transaction = _reset_current_transaction();
    if (_handle_error_rsp(msg, transaction)) {
        return ;
    }
    transaction.on_success(msg['content']);
}

function _handle_on_close()
{
    _disconnected_callback();
}

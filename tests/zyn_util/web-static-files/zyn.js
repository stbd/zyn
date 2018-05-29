var _user_id = 0;
var _tab_id = 0;
var _unhandled_messages = [];
var _socket = null;
var _callback_for_success = null;
var _callback_for_error = null;
var _transaction_ongoing = false;
var _current_path = [];

var _current_transaction = null;

var ZYN_ERROR_CODE_TRANSACTION_ALREADY_IN_PROGRESS = 1;


function zyn_current_directory_path()
{
    if (_current_path.length == 0) {
        return '/'
    }
    return '/' + _current_path.join('/');
}

function zyn_current_directory_parent_path()
{
    if (_current_path.length == 0) {
        return '/';
    }

    var parent = _current_path.slice();
    parent.pop();
    if (parent.length == 0) {
        return '/';
    }
    return '/' + parent.join('/');
}

function _handle_change_current_directory_response(websocket_msg)
{
    var msg = JSON.parse(websocket_msg.data);
    var path = String(msg['path']);
    var exists = Boolean(msg['exists']);
    _transaction_ongoing = false;
    _callback_for_success(path, exists);
    if (exists) {
        var elements = path.split('/').filter(element => element.length > 0);
        _current_path = elements;
    }
}

function zyn_change_current_directory(path, callback_for_success, callback_for_error)
{
    if (_transaction_ongoing) {
        callback_for_error(ZYN_ERROR_CODE_TRANSACTION_ALREADY_IN_PROGRESS);
        return ;
    }

    // console.log('changing to ' +  path)
    _callback_for_error = callback_for_error;
    _transaction_ongoing = true;
    _callback_for_success = callback_for_success;
    _socket.onmessage = _handle_change_current_directory_response
    _socket.send(_to_json_message(
        'test-path-exists-and-is-directory',
        {
            'path': path,
        }
    ));
}

function zyn_edit_file(
    node_id,
    revision,
    content_original,
    content_edited,
    callback_for_success,
) {
    if (_socket == null) {
        return ;
    }

    if (_transaction_ongoing) {
        callback_for_error(ZYN_ERROR_CODE_TRANSACTION_ALREADY_IN_PROGRESS);
        return ;
    }

    _transaction_ongoing = true;
    _callback_for_success = callback_for_success;
    _socket.onmessage = _parse_response_and_forward;
    _socket.send(_to_json_message(
        'edit-file',
        {
            'node-id': node_id,
            'revision': revision,
            'content-original': btoa(content_original),
            'content-edited': btoa(content_edited),
        }
    ));
}

function _handle_load_file_response(websocket_msg)
{
    _transaction_ongoing = false;
    var msg = JSON.parse(websocket_msg.data);
    var content = atob(msg['content']);
    var node_id = Number(msg['node-id']);
    var revision = Number(msg['revision']);
    var filename = String(msg['filename']);

    // console.log('loading file, l ' + content.length, _callback_for_success);

    if (_callback_for_success != null) {
        _callback_for_success(node_id, filename, revision, content);
    }
}

function zyn_load_file(node_id, filename, callback_for_success)
{
    if (_socket == null) {
        return ;
    }

    if (_transaction_ongoing) {
        callback_for_error(ZYN_ERROR_CODE_TRANSACTION_ALREADY_IN_PROGRESS);
        return ;
    }

    _transaction_ongoing = true;
    _callback_for_success = callback_for_success;
    _socket.onmessage = _handle_load_file_response;
    _socket.send(_to_json_message(
        'load-file',
        {
            'node-id': node_id,
            'filename': filename,
        }
    ));
}

function _zyn_load_folder_contents(path, callback_for_success, callback_for_error) {
    if (_socket == null) {
        return ;
    }

    if (_transaction_ongoing) {
        callback_for_error(ZYN_ERROR_CODE_TRANSACTION_ALREADY_IN_PROGRESS);
        return ;
    }

    _transaction_ongoing = true;
    _callback_for_success = callback_for_success;
    _callback_for_error = callback_for_error;
    _socket.onmessage = _parse_response_and_forward;
    _socket.send(_to_json_message(
        'list-directory-content',
        {
            'path': path,
        }
    ));
}


function _start_transaction(transaction)
{
    if (_socket == null) {
        transaction.on_error(); // todo
        return false;
    }

    if (_current_transaction != null) {
        transaction.on_error(ZYN_ERROR_CODE_TRANSACTION_ALREADY_IN_PROGRESS);
        return false;
    }

    _current_transaction = transaction;
    return true;
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
            'path': path,
        }
    ));
}


function _handle_register_response(websocket_msg)
{
    var transaction = _reset_current_transaction();
    var msg = JSON.parse(websocket_msg.data);

    if (msg['type'] != 'register-rsp') {
        _handle_unrecoverable_error();
        return ;
    }

    _tab_id = msg['tab-id'];
    transaction.on_success(msg);
}

function zyn_init(user_id, transaction)
{
    _user_id = user_id;
    _current_transaction = transaction;

    var url = new URL(window.location.href)
    var protocol = "ws";
    if (location.protocol === 'https:') {
        protocol = "wss";
    }
    var websocket_url = protocol + '://' + url.hostname + ":" + url.port + "/websocket";

    _socket = new WebSocket(websocket_url);
    _socket.onmessage = _handle_register_response;
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

function _handle_unrecoverable_error()
{
    _socket.close();
    socket = null;
    alert('Unrecoverable error');
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

function _reset_current_transaction()
{
    var transaction = _current_transaction;
    _current_transaction = null;
    return transaction;
}

function _parse_response_and_forward(websocket_msg)
{
    transaction = _reset_current_transaction();
    var msg = JSON.parse(websocket_msg.data);
    transaction.on_success(msg);
}

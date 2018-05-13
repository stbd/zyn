var _user_id = 0;
var _tab_id = 0;
var _unhandled_messages = [];
var _socket = null;
var _callback_for_success = null;
var _callback_for_error = null;
var _transaction_ongoing = false;
var _current_path = [];


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

function _handle_load_file_response(websocket_msg)
{
    _transaction_ongoing = false;
    var msg = JSON.parse(websocket_msg.data);
    var content = atob(String(msg['content']));
    var node_id = Number(msg['node-id']);
    var filename = String(msg['filename']);

    // console.log('loading file, l ' + content.length, _callback_for_success);

    if (_callback_for_success != null) {
        _callback_for_success(node_id, filename, content);
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

function zyn_load_folder_contents(path, callback_for_success, callback_for_error) {
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

function _handle_register_response(websocket_msg)
{
    _transaction_ongoing = false;
    var msg = JSON.parse(websocket_msg.data);

    if (msg['type'] != 'register-rsp') {
        _handle_unrecoverable_error();
        return ;
    }

    _tab_id = msg['tab-id'];
    if (_callback_for_success != null) {
        _callback_for_success(msg);
    }
}

function zyn_init(user_id, callback_for_success, callback_for_error) {
    _user_id = user_id;

    var url = new URL(window.location.href)
    var websocket_url = "ws://" + url.hostname + ":" + url.port + "/websocket"

    _socket = new WebSocket(websocket_url);
    _callback_for_success = callback_for_success;
    _callback_for_error = callback_for_error;
    _transaction_ongoing = true;
    _socket.onmessage = _handle_register_response;

    _socket.onopen = function () {
        _socket.send(_to_json_message('register', null));
    };
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

function _parse_response_and_forward(websocket_msg)
{
    _transaction_ongoing = false;
    var msg = JSON.parse(websocket_msg.data);
    if (_callback_for_success != null) {
        _callback_for_success(msg);
    }
}

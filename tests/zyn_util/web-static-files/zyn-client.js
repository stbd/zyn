let ElementType = Object.freeze({
    'file': 'file',
    'directory': 'dir',
});

let ZynFileType = Object.freeze({
    'ra': 'random-access',
    'blob': 'blob',
});

let OpenMode = Object.freeze({
    'read': 'read',
    'edit': 'write',
})

class ZynClient {
    constructor(
        user_id,
        url_root,
        path,
        notification_callback,
        init_callback,
    ) {
        this._user_id = user_id;
        this._tab_id = 0;
        this._url_root = url_root;
        this._working_directory = null;
        this._children = [];
        this._socket = null;
        this._callback = null;
        this._file_handlers = [
            {'types': ['md'], 'handler': ZynMarkdownHandler},
            {'types': ['pdf'], 'handler': ZynPdfHandler},
        ];
        this._current_file_handler = null;
        this._file_mode = OpenMode.read;
        this._target_id = 'zyn-client-target';
        this._poll_timer = null;
        this._notification_callback = notification_callback;

        this.TIMER_INTERVAL_NORMAL = 5000;
        this.TIMER_INTERVAL_PROMPT = 1000;

        let url = zyn_websocket_server_url();
        console.log(`Connecting to server at ${url}`);

        this._socket = new WebSocket(url);
        this._socket.onmessage = (msg) => { this.handle_socket_message(msg) };
        this._socket.onclose = () => { this.handle_socket_close(); };
        this._callback = (msg) => { this.handle_register_rsp(msg, path, init_callback); }
        this._socket.onopen = () => {
            this._socket.send(this.json_message('register', null));
        }
    }

    root_url() { return this._url_root; }
    working_directory() { return this._working_directory.clone(); }
    children() { return this._children; }
    has_current_file() { return this._current_file_handler !== null; }
    current_file_handler() { return this._current_file_handler; }
    is_in_edit_mode() { return this._file_mode == OpenMode.edit; }
    is_in_read_mode() { return this._file_mode == OpenMode.read; }
    file_mode() { return this._file_mode; }
    is_editable(name) { return this.filetype_handler(name).is_editable(); }

    handle_socket_close(error) {
        this.stop_poll_timer();
        zyn_show_modal_error(ErrorLevel.error, "Connection to server lost");
    }

    stop_poll_timer() {
        if (this._poll_timer !== null) {
            clearTimeout(this._poll_timer);
            this._poll_timer = null;
        }
    }

    set_poll_timer_interval(interval) {
        this.stop_poll_timer();
        this._poll_timer = setInterval(
            () => this.handle_poll_timer_timeout(),
            interval,
        );
    }

    handle_poll_timer_timeout() {
        this.send_msg(
            'check-notifications',
        );
    }

    get_child(name) {
        for (let c of this._children) {
            if (c['name'] === name) {
                return c;
            }
        }
        return null;
    }

    file_content_edited() {
        if (this.has_current_file()) {
            this._current_file_handler.content_edited();
        }
    }

    filetype_handler(filename) {
        var split_name = filename.split('.');
        if (split_name.length === 1) {
            return ZynFileHandler;
        }
        let type = split_name[split_name.length - 1].toLowerCase();
        for (let handler of this._file_handlers) {
            if (handler.types.indexOf(type) != -1) {
                return handler.handler;
            }
        }
        return ZynFileHandler;
    }

    is_transaction_active() {
        return this._callback !== null;
    }

    reset_transaction() {
        let callback = this._callback;
        this._callback = null;
        return callback;
    }

    json_message(msg_type, content) {
        var msg = {
            'user-id': this._user_id,
            'tab-id': this._tab_id,
            'type': msg_type,
        };
        if (content != null) {
            msg['content'] = content;
        }
        return JSON.stringify(msg);
    }

    send_msg(msg_type, content) {
        this._socket.send(this.json_message(
            msg_type,
            content,
        ));
    }

    handle_socket_message(websocket_msg) {
        let msg = new ZynMessage(JSON.parse(websocket_msg.data));

        if (this.handle_notification(msg)) {
            return ;
        }

        if (!this.is_transaction_active()) {
            console.log('Discarding unexpected message');
            return
        }

        let callback = this.reset_transaction();
        callback(msg);
    }

    log_on_server(message, level='debug') {
        var supported_levels = ['debug', 'info']
        if (supported_levels.indexOf(level) == -1) {
            console.log('Invalid log level: ' + level);
            return
        }

        this.send_msg(
            'log',
            {
                'level': level,
                'message': message,
            },
        );
    }

    handle_notification(msg) {
        if (msg.is_notification()) {
            let n = msg.notification();
            zyn_add_notification(n.source(), n.to_string());
            if (n.is_disconnect()) {
                this.stop_poll_timer();
            } else if (n.is_file_edit()) {
                if (!this.has_current_file()) {
                    zyn_unhandled();
                }
                let node_id = this._current_file_handler.properties()['node-id'];
                if (node_id != n.content()['node-id']) {
                    zyn_unhandled();
                }
                this._current_file_handler.handle_notification(n);
                this._current_file_handler.render(
                    this._file_mode,
                    this._target_id,
                    null,
                );
                this._notification_callback();
            }
            return true;
        }
        return false;
    }

    execute_command(msg_type, content, callback) {
        this._callback = callback;
        this.send_msg(msg_type, content);
    }

    create_ra_file(name, callback) {
        this._callback = callback;
        this.send_msg(
            'create-element',
            {
                'name': name,
                'path-parent': this._working_directory.to_str(),
                'element-type': ElementType.file,
                'file-type': ZynFileType.ra,
            }
        );
    }

    create_directory(name, callback) {
        this._callback = callback;
        this.send_msg(
            'create-element',
            {
                'name': name,
                'path-parent': this._working_directory.to_str(),
                'element-type': ElementType.directory,
            }
        );
    }

    refresh_working_directory(callback) {
        this.change_working_directory(
            this._working_directory.clone(),
            callback,
        );
    }

    handle_register_rsp(msg, path, callback) {
        this._tab_id = msg.msg()['tab-id'];
        this.log_on_server('Client ready');
        this.change_working_directory(path, callback);
        this.set_poll_timer_interval(this.TIMER_INTERVAL_NORMAL);
    }

    change_working_directory(path, callback) {

        this._callback = (msg) => {
            this.handle_change_working_directory_rsp(
                path,
                msg,
                callback
            );
        };
        this.send_msg(
            'list-directory-content',
            {
                'path': path.to_str(),
            }
        );
    }

    handle_change_working_directory_rsp(path, msg, callback) {
        this._children = [];
        if (!msg.is_error()) {
            this._working_directory = path;
            this._children = msg.content()['elements'];
        }
        callback(msg);
    }

    set_content_area_text(text, target_id) {
        this.reset_file_content(OpenMode.read, target_id);

        let content = document.getElementById(this._target_id);
        let text_content = document.createElement('div');
        text_content.innerText = text
        text_content.style.fontSize = '20px';
        text_content.style.fontWeight = 'bold';
        text_content.style.color = 'gray';
        text_content.style.top = '50%';
        text_content.style.left = '50%';
        text_content.style.position = 'absolute';
        content.appendChild(text_content);
    }

    redraw_content(height=null) {
        let content = document.getElementById(this._target_id);
        content.style.height = `${height}px`;
    }

    reset_file_content(mode, target_id) {
        let target = document.getElementById(target_id);
        while (target.childElementCount > 0) {
            target.removeChild(target.childNodes[0])
        }

        if (mode === OpenMode.edit) {

            let content = document.createElement('pre');
            content.id = this._target_id;
            content.contentEditable  = 'true';
            content.style.overflow = 'auto';
            content.onkeyup = () => { this.file_content_edited(); };
            target.appendChild(content)

        } else if (mode === OpenMode.read) {

            let content = document.createElement('div');
            content.id = this._target_id;
            content.style.display = 'inline-block';
            content.style.overflow = 'auto';
            content.style.width = '100%';
            target.appendChild(content)

        } else {
            _zyn_unhandled();
        }

        let height = document.getElementById(target_id).offsetHeight
        this.redraw_content(height);
    }

    close_file(callback) {
        if (!this.has_current_file()) {
            return ;
        }

        let _close = () => {
            this._current_file_handler.close(callback);
            this._current_file_handler = null;
        };
        if (this._current_file_handler.mode() == OpenMode.edit) {
            if (this._current_file_handler.has_edits()) {
                zyn_show_modal_ask(
                    'File has changes, Would you like to save them?',
                    {
                        'Yes': () => {
                            this._current_file_handler.save(
                                this._target_id,
                                () => {
                                    zyn_hide_modals();
                                    this.close_file(callback);
                                },
                            );
                        },
                        'No': () => {
                            zyn_hide_modals();
                            _close();
                        },
                    },
                );
                return ;
            }
        }
        _close();
    }

    open_file(node_id, name, mode, target_id, callback) {
        if (this.has_current_file()) {
            let properties = this._current_file_handler.properties();
            if (properties['node-id'] === node_id) {
                if (this._current_file_handler.mode() !== mode) {
                    this.change_file_to_mode(mode, target_id, callback);
                } else {
                    callback();
                }
            } else {
                this.close_file(() => {
                    this.open_file(node_id, name, mode, target_id, callback);
                });
            }
            return ;
        }

        this.reset_file_content(mode, target_id);
        this._file_mode = mode;
        let handler_type = this.filetype_handler(name);
        this._current_file_handler = new handler_type(node_id, name, this);
        this._current_file_handler.open(
            mode,
            () => {
                this._current_file_handler.initial_load(
                    () => {
                        this._current_file_handler.render(
                            this._file_mode,
                            this._target_id,
                            callback,
                        );
                    }
                );
            },
        );
    }

    change_file_to_mode(mode, target_id, callback) {
        let target = document.getElementById(target_id);
        if (!this.has_current_file()) {
            _zyn_unhandled();
            return ;
        }

        if (this._file_mode === mode) {
            callback();
            return ;
        }

        if (mode === OpenMode.edit) {
            if (!this._current_file_handler.is_editable()) {
                _zyn_unhandled();
                return ;
            }
        } else if (mode === OpenMode.read) {
            if (this._current_file_handler.has_edits()) {
                zyn_show_modal_ask(
                    'File has changes, save or cancel changes?',
                    {
                        'Ok': () => { zyn_hide_modals(); },
                    },
                );
                return ;
            }
        } else {
            _zyn_unhandled();
        }

        this.reset_file_content(mode, target_id);
        this._current_file_handler.change_file_mode(
            mode,
            () => {
                this._file_mode = mode;
                this._current_file_handler.render(
                    mode,
                    this._target_id,
                    callback,
                )
            }
        );
    }

    save_file_edits(callback) {
        if (!this.has_current_file()) {
            _zyn_unhandled();
            return ;
        }
        if (!this._current_file_handler.is_editable()) {
            _zyn_unhandled();
            return ;
        }
        this._current_file_handler.save(this._target_id, callback);
    }

    cancel_file_edits(callback) {
        if (!this.has_current_file()) {
            _zyn_unhandled();
            return ;
        }
        if (!this._current_file_handler.is_editable()) {
            _zyn_unhandled();
            return ;
        }
        this._current_file_handler.render(
            this._file_mode,
            this._target_id,
            callback,
        );
    }
}

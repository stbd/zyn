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
        url_root,
        notification_callback,
        connection,
        file_handlers,
    ) {
        this._url_root = url_root;
        this._notification_callback = notification_callback;
        this._connection = connection;

        this._working_directory = null;
        this._children = [];
        this._file_handlers = file_handlers,
        this._current_file_handler = null;
        this._file_mode = OpenMode.read;
        this._target_id = 'zyn-client-target';

        this._connection.register_event_handlers(
            (e) => this._handle_socket_closed(e),
            (e) => this._handle_socket_error(e),
            (n) => this._handle_notification(n),
        )
    }

    connection() { return this._connection; }
    root_url() { return this._url_root; }
    working_directory() { return this._working_directory.clone(); }
    children() { return this._children; }
    has_current_file() { return this._current_file_handler !== null; }
    current_file_handler() { return this._current_file_handler; }
    is_in_edit_mode() { return this._file_mode == OpenMode.edit; }
    is_in_read_mode() { return this._file_mode == OpenMode.read; }
    file_mode() { return this._file_mode; }
    is_editable(name) { return this.filetype_handler(name).is_editable(); }

    _handle_socket_closed(event) {
        console.lo(event);
        zyn_show_modal_error(ErrorLevel.error, `Connection to server lost`);
    }

    _handle_socket_error(event) {
        console.lo(event);
    }

    _handle_notification(notification) {
        if (notification.is_disconnect()) {
            zyn_add_notification(NotificationSource.server, notification.to_string());
        } else if (notification.is_edit()) {
            zyn_add_notification(NotificationSource.server, notification.to_string());
            if (!this.has_current_file()) {
                zyn_unhandled();
            }
            let node_id = this._current_file_handler.properties()['node-id'];
            if (node_id !== notification.node_id) {
                zyn_unhandled();
            }
            this._current_file_handler.handle_notification(notification, this._file_mode, this._target_id);

        } else {
            console.log('Unhandeld notification', notification);
            zyn_unhandled();
        }
    }

    set_focus() {
        document.getElementById(this._target_id).focus();
    }

    handle_notification(msg) {
        if (msg.is_notification()) {
            let n = msg.notification();
            if (n.is_disconnect()) {
                zyn_add_notification(n.source(), n.to_string());
                this.stop_poll_timer();
            } else if (n.is_file_edit()) {
                zyn_add_notification(n.source(), n.to_string());
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
            } else if (n.is_progress_update()) {
                if (zyn_is_modal_loading_vissible()) {
                    let desc = n.content()['description'];
                    zyn_show_modal_loading(desc);
                } else {
                    zyn_add_notification(n.source(), n.to_string());
                }
            }
            return true;
        }
        return false;
    }

    change_working_directory(path, callback) {
        this._connection.query_element_children(path.to_str(), (rsp) => {
            if (!rsp.is_error()) {
                this._working_directory = path;
                this._children = rsp.elements;
            } else {
                zyn_show_modal_error(ErrorLevel.error, `Failed to query elements ${path.to_str()}`);
            }
            callback(rsp);
        });
    }

    refresh_working_directory(callback) {
        this.change_working_directory(
            this._working_directory.clone(),
            callback,
        );
    }

    get_wd_child(name) {
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

    query_system_properties(callback) {
        this._connection.query_system(callback);
    }

    delete_element(node_id, callback) {
        this._connection.delete_element(node_id, callback);
    }

    create_ra_file(name, callback) {
        this._connection.create_file_ra(name, this._working_directory.to_str(), callback);
    }

    create_directory(name, callback) {
        this._connection.create_directory(name, this._working_directory.to_str(), callback);
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
            zyn_unhandled();
        }

        let height = document.getElementById(target_id).offsetHeight
        this.redraw_content(height);
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
            zyn_unhandled();
            return ;
        }

        if (this._file_mode === mode) {
            callback();
            return ;
        }

        if (mode === OpenMode.edit) {
            if (!this._current_file_handler.is_editable()) {
                zyn_unhandled();
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
            zyn_unhandled();
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
            zyn_unhandled();
            return ;
        }
        if (!this._current_file_handler.is_editable()) {
            zyn_unhandled();
            return ;
        }
        this._current_file_handler.save(this._target_id, callback);
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

    cancel_file_edits(callback) {
        if (!this.has_current_file()) {
            zyn_unhandled();
            return ;
        }
        if (!this._current_file_handler.is_editable()) {
            zyn_unhandled();
            return ;
        }
        this._current_file_handler.render(
            this._file_mode,
            this._target_id,
            callback,
        );
    }
}

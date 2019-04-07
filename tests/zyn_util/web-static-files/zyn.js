function zyn_websocket_server_url() {
    var url = new URL(window.location.href)
    var protocol = "ws";
    if (location.protocol === 'https:') {
        protocol = "wss";
    }
    return protocol + '://' + url.hostname + ":" + url.port + "/websocket";
}

function zyn_sort_filesystem_elements(elements, name_filter) {
    let dirs = [];
    let files = [];

    function _sort_by_name(a, b) {
        let name_a = a['name'].toLowerCase()
        let name_b = b['name'].toLowerCase()
        if (name_a < name_b) {return -1;}
        if (name_a > name_b) {return 1;}
        return 0;
    }

    for (let e of elements) {
        if (name_filter !== null && e['name'].indexOf(name_filter) === -1) {
            continue ;
        }
        if (e['element-type'] == ElementType.file) {
            files.push(e);
        } else if (e['element-type'] == ElementType.directory) {
            dirs.push(e);
        } else {
            zyn_unhandled();
        }
    }

    files.sort(_sort_by_name);
    dirs.sort(_sort_by_name);
    return files.concat(dirs);
}

class ZynError {
    constructor(client=null, web_server=null, zyn_server=null) {
        this.client = client;
        this.web_server = web_server;
        this.zyn_server = zyn_server;
        this._validate();
    }

    _validate() {
        if (
            this.client === null
            && this.web_server === null
            && this.zyn_server === null
        ) {
            throw "No error defined";
        }
    }

    to_string() {
        if (this.client !== null) {
            return `Client: ${this.client}`;
        } else if (this.web_server !== null) {
            return `Web server: ${this.web_server}`;
        } else if (this.zyn_server !== null) {
            return `Zyn server: ${this.zyn_server}`;
        }
    }
}

class ZynMessage {
    constructor(msg) {
        this._msg = msg;
    }

    msg() {
        return this._msg;
    }

    is_error() {
        return 'error' in this._msg;
    }

    error() {
        console.log(this._msg)
        return new ZynError(
            null,
            this._msg['error']['web-server-error'],
            this._msg['error']['zyn-server-error'],
        );
    }

    is_notification() {
        return 'notification' in this._msg;
    }

    notification() {
        return new ZynNotification(this);
    }

    content() {
        return this._msg['content'];
    }
}

class ZynPath {
    constructor() {
        this._path = [];
    }

    static from_string(path) {
        let p = new ZynPath();
        for (let s of path.split('/')) {
            let t = s.trim();
            if (t.length != 0) {
                p.append(t);
            }
        }
        return p;
    }

    is_root() {
        if (this._path.length == 0) {
            return true;
        } else {
            return false;
        }
    }

    to_str() {
        if (this.is_root()) {
            return '/';
        }
        return '/' + this._path.join('/');
    }

    is_equal(other) {

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
        var copy = new ZynPath();
        copy._path = this._path.slice();
        return copy;
    }
}

class ZynNotification {
    constructor(msg) {
        this._notification = msg.msg()['notification'];
    }

    type() {
        return this._notification['type']
    }

    source() {
        return this._notification['source']
    }

    content() {
        return this._notification['content']
    }

    is_disconnect() {
        return this.type() === 'disconnected';
    }

    is_file_edit_modified() {
        return this.type() === 'random-access-modification';
    }

    is_file_edit_insert() {
        return this.type() === 'random-access-insert';
    }

    is_file_edit_delete() {
        return this.type() === 'random-access-delete';
    }

    is_file_edit() {
        return this.is_file_edit_modified() ||
               this.is_file_edit_insert() ||
               this.is_file_edit_delete();
    }

    to_string() {
        if (this.is_disconnect()) {
            return `Server stopped: ${this.content()['reason']}`;
        } else if (this.is_file_edit) {
            let c = this.content();
            return `File ${c['node-id']} updated to revision ${c['revision']}, type: ${this.type()}`;
        } else if (this.type() === 'reconnect') {
            return `Reconnecting, trial: ${this.content()['trial']}`;
        } else {
            return `Unhandled notification: ${this.type()}`;
        }
    }
}

class ZynFileHandler {
    constructor(node_id, filename, client) {
        this._client = client;
        this._node_id = node_id,
        this._name = filename;
        this._revision = null;
        this._block_size = null;
        this._size = null;
        this._file_type = null;
        this._mode = null;
        this.content = null;
    }
    static is_editable() { return false; }
    is_editable() { return false; }
    mode() { return this._mode; }

    properties() {
        return {
            'name': this._name,
            'node-id': this._node_id,
            'revision': this._revision,
            'block-size': this._block_size,
            'file-type': this._file_type,
        }
    }

    open(mode, callback) {
        this._client.execute_command(
            'open-file',
            {
                'node-id': this._node_id,
                'mode': mode,
            },
            (msg) => {
                this._mode = mode;
                this.handle_open_rsp(msg, callback);
            },
        );
    }

    close(callback) {
        this._client.execute_command(
            'close-file',
            {
                'node-id': this._node_id,
            },
            (msg) => this.handle_open_rsp(msg, callback),
        );
    }

    initial_load(callback) {
        if (this._size === 0) {
            this._content = '';
            callback();
            return ;
        }

        this.read(0, this._size, (msg) => {
            let bytes = msg.content()['bytes'];
            this._content = atob(bytes);
            callback();
        });
    }

    change_file_mode(mode, callback) {
        if (mode === this._mode) {
            callback();
            return ;
        }

        // Do not change to read from edit, to avoid
        // extra operation
        if (this._mode === OpenMode.edit) {
            callback();
            return ;
        }
        this.close(() => this.open(mode, callback));
    }

    handle_open_rsp(msg, callback) {
        let content = msg.content();
        this._revision = content['revision'];
        this._size = content['size'];
        this._block_size = content['block-size'];
        this._file_type = content['file-type'];
        callback();
    }

    read(offset, size, callback) {
        this._client.execute_command(
            'read-file',
            {
                'node-id': this._node_id,
                'offset': offset,
                'size': size,
            },
            callback,
        );
    }

    render(mode, target_id, callback=null) {
        let element = document.getElementById(target_id);
        if (this._content.length === 0) {
            this.render_empty_file(target_id);
        } else {
            element.innerHTML = this._content;
        }
        callback();
    }

    render_empty_file(target_id) {
        this._client.set_content_area_text(
            'Empty',
            target_id
        );
    }
}

class ZynPdfHandler {
    constructor(node_id, filename, client) {
        this._base = new ZynFileHandler(node_id, filename, client);
        this._content = null;
    }

    static is_editable() { return false; }
    is_editable() { return false; }
    properties() { return this._base.properties(); }
    mode() { return this._base.mode(); }
    close(callback) { this._base.close(callback); }

    open(mode, callback) {
        this._base.open(
            mode,
            () => {
                this.load_full_file(callback);
            },
        );
    }

    initial_load(callback) {
        this.load_full_file(callback);
    }

    load_full_file(callback) {
        if (this._base._size === 0) {
            this._content = '';
            callback();
            return ;
        }

        this._base.read(0, this._base._size, (msg) => {
            let bytes = msg.content()['bytes'];
            this._content = atob(bytes);
            callback();
        });
    }

    render(mode, target_id, callback=null) {
        let element = document.getElementById(target_id);
        if (mode !== OpenMode.read) {
            _zyn_unhandled();
            return ;
        }

        var root = document.createElement('div');
        var loadingTask = pdfjsLib.getDocument({data: this._content});
        loadingTask.promise.then(function(pdf) {
            var pageNumber = 1;
            for (var pageNumber = 1; pageNumber <= pdf.numPages; pageNumber++) {
                pdf.getPage(pageNumber).then(function(page) {
                    var scale = 1.5;
                    var viewport = page.getViewport(scale);
                    var canvas = document.createElement('canvas');

                    root.appendChild(canvas);
                    var context = canvas.getContext('2d');
                    canvas.height = viewport.height;
                    canvas.width = viewport.width;
                    var renderContext = {
                        canvasContext: context,
                        viewport: viewport
                    };
                    var renderTask = page.render(renderContext);
                    renderTask.then(function () {});
                });
            }
        }, function (reason) {
            console.error(reason);
        });
        element.appendChild(root);

        if (callback !== null) {
            callback();
        }
    }
}

class ZynMarkdownHandler {
    constructor(node_id, filename, client) {
        this._base = new ZynFileHandler(node_id, filename, client);
        this._is_edited = false;
        this._content =  null;
    }

    static is_editable() { return true; }
    is_editable() { return true; }
    properties() { return this._base.properties(); }
    has_edits() { return this._is_edited; }
    mode() { return this._base.mode(); }
    change_file_mode(mode, callback) { this._base.change_file_mode(mode, callback); }
    close(callback) { this._base.close(callback); }
    content_edited() { this._is_edited = true; }

    update_content(content, revision) {
        this._base._revision = revision;
        this._base._size = content.length;
        this._is_edited = false;
        this._content = content;
    }

    open(mode, callback) {
        this._base.open(
            mode,
            () => {
                this.load_full_file(callback);
            },
        );
    }

    initial_load(callback) {
        this.load_full_file(callback);
    }

    load_full_file(callback) {
        if (this._base._size === 0) {
            this._content = '';
            callback();
            return ;
        }

        this._base.read(0, this._base._size, (msg) => {
            let bytes = msg.content()['bytes'];
            this._content = utf8.decode(atob(bytes));
            callback();
        });
    }

    render(mode, target_id, callback=null) {
        let element = document.getElementById(target_id);
        if (mode === OpenMode.read) {
            let content = this._content;
            if (content.length === 0) {
                this._base.render_empty_file(target_id);
            } else {
                var converter = new showdown.Converter({
                    'simplifiedAutoLink': true,
                    'tables': true,
                });
                let html = converter.makeHtml(content);
                element.innerHTML = html;
            }
        } else if (mode === OpenMode.edit) {
            let content = this._content;
            element.classList.add('w3-pale-green');
            element.innerText = content;
        } else {
            _zyn_unhandled();
        }

        this._is_edited = false;
        if (callback !== null) {
            callback();
        }
    }

    save(source_id, callback) {
        let element = document.getElementById(source_id);
        let modified_content = element.innerText;
        let modifications = []
        let offset = 0
        var diff = JsDiff.diffChars(this._content, modified_content);

        for (let d of diff) {
            let bytes = utf8.encode(d.value);
            if (d.added) {
                modifications.push({
                    'type': 'add',
                    'offset': offset,
                    'bytes': btoa(bytes),
                });
                offset += bytes.length;
            } else if (d.removed) {
                modifications.push({
                    'type': 'delete',
                    'offset': offset,
                    'size': bytes.length,
                });
            } else {
                offset += bytes.length
            }
        }
        if (modifications.length == 0) {
            callback();
            return ;
        }

        this._base._client.execute_command(
            'modify-file',
            {
                'node-id': this._base._node_id,
                'revision': this._base._revision,
                'modifications': modifications,
            },
            (msg) => {
                this.update_content(
                    modified_content,
                    msg.content()['revision'],
                );
                callback();
            }
        );
    }

    handle_notification(notification, mode, target_id, callback) {
        let offset = notification.content()['offset'];
        let bytes = utf8.encode(this._content);

        if (notification.is_file_edit_delete()) {

            let size = notification.content()['size'];
            bytes = bytes.slice(0, offset)
                  + bytes.slice(offset + size, bytes.length);

        } else if (notification.is_file_edit_insert()) {

            let edited = atob(notification.content()['bytes']);
            bytes = bytes.slice(0, offset)
                  + edited
                  + bytes.slice(offset, bytes.length);

        } else if (notification.is_file_edit_modified()) {

            let edited = atob(notification.content()['bytes']);
            bytes = bytes.slice(0, offset)
                  + edited
                  + bytes.slice(offset + edited.length, bytes.length);

        } else {
            zyn_unhandled();
        }
        this._base._revision = notification.content()['revision'];
        this._content = utf8.decode(bytes);
    }
}

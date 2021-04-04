function zyn_sort_filesystem_elements(elements, name_filter) {
    let dirs = [];
    let files = [];

    function _sort_by_name(a, b) {
        let name_a = a.name.toLowerCase()
        let name_b = b.name.toLowerCase()
        if (name_a < name_b) {return -1;}
        if (name_a > name_b) {return 1;}
        return 0;
    }

    for (let e of elements) {
        if (name_filter !== null && e.name.indexOf(name_filter) === -1) {
            continue ;
        }
        if (e.is_file()) {
            files.push(e);
        } else if (e.is_directory()) {
            dirs.push(e);
        } else {
            zyn_unhandled();
        }
    }

    files.sort(_sort_by_name);
    dirs.sort(_sort_by_name);
    return files.concat(dirs);
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

class DisconnectNotification extends Notification {
    constructor(description) {
        super();
        this.description = description;
    }

    to_string() { return `Server disconeccted: ${this.description}`; }
    is_disconnect() { return true; }
}

class EditNotification extends Notification {
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

class FilesystemElement {
    constructor() {
    }

    is_file() { throw 'not implemented'; }
    is_directory() { throw 'not implemented'; }
}

class FilesystemElementFile extends FilesystemElement {
    constructor(name, node_id, revision, file_type, size, is_open) {
        super();
        this.name = name;
        this.node_id = node_id;
        this.revision = revision;
        this.file_type = file_type;
        this.size = size;
        this.is_open = is_open;
    }

    is_file() { return true; }
    is_directory() { return false; }
}

class FilesystemElementDirectory extends FilesystemElement {
    constructor(name, node_id, authtority_read, authority_write) {
        super();
        this.name = name;
        this.node_id = node_id;
        this.authtority_read = authtority_read;
        this.authority_write = authority_write;
    }

    is_file() { return false; }
    is_directory() { return true; }
}

class ListChildrenRsp extends MessageRsp {
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

class CreateFileRsp extends MessageRsp {
   constructor(namespace, transaction_id, error_code, node_id, revision) {
        super(namespace, transaction_id, error_code);
        this.node_id = node_id;
        this.revision = revision;
   }
}

class CreateDirectoryRsp extends MessageRsp {
   constructor(namespace, transaction_id, error_code, node_id) {
        super(namespace, transaction_id, error_code);
        this.node_id = node_id;
   }
}

class ReadRsp extends MessageRsp {
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

class EditRsp extends MessageRsp {
    constructor(namespace, transaction_id, error_code, revision) {
        super(namespace, transaction_id, error_code);
        this.revision = revision;
    }
}

class BatchEditRsp extends MessageRsp {
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

class ExpectRsp {
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

class Authority {
    constructor(type_of, name) {
        this._type_of = type_of;
        this.name = name;
    }
}

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
        this._client.connection().open_file(this._node_id, mode, (rsp) => {

            if (!rsp.is_error()) {
                this._revision =  rsp.revision;
                this._size =  rsp.size;
                this._block_size =  rsp.page_size;
                this._file_type = rsp.file_type;
                this._mode = mode;
            }
            callback();
        });
    }

    close(callback) {
        this._client.connection().close_file(this._node_id, callback);
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

    read(offset, size, callback) {
        this._client.connection().read_file(this._node_id, offset, size, callback);
    }

    initial_load(callback) {
        this.read_full_file(callback);
    }

    read_full_file(callback) {
        class FullFileReader {
            constructor(file, callback) {
                this._file = file;
                this._callback = callback;
                this._buffer = new ZynByteBuffer(this._file._size);

                if (this._file._size > 0) {
                    this._read_block();
                } else {
                    callback(this._buffer);
                }
            }

            set_loading_modal() {
                if (zyn_is_modal_loading_vissible()) {
                    let number_of_blocks = Math.floor(this._file._size / this._file._block_size) + 1;
                    let current_block = 1;

                    if (this._buffer.current_size() > 0) {
                        current_block = (this._buffer.current_size() / this._file._block_size) + 1;
                    }
                    zyn_show_modal_loading(`Loading file ${current_block} / ${number_of_blocks}`);
                }
            }

            _read_block() {
                this.set_loading_modal();
                this._file.read(this._buffer.current_size(), this._file._block_size, (msg) => { this.handle_rsp(msg); });
            }

            handle_rsp(msg) {
                this._buffer.add(msg.data());

                if (this._buffer.is_complete()) {
                    this._callback(this._buffer);
                } else {
                    this._read_block();
                }
            }
        }
        new FullFileReader(this, callback);
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

class ZynPdfHandler extends ZynFileHandler {
    constructor(node_id, filename, client) {
        super(node_id, filename, client);
        this._content = null;
    }

    static is_editable() { return false; }
    is_editable() { return false; }

    initial_load(callback) {
        super.initial_load((content) => {
            this._content = content.data();
            callback();
        });
    }

    render(mode, target_id, callback=null) {
        let element = document.getElementById(target_id);
        if (mode !== OpenMode.read) {
            zyn_unhandled();
            return ;
        }

        var root = document.createElement('div');
        pdfjsLib.GlobalWorkerOptions.workerSrc = PATH_PDFJS_WORKER;

        var loadingTask = pdfjsLib.getDocument({data: this._content});
        loadingTask.promise.then(function(pdf) {
            var pageNumber = 1;
            for (var pageNumber = 1; pageNumber <= pdf.numPages; pageNumber++) {
                pdf.getPage(pageNumber).then(function(page) {
                    var scale = 1.5;
                    var viewport = page.getViewport({scale: scale});
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
                    renderTask.promise.then(function () {});
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

class ZynMarkdownHandler extends ZynFileHandler {
    constructor(node_id, filename, client) {
        super(node_id, filename, client);
        this._is_edited = false;
        this._content =  null;
    }

    static is_editable() { return true; }
    is_editable() { return true; }
    has_edits() { return this._is_edited; }
    content_edited() { this._is_edited = true; }

    update_content(content, revision) {
        this._base._revision = revision;
        this._base._size = content.length;
        this._is_edited = false;
        this._content = content;
    }

    initial_load(callback) {
        super.initial_load((content) => {
            let decoder = new TextDecoder();
            this._content = utf8.decode(decoder.decode(content.data()));
            callback();
        });
    }

    render(mode, target_id, callback=null) {
        let element = document.getElementById(target_id);
        if (mode === OpenMode.read) {
            let content = this._content;
            if (content.length === 0) {
                this.render_empty_file(target_id);
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
            element.style.margin = "0px 5px 0px 5px";
            element.style.paddingLeft = "1px";
            element.innerText = content;
            element.addEventListener("paste", function(e) {
                e.preventDefault();
                var text = e.clipboardData.getData("text/plain");
                document.execCommand("insertHTML", false, text);
            });
        } else {
            zyn_unhandled();
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
        let encoder = new TextEncoder();

        for (let d of diff) {
            let bytes = encoder.encode(utf8.encode(d.value));
            if (d.added) {
                modifications.push({
                    'type': 'add',
                    'offset': offset,
                    'bytes': bytes,
                });
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

        this._client.connection().apply_modifications(this._node_id, this._revision, modifications, (rsp) => {
            if (!rsp.is_error()) {
                this._revision = rsp.revision;
                this._content = modified_content;
                this._is_edited = false;
            }
            callback(rsp);
        });
    }

    handle_notification(notification, mode, target_id) {

        let decoder = new TextDecoder();
        let encoder = new TextEncoder();

        let _show_error = (error_code) => {
            zyn_show_modal_error(ErrorLevel.error, "There was an applying remote updates, please refresh...", `Error code: ${error_code}`);
        };

        if (notification.type_of_edit == 'insert') {

            this.read(notification.offset, notification.size, (rsp) => {
                if (rsp.is_error()) {
                    _show_error(rsp.error_code);
                    return ;
                }

                let bytes = encoder.encode(utf8.encode(this._content));
                let buffer = new ZynByteBuffer(bytes.length + notification.size);
                buffer.add(bytes.slice(0, notification.offset));
                buffer.add(rsp.data());
                buffer.add(bytes.slice(notification.offset, bytes.length));
                this._content = utf8.decode(decoder.decode(buffer.data()));
                this.render(mode, target_id);
            });

        } else if (notification.type_of_edit == 'modify') {

            this.read(notification.offset, notification.size, (rsp) => {
                if (rsp.is_error()) {
                    _show_error(rsp.error_code);
                    return ;
                }
                let bytes = encoder.encode(utf8.encode(this._content));
                let buffer = new ZynByteBuffer(bytes.length + notification.size);
                buffer.add(bytes.slice(0, notification.offset));
                buffer.add(rsp.data());
                buffer.add(bytes.slice(notification.offset + notification.size, bytes.length));
                this._content = utf8.decode(decoder.decode(buffer.data()));
                this.render(mode, target_id);
            });

        } else if (notification.type_of_edit == 'delete') {

            let bytes = encoder.encode(utf8.encode(this._content));
            let buffer = new ZynByteBuffer(bytes.length - notification.size);
            buffer.add(bytes.slice(0, notification.offset));
            buffer.add(bytes.slice(notification.offset + notification.size, bytes.length));
            this._content = utf8.decode(decoder.decode(buffer.data()));
            this.render(mode, target_id);

        } else {
            zyn_unhandled();
        }
    }
}

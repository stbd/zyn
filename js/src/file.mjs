import {
  OpenMode,
  log,
} from './common.mjs';


class ReadState {
  constructor(start_offset, size, file, callback) {
    this.start_offset = start_offset;
    this.size = size;
    this.file = file;
    this.bytes_read = 0;
    this.callback = callback;
    this.responses = []
  }

  next_block() {
    const start = this.start_offset + this.bytes_read;
    const bytes_left = this.size - this.bytes_read;
    const end_offset = start + bytes_left - 1;

    const start_page = Math.floor(start / this.file._page_size);
    const end_page = Math.floor(end_offset / this.file._page_size);
    let size = end_offset - start + 1;
    if (start_page != end_page) {
      size = this.file._page_size - start;
    }

    return {
      "start": start,
      "size": size,
    };
  }

  is_error() {
    for (let rsp of this.responses) {
      if (rsp.is_error()) {
        return true;
      }
      return false;
    }
  }

  add_response(rsp) {
    this.responses.push(rsp);
    if (!rsp.is_error()) {
      this.bytes_read += rsp.size();
    }
  }

  is_complete() {
    return this.bytes_read == this.size;
  }

  complete() {
    let i = 0;
    let bytes = new Uint8Array(this.size);
    let revision = null;
    for (let rsp of this.responses) {
      bytes.set(rsp.data(), i);
      i += rsp.size();
      revision = rsp.revision;
    }
    this.callback(bytes, revision);
  }
}


class Base {
  static filename_extension = null;
  static is_editable = false;

  constructor(open_rsp, client, filename) {
    this._client = client
    this._node_id = open_rsp.node_id;
    this._revision = open_rsp.revision;
    this._page_size = open_rsp.page_size;
    this._path_parent = client._path_dir;
    this._filename = filename;
    this._mode = OpenMode.read;
  }

  filename() { return this._filename; }
  node_id() { return this._node_id; }
  revision() { return this._revision; }
  open_mode() { return this._mode; }
  has_changes() { return false; }
  render() { throw 'Not implemented'; }
  save() { throw 'Not implemented'; }
  handle_notification(notification) { throw 'Not implemented'; }

  show_internal_error(description) {
    this._client.ui().unhandled_sittuation_modal(`Internal error: ${description}`);
    throw 'Internal error';
  }

  path_to_file() {
    if (this._path_parent === '/') {
      return `${this._filename}`;
    }
    let path = `${this._path_parent}/${this._filename}`;
    if (path.startsWith(path)) {
      path = path.slice(1);
    }
    return path;
  }

  render_empty() {
    this._client.ui().set_file_content_text('Empty File');
  }

  _read_callback(rsp, state) {
    state.add_response(rsp);
    if (rsp.is_error()) {
      this.show_internal_error(`Received error code ${rsp.error_code}`);
      return ;
    }

    if (state.is_complete()) {
      state.complete();
    } else {
      const block = state.next_block();
      this._client.connection().read_file(
        this._node_id,
        block.start,
        block.size,
        (rsp) => this._read_callback(rsp, state)
      );
    }
  }

  read_file_content(offset, size, callback) {
    const state = new ReadState(offset, size, this, callback);
    const block = state.next_block();
    this._client.connection().read_file(
      this._node_id,
      block.start,
      block.size,
      (rsp) => this._read_callback(rsp, state)
    );
  }
}

export { Base, ReadState };

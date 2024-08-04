const {
  OpenMode,
} = require('./common');
const diff = require('diff');
const showdown = require('showdown');

class Base {
  constructor(open_rsp, client, element) {
    this._client = client
    this._ui = client._ui;
    this._connection = client._connection;
    this._node_id = open_rsp.node_id;
    this._revision = open_rsp.revision;
    this._path_parent = client._path_dir;
    this._filename = element.name;
  }

  render() { throw 'Not implemented'; }
  switch_to_edit_mode() { throw 'Not implemented'; }
  switch_to_view_mode() { throw 'Not implemented'; }
  save() { throw 'Not implemented'; }
  open_mode() { return null; }
  has_changes() { return false; }

  path_to_file() {
    if (this._path_parent === '/') {
      return `${this._filename}`;
    }
    return `${this._path_parent}/${this._filename}`;
  }

  render_empty() {
    this._ui.set_file_content_text('Empty File');
  }

  read_file_content(offset, size, page_size, callback) {
    if (size < page_size) {
      this._connection.read_file(
        this._node_id,
        offset,
        size,
        callback
      );
    } else {
      this._connection.read_file(
        node_id,
        offset,
        size,
        (rsp) => {
          throw 'not implemented'
        }
        );
    }
  }
}

class MarkdownFile extends Base {
  constructor(open_rsp, client, element, mode) {
    super(open_rsp, client, element);
    this._content = null;
    this._mode = null;
    this._converter = new showdown.Converter();
    this._set_mode(mode);

    if (open_rsp.size === 0) {
      this._content = '';
      this.render()
    } else {
      this.read_file_content(
        0,
        open_rsp.size,
        open_rsp.page_size,
        (rsp) => {
          if (rsp.is_error()) {
            throw 'not implemetneed'
          }
          if (!rsp.is_complete()) {
            throw 'not implemetneed'
          }
          this._revision = rsp.revision
          this._content = this._connection.decode_from_bytes(rsp.data());
          this.render();
        }
      )
    }
  }

  open_mode() { return this._mode; }

  _set_mode(mode) {
    this._mode = mode;
    if (this._mode === OpenMode.read) {
      this._ui.file_area_button_done(false);
      this._ui.file_area_button_edit(true);
      this._ui.file_area_button_save(false);
      this._ui.file_area_button_cancel(false);
    } else if (this._mode === OpenMode.edit) {
      this._ui.file_area_button_done(true);
      this._ui.file_area_button_edit(false);
      this._ui.file_area_button_save(true);
      this._ui.file_area_button_cancel(true);
    }
  }

  set_mode(mode) {
    if (mode === this._mode) {
      return ;
    }

    if (mode === OpenMode.edit) {
      // File needs to opened in edit mode
      this._ui.show_loading_modal();
      this._connection.close_file(this._node_id, (rsp) => {
        if (rsp.is_error()) {
          throw 'not implemetneed';
        }
        this._connection.open_file(this._node_id, mode, (rsp) => {
          if (rsp.is_error()) {
            throw 'not implemetneed';
          }
          this._set_mode(mode);
          this.render();
          this._ui.hide_modals();
          this._client.update_browser_url();
        });
      });
    } else if (mode === OpenMode.read) {
      // We can just rerender
      this._set_mode(mode);
      this.render();
    }
  }

  save() {
    let edited_content = this._ui.get_file_textarea_content()
    let modifications = []
    let offset = 0;

    for (let mod of diff.diffChars(this._content, edited_content)) {
      console.log(mod)
      if (mod.added === true) {
        modifications.push({
          'type': 'add',
          'offset': offset,
          'bytes': this._connection.encode_to_bytes(mod.value),
        })
        offset += mod.count;
      } else if (mod.removed === true) {
        modifications.push({
          'type': 'delete',
          'offset': offset,
          'size': mod.count,
        })
      } else {
        offset += mod.count;
      }
    }

    this._ui.show_loading_modal();
    this._connection.apply_modifications(
      this._node_id,
      this._revision,
      modifications,
      (rsp) => this.handle_edit_completed(rsp, edited_content),
    )
  }

  handle_edit_completed(rsp, new_content) {
    this._ui.hide_modals();
    if (rsp.is_error()) {
      this._ui.unhandled_sittuation_modal(`server replied with error code ${rsp.error_code}`)
      return
    }
    this._revision = rsp.revision;
    this._content = new_content;
  }

  render() {
    console.log(`Rendering Markdown in mode "${this._mode}"`)
    if (this._mode == OpenMode.read) {
      if (this._content.length == 0) {
        this.render_empty();
      } else {
        this._ui.set_file_content(
          this._converter.makeHtml(this._content)
        );
      }
    } else if (this._mode == OpenMode.edit) {
      this._ui.set_file_content_textarea(this._content);
    }
  }
}

exports.MarkdownFile = MarkdownFile;

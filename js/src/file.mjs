import {
  OpenMode,
  log,
  encode_to_bytes,
  decode_from_bytes,
} from './common.mjs';
import { diffArrays } from 'diff';
import showdown from 'showdown';
import {getDocument, GlobalWorkerOptions} from 'pdfjs-dist';


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

class MarkdownFile extends Base {
  static filename_extension = '.md';
  static is_editable = true;

  constructor(open_rsp, client, filename, mode) {
    super(open_rsp, client, filename);
    this._content = null;
    this._mode = null;
    this._mode_server = null;
    this._converter = new showdown.Converter({'simplifiedAutoLink': true});
    this._set_mode(mode);
    this._edited = false;

    this._client.ui().show_loading_modal('Loading file content...')

    if (open_rsp.size === 0) {
      this._content = new Uint8Array([]);
      this.render();
    } else {
      this.read_file_content(
        0,
        open_rsp.size,
        (data, revision) => {
          this._revision = revision;
          this._content = data;
          this.render();
        }
      )
    }
  }

  has_changes() { return this._edited; }
  open_mode() { return this._mode; }
  file_edited() { this._edited = true; }
  revert_edits() {
    this.render();
    this._edited = false;
  }

  _set_mode(mode) {
    this._mode = mode;
    if (this._mode === OpenMode.read) {
      this._client.ui().file_area_button_done(false);
      this._client.ui().file_area_button_edit(true);
      this._client.ui().file_area_button_save(false);
      this._client.ui().file_area_button_cancel(false);
    } else if (this._mode === OpenMode.edit) {
      this._client.ui().file_area_button_done(true);
      this._client.ui().file_area_button_edit(false);
      this._client.ui().file_area_button_save(true);
      this._client.ui().file_area_button_cancel(true);
      this._mode_server = OpenMode.edit;
    }
    if (this._mode_server === null) {
      this._mode_server = mode;
    }
  }

  set_mode(mode) {
    if (mode === this._mode) {
      return ;
    }

    if (mode === OpenMode.edit && this._mode_server !== OpenMode.edit) {
      // File needs to opened in edit mode
      this._client.ui().show_loading_modal();
      this._client.connection().close_file(this._node_id, (rsp) => {
        if (rsp.is_error()) {
          throw 'not implemetneed';
        }
        this._client.connection().open_file(this._node_id, mode, (rsp) => {
          if (rsp.is_error()) {
            throw 'not implemetneed';
          }
          this._set_mode(mode);
          this.render();
          this._client.ui().hide_modals();
          this._client.update_browser_url();
        });
      });
    } else {
      // We can just rerender
      this._set_mode(mode);
      this.render();
      this._client.update_browser_url();
    }
  }

  handle_notification(notification) {
    if (notification.node_id !== this._node_id) {
      console.log(`Received notification for unknown file ${notification.node_id}`)
      return ;
    }
    if (this._mode !== OpenMode.edit && this.has_changes()) {

      this._client.ui().unhandled_sittuation_modal(`File has been modified on remote, please copy changes and refresh manually`);

    } else {

      if (notification.type_of_edit == 'insert') {

        this.read_file_content(
          notification.offset,
          notification.size,
          (data, revision) => {

            let result = new Uint8Array(this._content.length + notification.size);
            result.set(this._content.subarray(0, notification.offset));
            result.set(data, notification.offset);

            const remaining = this._content.length - notification.offset;
            result.set(
              this._content.subarray(
                notification.offset,
                notification.offset + remaining
              ),
              notification.offset + notification.size,
            );
            this._content = result
            this._revision = revision;
            this.render();
          });

      } else if (notification.type_of_edit == 'modify') {

        this.read_file_content(
          notification.offset,
          notification.size,
          (data, revision) => {
            let result = new Uint8Array(this._content.length);
            result.set(this._content.subarray(0, notification.offset));
            result.set(data, notification.offset);
            const remaining = this._content.length - notification.offset - notification.size;

            result.set(
              this._content.subarray(
                notification.offset + notification.size,
                notification.offset + notification.size + remaining
              ),
              notification.offset + notification.size,
            );
            this._content = result
            this._revision = revision;
            this.render();
          });


      } else if (notification.type_of_edit == 'delete') {
        let result = new Uint8Array(this._content.length - notification.size);
        result.set(this._content.subarray(0, notification.offset));
        result.set(
          this._content.subarray(notification.offset + notification.size, this._content.length),
          notification.offset,
        );
        this._content = result
        this._revision = notification.revision;
        this.render();
      }
    }
  }

  save() {
    const scroll = this._client.ui().get_file_text_area_scroll();
    let edited_content = this._client.ui().get_file_textarea_content();
    let modifications = []
    let offset = 0;

    for (let mod of diffArrays(
      this._content,
      encode_to_bytes(edited_content),
    )) {
      if (mod.added === true) {
        modifications.push({
          'type': 'add',
          'offset': offset,
          'bytes': mod.value,
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

    if (modifications.length == 0) {
      return ;
    }

    this._client.ui().show_loading_modal();
    this._client.connection().apply_modifications(
      this._node_id,
      this._revision,
      modifications,
      (rsp) => this.handle_edit_completed(rsp, encode_to_bytes(edited_content), scroll),
      (operation_number, total_operations) => {
        this._client.ui().show_loading_modal(`Applying modification ${operation_number} / ${total_operations}`);
      },
    )
  }

  handle_edit_completed(rsp, new_content, scroll) {
    this._client.ui().hide_modals();
    if (rsp.is_error()) {
      this._client.ui().unhandled_sittuation_modal(`server replied with error code ${rsp.error_code}`)
      return
    }
    this._revision = rsp.revision;
    this._content = new_content;
    this._edited = false;
    this._client.ui().show_save_button_indication();
    this.render();
    this._client.ui().set_file_text_area_scroll(scroll);
  }

  render() {
    console.log(`Rendering Markdown in mode "${this._mode}"`)
    if (this._mode == OpenMode.read) {
      if (this._content.length == 0) {
        this.render_empty();
      } else {
        // Prose descriptions are from here: https://github.com/tailwindlabs/tailwindcss-typography
        this._client.ui().set_file_content(
          `
<div class="prose prose-headings:leading-none prose-ul:leading-none prose-li:leading-none">
${this._converter.makeHtml(decode_from_bytes(this._content))}
</div>
          `
        );
      }
    } else if (this._mode == OpenMode.edit) {
      this._client.ui().set_file_content_textarea(
        decode_from_bytes(this._content),
        () => this.file_edited(),
      );
    }
    this._client.ui().hide_modals();
  }
}


class PdfFile extends Base {
  static filename_extension = '.pdf';
  static is_editable = false;

  constructor(open_rsp, client, filename, mode) {
    super(open_rsp, client, filename);
    this._content = null;

    this._client.ui().show_loading_modal('Loading file content...')

    if (open_rsp.size === 0) {
      this._content = '';
      console.log('empty')
      this.render()
    } else {
      this.read_file_content(
        0,
        open_rsp.size,
        (data, revision) => {
          this._revision = revision
          this._content = decode_from_bytes(data);
          console.log(`content ${this._content.length}` )
          this.render();
        }
      );
    }
  }

  render() {
    console.log(`Rendering PDF`)
    let canvas = this._client.ui().create_file_content_canvas();
    let context = canvas.getContext('2d');
    var scale = 1.5;

    GlobalWorkerOptions.workerSrc = `${this._client.ui().get_browser_url().origin}/static/pdf.worker.mjs`

    getDocument({data: this._content}).promise.then(function(pdf) {
      console.log('PDF loaded');

      let pageNumber = 1;
      pdf.getPage(pageNumber).then(function(page) {
        var viewport = page.getViewport({scale: scale});
        var render_context = {
          canvasContext: context,
          viewport: viewport
        };
        page.render(render_context).promise.then(function () {
          console.log('Page rendered');
        });
      });
    }, function (reason) {
      console.error(reason);
    });
    this._client.ui().hide_modals();
  }
}

export { Base, MarkdownFile, PdfFile, ReadState };

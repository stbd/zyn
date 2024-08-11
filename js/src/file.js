const {
  OpenMode,
} = require('./common');
const diff = require('diff');
const showdown = require('showdown');
const pdfjs = require('pdfjs-dist');
const pdfjs_worker = require('pdfjs-dist/build/pdf.worker.mjs');


class Base {
  static filename_extension = null;
  static is_editable = false;

  constructor(open_rsp, client, filename) {
    this._client = client
    this._node_id = open_rsp.node_id;
    this._revision = open_rsp.revision;
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

  path_to_file() {
    if (this._path_parent === '/') {
      return `${this._filename}`;
    }
    return `${this._path_parent}/${this._filename}`;
  }

  render_empty() {
    this._client.ui().set_file_content_text('Empty File');
  }

  read_file_content(offset, size, page_size, callback) {
    if (size < page_size) {
      this._client.connection().read_file(
        this._node_id,
        offset,
        size,
        callback
      );
    } else {
      this._client.connection().read_file(
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
  static filename_extension = '.md';
  static is_editable = true;

  constructor(open_rsp, client, filename, mode) {
    super(open_rsp, client, filename);
    this._content = null;
    this._mode = null;
    this._mode_server = null;
    this._converter = new showdown.Converter();
    this._set_mode(mode);

    this._client.ui().show_loading_modal('Loading file content...')

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
          this._revision = rsp.revision
          this._content = this._client.connection().decode_from_bytes(rsp.data());
          this.render();
        }
      )
    }
  }

  open_mode() { return this._mode; }

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

  save() {
    let edited_content = this._client.ui().get_file_textarea_content()
    let modifications = []
    let offset = 0;

    for (let mod of diff.diffChars(this._content, edited_content)) {
      console.log(mod)
      if (mod.added === true) {
        modifications.push({
          'type': 'add',
          'offset': offset,
          'bytes': this._client.connection().encode_to_bytes(mod.value),
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

    this._client.ui().show_loading_modal();
    this._client.connection().apply_modifications(
      this._node_id,
      this._revision,
      modifications,
      (rsp) => this.handle_edit_completed(rsp, edited_content),
    )
  }

  handle_edit_completed(rsp, new_content) {
    this._client.ui().hide_modals();
    if (rsp.is_error()) {
      this._client.ui().unhandled_sittuation_modal(`server replied with error code ${rsp.error_code}`)
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
        this._client.ui().set_file_content(
          `
<div class="prose">
${this._converter.makeHtml(this._content)}
</div>
          `
        );
      }
    } else if (this._mode == OpenMode.edit) {
      this._client.ui().set_file_content_textarea(this._content);
    }
    this._client.ui().hide_modals();
  }
}

exports.MarkdownFile = MarkdownFile;

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
        open_rsp.page_size,
        (rsp) => {
          if (rsp.is_error()) {
            throw 'not implemetneed'
          }
          this._revision = rsp.revision
          this._content = this._client.connection().decode_from_bytes(rsp.data());
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

    pdfjs.GlobalWorkerOptions.workerSrc = 'static/pdf-worker.mjs'

    pdfjs.getDocument({data: this._content}).promise.then(function(pdf) {
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

exports.PdfFile = PdfFile;

import { Base } from './file.mjs';
import { diffArrays } from 'diff';
import showdown from 'showdown';
import {
  OpenMode,
  encode_to_bytes,
  decode_from_bytes,
} from './common.mjs';

class MarkdownFile extends Base {
  static filename_extension = '.md';
  static is_editable = true;
  static default_open_mode = OpenMode.read;

  constructor(open_rsp, client, filename, mode) {
    super(open_rsp, client, filename);
    this._content = null;
    this._mode = mode;
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

export { MarkdownFile };

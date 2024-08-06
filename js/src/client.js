const connection = require('./connection');
const {
  MarkdownFile,
  PdfFile,
} = require('./file');
const {
  OpenMode,
} = require('./common');

FilesystemElementTypes = Object.freeze({
  'dir': 'Directory',
  'markdown': 'Markdown',
})

const HEALTHCHECK_TIMER_DURATION = 30000;

class Client {
  constructor(
    authentication_token,
    server_address,
    user_id,
    root_url,
    path_parent,
    filename,
    file_mode,
    controller
  ) {

    if (filename === "") {
      filename = null;
    }


    console.log(`Initializing client with parent "${path_parent}", file "${filename}", root url "${root_url}" to server at "${server_address}"`)

    this._path_dir = path_parent;
    this._root_url = root_url;
    this._ui = controller;
    this._file = null;

    this._ui.show_loading_modal('Initializing...')
    this._ui.register_client_callbacks(this);
    this._ui.set_file_content_text('File');

    if (filename && !file_mode) {
      file_mode = OpenMode.read;
    }

    this._connection = new connection(
      server_address,
      (event) => {
        this._connection.authenticate_with_token(authentication_token, (rsp) => this.handle_connection_completed(rsp, filename, file_mode));
      },
      (event) => this.on_socket_error(event),
      (event) => this.on_socket_close(event),
    );

    setTimeout(() => this.healtcheck_callback(), HEALTHCHECK_TIMER_DURATION);
    this.update_browser_url();
    this._ui.show_full_sidebar();
  }

  path(children=null) {
    if (children === null) {
      return this._path_dir;
    } else {
      let path = null;
      if (this._path_dir === '/') {
        path = '';
      } else {
        path = this._path_dir;
      }
      for (let p of children) {
        path += '/' + p;
      }
      return path;
    }
  }

  path_parent() {
    if (this._path_dir === '/') {
      return this._path_dir;
    }
    let parts = this._path_dir.split('/');
    parts.pop();
    let path = '/';
    for (let p of parts) {
      if (!p) {
        continue;
      }
      path += '/' + p;
    }
    return path;
  }

  update_browser_url() {
    if (this._file === null) {
      this._ui.set_browser_url(this._root_url)
    } else {
      let url_postfix = `${this._root_url}/${this._file.path_to_file()}`;
      let params = {}
      let mode = this._file.open_mode();
      if (mode !== null) {
        params['mode'] = mode
      }
      this._ui.set_browser_url(url_postfix, params);
    }
  }

  map_filename_to_handler(filename) {
    if (filename.endsWith(MarkdownFile.filename_extension)) {
      return MarkdownFile;
    } else if (filename.endsWith(PdfFile.filename_extension)) {
      return PdfFile;
    } else {
      return null;
    }
  }

  healtcheck_callback() {
    if (!this._connection.is_ok()) {
      console.log('reconnecting')
    }
    setTimeout(() => this.healtcheck_callback(), HEALTHCHECK_TIMER_DURATION);
  }

  on_socket_error(e) {
    console.log(`Error on socket: ${e}`);
  }

  on_socket_close(e) {
    console.log(`Socket closed: ${e}`);
  }

  change_directory(path) {
    console.log(`Changing path to ${path}`)
    this._connection.query_element_children(
      path,
      (rsp) => this.handle_query_children_rsp(rsp, path),
    );
  }

  handle_create_markdown_clicked() {
    this._ui.show_create_element_modal(
      "Filename should end with .md",
      "filename.md",
      (name) => this.handle_create_clicked("markdown", "create", name),
      (name) => this.handle_create_clicked("markdown", "cancel", name),
    );
  }

  handle_file_done_clicked() {
    if (this._file !== null) {
      this._file.set_mode(OpenMode.read);
    }
  }

  handle_file_edit_clicked() {
    if (this._file !== null) {
      this._file.set_mode(OpenMode.edit);
    }
  }

  handle_file_save_clicked() {
    if (this._file !== null) {
      this._file.save();
    }
  }

  handle_file_cancel_clicked() {
    if (this._file !== null) {
      this._file.cancel_edits();
    }
  }

  handle_file_file_info_clicked() {
    if (this._file !== null) {

    }
  }

  handle_create_clicked(type_of_element, user_action, name) {
    console.log(`handle_create_clicked ${type_of_element}  ${user_action}`);
    if (user_action == 'cancel') {
      this._ui.reset_and_hide_create_element_modal();
      return
    }

    if (type_of_element == 'markdown') {
      if (this.map_filename_to_handler(name) !== MarkdownFile) {
        this._ui.activate_modal_notification(
          'Note',
          `Markdown filename should have extension ${MarkdownFile.filename_extension}
          <br\>
          <br\>
          Plaese try again
          `,
          'Ok',
          () => {
            this._ui.hide_modals();
          }
        );
        return ;
      }

      console.log(`Creating New Markdown file ${name}`);
      this._connection.create_file_ra(name, this._path_dir, (rsp) => this.handle_create_response('markdown', rsp));
      this._ui.show_loading_modal();
    } else {
      throw 'Invalid '
    }
  }

  handle_create_response(type_of_element, rsp) {
    if (rsp.is_error()) {
      this._ui.unhandled_sittuation_modal(`server replied with error code ${rsp.error_code}`)
      return ;
    }
    this._ui.reset_and_hide_create_element_modal();
    this._connection.query_element_children(
      this._path_dir,
      (rsp) => this.handle_query_children_rsp(rsp, this._path_dir),
    );
  }

  handle_connection_completed(rsp, filename, file_mode) {
    if (rsp.is_error()) {
      this._ui.unhandled_sittuation_modal(`server replied with error code ${rsp.error_code}`)
      return ;
    }
    this._connection.query_element_children(
      this._path_dir,
      (rsp) => {
        const children = this.handle_query_children_rsp(rsp, this._path_dir);
        if (filename === null) {
          return ;
        }
        for (const c of children) {
          if (c.name === filename) {
            this.handle_file_clicked(c, file_mode);
            return ;
          }
        }
        this._ui.nothing_to_do_sittuation_modal(`file ${filename} was not found under ${this._path_dir}`)
      },
    );
  }

  handle_query_children_rsp(rsp, new_path_dir) {
    if (rsp.is_error()) {
      this._ui.unhandled_sittuation_modal(`server replied with error code ${rsp.error_code}`)
      return
    }

    this._path_dir = new_path_dir
    this._ui.set_working_dir_path(this._path_dir);
    this._ui.render_filesystem_elements(
      rsp.elements,
      this.map_filename_to_handler,
      (element, mode) => this.handle_file_clicked(element, mode),
      (element) => this.handle_directory_clicked(element),
      (element) => this.handle_fs_element_delete_clicked(element),
    );
    this._ui.hide_modals();
    return rsp.elements;
  }

  handle_file_clicked(element, mode) {
    this._ui.show_loading_modal();
    console.log(`Opening element "${element.name}" with node id ${element.node_id}`)
    this._connection.open_file(element.node_id, mode, (rsp) => this.handle_open_file_rsp(rsp, mode, element));
  }

  handle_directory_clicked(element) {
    const path = this.path([element.name])
    this.change_directory(path);
  }

  handle_change_dir_to_parent_clicked() {
    const new_path_dir = this.path_parent();
    this.change_directory(new_path_dir);
  }

  handle_fs_element_delete_clicked(element) {
    console.log(element)
  }

  handle_open_file_rsp(rsp, mode, element) {
    // todo: handle case when user does not have edit permissions
    if (rsp.is_error()) {
      this._ui.unhandled_sittuation_modal(`server replied with error code ${rsp.error_code}`)
      return
    }

    this._ui.hide_modals();
    const object = this.map_filename_to_handler(element.name);
    if (object === null) {
      this._ui.unhandled_sittuation_modal(`no handler found for elment with name ${element.name}`);
      return ;
    }
    this._file = new object(rsp, this, element, mode);
    this.update_browser_url();
  }
}

module.exports = Client;

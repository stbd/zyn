import { Connection } from './connection.mjs'
import { MarkdownFile } from './file_markdown.mjs';
import { PdfFile } from './file_pdf.mjs';
import { ListFile } from './file_list.mjs';
import {
  OpenMode,
  log,
} from './common.mjs';

const FilesystemElementTypes = Object.freeze({
  'dir': 'Directory',
  'markdown': 'Markdown',
})

const HEALTHCHECK_TIMER_DURATION = 3000;

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

    log(`Initializing client with parent "${path_parent}", file "${filename}", root url "${root_url}" to server at "${server_address}"`)

    this._path_dir = path_parent;
    this._root_url = root_url;
    this._ui = controller;
    this._server_address = server_address;
    this._file = null;

    this._ui.show_loading_modal('Loading...')
    this._ui.register_client_callbacks(this);
    this._ui.set_file_content_text('File');

    if (filename && !file_mode) {
      file_mode = OpenMode.read;
    }

    this.create_connection_and_auhenticate(
      authentication_token,
      (rsp) => this.handle_connection_completed(rsp, filename, file_mode),
    );

    this.update_browser_url();
    this._ui.show_full_sidebar();
  }

  connection() {
    return this._connection;
  }

  ui() {
    return this._ui;
  }

  create_connection_and_auhenticate(token, callback) {
    this._connection = new Connection(
      this._server_address,
      (_event) => {
        setTimeout(() => this.healtcheck_callback(), HEALTHCHECK_TIMER_DURATION);
        this._connection.authenticate_with_token(token, (rsp) => callback(rsp));
      },
      (event) => this.on_socket_error(event),
      (event) => this.on_socket_close(event),
      (msg) => this.handle_notification(msg),
    );
  }

  handle_notification(msg) {
    console.log('Notification')
    console.log(msg)
    if (msg.is_edit()) {
      if (this._file !== null) {
        this._file.handle_notification(msg);
      } else {
        console.log(`Received edit notification when no file is active, node_id ${msg.node_id}`);
      }
    }
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
    let path = '';
    for (let p of parts) {
      if (!p) {
        continue;
      }
      path += '/' + p;
    }
    if (path === '') {
      path = '/';
    }
    return path;
  }

  update_browser_url() {
    if (this._file === null) {
      this._ui.set_browser_url(this._root_url)
    } else {
      let url_postfix = `${this._root_url}/${this._file.path_to_file()}`;
      let params = {}
      if (this._file.constructor.is_editable) {
        let mode = this._file.open_mode();
        if (mode !== null) {
          params['mode'] = mode
        }
      }

      this._ui.set_browser_url(url_postfix, params);
    }
  }

  map_filename_to_handler(filename) {
    if (filename.endsWith(MarkdownFile.filename_extension)) {
      return MarkdownFile;
    } else if (filename.endsWith(ListFile.filename_extension)) {
      return ListFile;
    } else if (filename.endsWith(PdfFile.filename_extension)) {
      return PdfFile;
    } else {
      return null;
    }
  }

  healtcheck_callback() {
    if (!this._connection.is_ok()) {
      this._ui.activate_modal_notification(
        'Connection closed',
        'Connection to server was closed, click reconnect to reconnect',
        'Reconnect',
        () => this.handle_reconnect(),
      );
    } else {
      setTimeout(() => this.healtcheck_callback(), HEALTHCHECK_TIMER_DURATION);
    }
  }

  handle_reconnect() {
    this._ui.show_loading_modal('Reconnecting...')
    let browser_url = this._ui.get_browser_url();
    let reconnect_url = new URL(`${browser_url.protocol}//${browser_url.host}/relogin`);
    let http = new XMLHttpRequest();
    let client = this;

    http.onreadystatechange = function() {
      if (http.readyState != 4) {
        return ;
      }
      if (http.status != 200) {

        return ;
      }
      const token = JSON.parse(http.responseText)['token'];
      client.create_connection_and_auhenticate(token, (rsp) => client.restore_state_after_reauthenticating(rsp));
    };
    http.open("POST", reconnect_url.href, true);
    http.send(null);
  }

  restore_state_after_reauthenticating(rsp) {
    if (rsp.is_error()) {
      this._ui.unhandled_sittuation_modal(`server replied with error code ${rsp.error_code}`)
      return ;
    }

    if (this._file === null) {
      this._ui.hide_modals();
      return ;
    }

    if (this._file.constructor.is_editable && this._file.open_mode() == OpenMode.edit) {
      this._connection.open_file(
        this._file.node_id(),
        OpenMode.edit,
        (rsp) => {
          if (rsp.revision != this._file.revision()) {

            if (this._file.has_changes()) {

              this._ui.activate_modal_notification(
                'Note',
                `This file has been modified on remote, please copy changes and reload
                <br\>
                <br\>
                `,
                'Ok',
                () => {
                  this._ui.hide_modals();
                }
              );

            } else {

              const object = this.map_filename_to_handler(this._file.filename());
              this._file = new object(rsp, this, this._file.filename(), OpenMode.edit);

            }
          } else {

            this._ui.hide_modals();

          }
      });

    } else {

      this._connection.open_file(
        this._file.node_id(),
        OpenMode.read,
        (rsp) => {
          if (rsp.revision != this._file.revision()) {

            const object = this.map_filename_to_handler(this._file.filename());
            this._file = new object(rsp, this, this._file.filename(), OpenMode.read);

          } else {
            this._ui.hide_modals();
          }
      });

    }
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
      "Filename which should end with .md",
      "filename.md",
      (name) => this.handle_create_clicked("markdown", "create", name),
      (name) => this.handle_create_clicked("markdown", "cancel", name),
    );
  }

  handle_create_list_clicked() {
    this._ui.show_create_element_modal(
      "Filename which should end with .ls",
      "todo.ls",
      (name) => this.handle_create_clicked("list", "create", name),
      (name) => this.handle_create_clicked("list", "cancel", name),
    );
  }

  handle_create_directory_clicked() {
    this._ui.show_create_element_modal(
      "Name of the directory",
      "photos",
      (name) => this.handle_create_clicked("directory", "create", name),
      (name) => this.handle_create_clicked("directory", "cancel", name),
    );
  }

  handle_file_done_clicked() {
    if (this._file !== null) {
      if (this._file.has_changes()) {
        this._ui.nothing_to_do_sittuation_modal('File has changes, please save them first');
        return ;
      }
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
      if (this._file.has_changes()) {
        this._file.save();
      } else {
        // Better indication
        console.log('File has no edits')
      }
    }
  }

  handle_file_cancel_clicked() {
    if (this._file !== null) {
      this._file.revert_edits();
    }
  }

  handle_file_info_clicked() {
    if (this._file !== null) {
      this._ui.render_file_info(
        this._file.filename(),
        this._file.node_id(),
        this._file.revision(),
      );
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

      console.log(`Creating new Markdown file ${name} in path ${this._path_dir}`);
      this._connection.create_file_ra(name, this._path_dir, (rsp) => this.handle_create_response('markdown', rsp));
      this._ui.show_loading_modal('Creating file...');

    } else if (type_of_element == 'list') {

      if (this.map_filename_to_handler(name) !== ListFile) {
        this._ui.activate_modal_notification(
          'Note',
          `List filename should have extension ${ListFile.filename_extension}
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

      console.log(`Creating new List file ${name} in path ${this._path_dir}`);
      this._connection.create_file_ra(name, this._path_dir, (rsp) => this.handle_create_response('markdown', rsp));
      this._ui.show_loading_modal('Creating file...');

    } else if (type_of_element == 'directory') {

      console.log(`Creating new directory ${name} in path ${this._path_dir}`);
      this._connection.create_directory(name, this._path_dir, (rsp) => this.handle_create_response('directory', rsp));
      this._ui.show_loading_modal('Creating directory...');

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
    let _open_file = () => {
      const file_type = this.map_filename_to_handler(element.name);
      if (file_type) {
        // This is bit of an hax, but allow file to be opened in
        // read mode by default. Especially for usefull for list files
        if (file_type.default_open_mode === OpenMode.edit) {
          mode = file_type.default_open_mode;
        }
      }
      log(`Opening element "${element.name}" with node id ${element.node_id} with mode "${mode}"`);
      this._connection.open_file(element.node_id, mode, (rsp) => this.handle_open_file_rsp(rsp, mode, element));
    };

    this._ui.show_loading_modal(`Loading file ${element.name}`);
    if (this._file !== null) {
      this._connection.close_file(this._file.node_id(), (rsp) => _open_file());
    } else {
      _open_file();
    }
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
    this._ui.show_verify_modal(
      `Are you sure it is ok to delete "${element.name}"?`,
      () => {
        this._connection.delete_element(
          element.node_id,
          (rsp) => {
            if (rsp.is_error()) {
              this._ui.unhandled_sittuation_modal(`server replied with error code ${rsp.error_code}`);
            } else {
              this._connection.query_element_children(
                this._path_dir,
                (rsp) => {
                  this.handle_query_children_rsp(rsp, this._path_dir);
                });
            }
          }
        )
      }
    );
  }

  handle_open_file_rsp(rsp, mode, element) {

    // todo: handle case when user does not have edit permissions
    if (rsp.is_error()) {
      this._ui.unhandled_sittuation_modal(`server replied with error code ${rsp.error_code}`);
      return
    }

    const object = this.map_filename_to_handler(element.name);
    if (object === null) {
      this._ui.unhandled_sittuation_modal(`No handler found for elment with name ${element.name}`);
      return ;
    }

    this._file = new object(rsp, this, element.name, mode);
    this.update_browser_url();
    this._ui.hide_modals();
    this._ui.show_small_sidebar();
  }

  handle_server_info_clicked() {
    this._connection.query_system((msg) => {
      this._ui.server_info_modal(
        msg.fields["server-id"].value,
        msg.fields["started-at"].value,
        msg.fields["number-of-open-files"].value,
        msg.fields["max-number-of-open-files-per-connection"].value,
      );
    });
  }
}

export { Client };

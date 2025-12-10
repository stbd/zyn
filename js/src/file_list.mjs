import { Base } from './file.mjs';
import Sortable from 'sortablejs';
import {
  OpenMode,
  encode_to_bytes,
  decode_from_bytes,
  log,
} from './common.mjs';
import showdown from 'showdown';

/*
Fileformat:
"content for element 0";\n
"content for element 1";\n
*/

const CHAR_LINE_FEED = 10;
const CHAR_SEMICOLON = 59;
const CHAR_DOUBLE_QUOTES = 34;
const ELEMENT_ID = "zyn-list-element";
const ELEMENT_TEXT_ID = "zyn-list-element-text";
const ELEMENT_BUTTON_SAVE_ID = "zyn-list-element-save";
const ELEMENT_BUTTON_DELETE_ID = "zyn-list-element-delete";

class ListElement {
  constructor(id, data) {
    this._data = data
    this._tags = [];
    this._id = id;
    this._move_to_index = null;
    if (this.is_initialized()) {
      log(`List element created, id: ${this._id}, size: ${this.data_size()}`);
      
      const end_of_text = this._data.indexOf(CHAR_DOUBLE_QUOTES, 1);
      if (this._data[end_of_text + 1] != CHAR_SEMICOLON) {
        const end_of_tags = this._data.indexOf(CHAR_DOUBLE_QUOTES, end_of_text + 2);
        if (end_of_tags !== -1) {
          const tags = decode_from_bytes(this._data.slice(end_of_text + 2, end_of_tags));
          this._tags = tags.split(',').map((tag) => this._unescape_text(tag));
        }
      }
    } else {
      log(`Empty list element created, id: ${this._id}`);
    }
  }

  id() { return this._id; }
  data() { return this._data; }
  is_initialized() { return this._data !== null; }
  move_to_index(new_index) { this._move_to_index = new_index; }
  is_moved() { return this._move_to_index !== null; }
  moved_index() { return this._move_to_index; }
  clear_move() { this._move_to_index = null; }
  tags() { return this._tags; }
  add_tag(tag) {
    this._tags.push(tag);
    this._tags = [...new Set(this._tags)]; 
  }
  data_size() { 
    if (!this.is_initialized()) {
      throw "ListElement not initialized";
    }
    return this._data.length;
  }

  _escape_text(text) {
    return text.replaceAll('%', '%%').replaceAll('"', "%34").replaceAll(';', '%59');
  }

  _unescape_text(text) {
    return text.trim().replaceAll('"', '').replaceAll('%59', ';').replaceAll('%34', '"').replaceAll('%%', '%');
  }

  _tags_to_text() {
    let text = ''
    for (let tag of this._tags) {
      if (text) {
        text += ', ';
      }
      text += this._escape_text(tag);
    }
    return text
  }

  update_text(text) { 
    const escaped = this._escape_text(text);
    const data = encode_to_bytes(`"${escaped}""${this._tags_to_text()}";\n`);

    if (this._data !== null) {
      if (data.toString() === this._data.toString()) {
        return false;
      }
    }
    this._data = data;
    return true;
  }

  text() {
    const end_of_text = this._data.indexOf(CHAR_DOUBLE_QUOTES, 1);
    const text = decode_from_bytes(this._data.slice(1, end_of_text));
    return this._unescape_text(text);
  }
}

class List {
  constructor(file) {
    this._elements = [];
    this._element_id_counter = 0;
    this._file = file;
  }

  add_element(data, add_to_top_of_the_list=false) {
    const id = this._element_id_counter;
    this._element_id_counter += 1;

    const element = new ListElement(id, data);
    if (add_to_top_of_the_list) {
      this._elements.unshift(element);
    } else {
      this._elements.push(element);
    }
    return element;
  }

  find_element(element_id) {
    for (const [_, e] of this.elements()) {
      if (e.id() === element_id) {
        return e;
      }
    }
    throw `No element found with id ${element_id}`;
  }

  add_new_empty_element() {
    return this.add_element(null, true);
  }

  is_empty() { return this._elements.length == 0; }
  elements() { return this._elements.entries(); }
  element_at(index) { return this._elements[index]; }
  size() { return this._elements.length; }
  delete_element(element_id) {

    this._file._client.ui().show_loading_modal();
    log(`Deleting element with id ${element_id}`);

    let index_delete = 0;
    let element = null;
    let element_index = null;
    for (const [i, e] of this.elements()) {
      if (e.id() === element_id) {
        element_index = i;
        element = e;
        break ;
      }
      if (e.is_initialized()) {
        index_delete += e.data_size();
      }
    }

    let modifications = [{
        'type': 'delete',
        'offset': index_delete,
        'size': element.data_size(),
    }]

    this._elements.splice(element_index, 1);

    this._file._client.connection().apply_modifications(
      this._file._node_id,
      this._file._revision,
      modifications,
      (rsp) => {
        this._handle_backend_response(rsp);
      }
    )
  }

  save_element(element_id, updated_text) {
    
    log(`Saving element with id ${element_id}`);
    let modifications = [];
    let element = null;
    let element_index = null;
    let was_moved = false;
    let index_delete = 0;

    for (const [i, e] of this.elements()) {
      if (e.id() === element_id) {
        element = e;
        element_index = i;
        break ;
      }
      if (e.is_initialized()) {
        index_delete += e.data_size();
      }
    }

    if (element.is_initialized()) {
      modifications.push({
        'type': 'delete',
        'offset': index_delete,
        'size': element.data_size(),
      })
    }

    if (element_index === element.moved_index()) {
      element.clear_move();
    }

    let index_insert = 0;
    if (element.is_moved()) {
      was_moved = true;
      let new_index = element.moved_index();
      while (true) {
        let next_index = null;
        if (element_index === new_index) {
          break ;
        } else if (element_index > new_index) {
          next_index = element_index - 1;
        } else if (element_index < new_index) {
          next_index = element_index + 1;
        }
        const s = this._elements[next_index];
        this._elements[next_index] = element;
        this._elements[element_index] = s;
        element_index = next_index;
      }

      for (const e of this._elements.values()) {
        if (e.id() === element_id) {
          break ;
        }
        if (e.is_initialized()) {
          index_insert += e.data_size();
        }
      }
    } else {
      index_insert = index_delete;
    }

    const was_changed = element.update_text(updated_text);
    if (was_changed || was_moved) {
      this._file._client.ui().show_loading_modal();
      modifications.push({
        'type': 'add',
        'offset': index_insert,
        'bytes': element.data(),
      })

      if (modifications.length > 1) {
        if (modifications[0].offset > modifications[1].offset) {
          log('Swapping modification order')
          const m = modifications[0];
          modifications[0] = modifications[1];
          m.offset += modifications[1].bytes.length;
          modifications[1] = m;
        }
      }

      this._file._client.connection().apply_modifications(
        this._file._node_id,
        this._file._revision,
        modifications,
        (rsp) => {
          this._handle_backend_response(rsp);
          element.clear_move();
        }
      )
    }
    return element;
  }

  _handle_backend_response(rsp) {
    
    if (rsp.is_error()) {
      log(rsp)
      this._file._client.ui().unhandled_sittuation_modal(
        `Failed to complete operation 
        <br />
        Server responded with error code ${rsp.error_code}
        `
      );
    } else {
      this._file._revision = rsp.revision;
      this._file._enable_sorting();
      this._file._client.ui().hide_modals();
    }
  }
}

class ListFile extends Base {
  static filename_extension = '.ls';
  static is_editable = false;
  static default_open_mode = OpenMode.edit;
  static ELEMENT_NAME_LIST = 'zyn-list-page-elements';
  static ELEMENT_NAME_ADD_BUTTON = 'zyn-list-page-add-button';

  constructor(open_rsp, client, filename, mode) {
    super(open_rsp, client, filename);
    this._list_content = new List(this);
    this._mode = mode;
    this._sortable = null;
    this._edited = false;
    this._converter = new showdown.Converter({
      'simplifiedAutoLink': true,
      'simpleLineBreaks': true,
    });

    this._client.ui().show_loading_modal('Loading file content...');
    this._client.ui().file_area_button_done(false);
    this._client.ui().file_area_button_edit(false);
    this._client.ui().file_area_button_save(false);
    this._client.ui().file_area_button_cancel(false);
    if (open_rsp.size === 0) {
      this.render();
    } else {
      this.read_file_content(
        0,
        open_rsp.size,
        (data, revision) => {
          this._revision = revision;
          try {
            this._parse_data(data);
            this.render();
          } catch (error) {            
            log(`Error while parsing data "${error}"`);
          }
        }
      )
    }
  }

  open_mode() { return this._mode; }

  _parse_data(data) {
    let index_start = 0;

    log(`Parsing ${data.length} bytes of data`)

    while (true) {
      const index_end = data.indexOf(CHAR_SEMICOLON, index_start);

      if (index_end == -1) {
        this._client.ui().unhandled_sittuation_modal(
          `File malformed, Missing end of line ";" near:
          <br />
          <br />
        ${decode_from_bytes(data.slice(index_start))}
          `
        );
        throw('Malformed data');
      }

      const d = data.slice(index_start, index_end);
      let row = new Uint8Array(d.length + 2);
      row.set(d, 0);
      row.set([CHAR_SEMICOLON, CHAR_LINE_FEED], d.length);

      let e = this._list_content.add_element(row);
      if (
        data.slice(index_start, index_start + e.data_size()).toString() 
        != e.data().toString()
      ) {
        this._client.ui().unhandled_sittuation_modal(
          `File malformed, please make sure it is in the right format. Missing end of line ";\\n" near:
          <br />
          <br />
        ${decode_from_bytes(data.slice(index_start, index_end + 2))}
          `
        );
        throw('Malformed data2');
      }

      index_start = index_end + 2;
      if (index_start === data.length) {
        break ;
      }
    }
  }

  handle_notification(notification) {}
  has_changes() { return this._edited; }

  render() {

    log('Rendering list file')

    this._client.ui().set_file_content(
      `
<button id="${ListFile.ELEMENT_NAME_ADD_BUTTON}" class="zyn-button m-3 px-5 h-[3rem]" >Add new row</button>
<div id="${ListFile.ELEMENT_NAME_LIST}" class="flex flex-col m-4">
</div>
      `,
      () => this.render_callback()
    );
    this._client.ui().hide_modals();
  }

  render_callback() {
    for (const [_, element] of this._list_content.elements()) {
      this._add_list_element(element);
    }

    this._sortable = new Sortable(
      this._client.ui().document().getElementById(ListFile.ELEMENT_NAME_LIST),
      {
        onEnd: (e) => this.handle_element_moved(e),
      },
    );
    this._client.ui().document().getElementById(ListFile.ELEMENT_NAME_ADD_BUTTON).addEventListener(
      'click',
      (_) => this._handle_add_button_clicked(),
    );
  }

  handle_element_moved(event) {
    this._set_row_editable(event.item, true);
    this._list_content.find_element(parseInt(event.item.dataset.row_id)).move_to_index(event.newIndex);
    this._disable_sorting();
  }

  _set_row_editable(row, editable) {
    let save_button = row.querySelector(`#${ELEMENT_BUTTON_SAVE_ID}`);
    let text = row.querySelector(`#${ELEMENT_TEXT_ID}`);

    if (editable) {
      save_button.classList.remove('hidden');
      text.setAttribute('contenteditable', true);
      text.classList.add('bg-zyn_green', 'rounded-md');
    } else {
      text.classList.remove('bg-zyn_green');
      text.setAttribute('contenteditable', false);
      save_button.classList.add('hidden');
    }
  }

  _add_list_element(element, add_to_top_of_the_list=false) {
    const row = this._client.ui().document().createElement('div');
    const text = this._client.ui().document().createElement('span');
    const save_button = this._client.ui().document().createElement('button');
    const delete_button = this._client.ui().document().createElement('button');

    row.classList.add('flex', 'rounded-md', 'border-2', 'my-1');
    row.id = ELEMENT_ID
    text.classList.add('flex-1', 'mx-2', 'my-2', 'p-1');
    text.id = ELEMENT_TEXT_ID
    save_button.classList.add('flex', 'justify-end', 'zyn-button', 'mx-2', 'px-2', 'my-2');
    save_button.id = ELEMENT_BUTTON_SAVE_ID;
    delete_button.classList.add('flex', 'justify-end', 'zyn-button', 'mx-2', 'px-2', 'my-2');
    delete_button.id = ELEMENT_BUTTON_DELETE_ID;

    save_button.innerText = 'Save';
    save_button.addEventListener('click', (event) => this._handle_row_save_button_clicked(event.srcElement));
    delete_button.addEventListener('click', (event) => this._handle_row_delete_button_clicked(event.srcElement));

    delete_button.innerText = 'Delete';

    row.appendChild(text);
    row.appendChild(save_button);
    row.appendChild(delete_button);

    row.dataset.row_id = element.id();      
    if (element.is_initialized()) {
      this._set_row_editable(row, false);
      text.innerHTML = this._converter.makeHtml(element.text());
      text.addEventListener('dblclick', (event) => this._handle_row_text_element_clicked(event.srcElement), {once: true});
    } else {
      this._set_row_editable(row, true);
    }

    let list = this._client.ui().document().getElementById(ListFile.ELEMENT_NAME_LIST);

    if (list.childNodes.length > 0 && add_to_top_of_the_list) {
      list.insertBefore(row, list.childNodes[0]);
    } else {
      list.appendChild(row);
    }
  }

  _handle_add_button_clicked() {
    const e = this._list_content.add_new_empty_element();
    this._add_list_element(e, true);
  }

  _handle_row_save_button_clicked(element) {
    let row = element.parentNode;

    this._set_row_editable(row, false);
    let text = row.querySelector(`#${ELEMENT_TEXT_ID}`);
    
    text.addEventListener('dblclick', (event) => this._handle_row_text_element_clicked(event.srcElement), {once: true});
    const e = this._list_content.save_element(parseInt(row.dataset.row_id), text.innerText);
    text.textContent = '';
    text.innerHTML = this._converter.makeHtml(e.text())
  }

  _handle_row_text_element_clicked(element) {
    let row = element.closest(`#${ELEMENT_ID}`);
    let text = element.closest(`#${ELEMENT_TEXT_ID}`);

    this._set_row_editable(row, true);
    text.textContent = '';
    text.innerText = this._list_content.find_element(parseInt(row.dataset.row_id)).text();

    let save_button = row.querySelector(`#${ELEMENT_BUTTON_SAVE_ID}`);
    save_button.classList.remove('hidden');
    this._disable_sorting();
  }

  _handle_row_delete_button_clicked(element) {
    let row = element.parentNode;
    this._list_content.delete_element(parseInt(row.dataset.row_id))
    
    let list = this._client.ui().document().getElementById(ListFile.ELEMENT_NAME_LIST);
    list.removeChild(row);
  }

  _disable_sorting() {
    this._sortable.option("disabled", true);
  }

  _enable_sorting() {
    this._sortable.option("disabled", false);
  }
}

export { ListFile };

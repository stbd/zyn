
function _toggle_create_element_content() {
    var input = document.getElementById('input-create-element-content');
    var class_show = 'w3-show';
    if (input.className.indexOf(class_show) == -1) {
        input.classList.add(class_show);
    } else {
        input.classList.remove(class_show);
    }
}

function _handle_get_create_element_name() {
    var input = document.getElementById('input-create-element-name');
    var name = input.value;
    if (name.length == 0) {
        input.style.backgroundColor = "red";
        return null;
    }
    input.style.backgroundColor = "white";
    input.value = '';
    _toggle_create_element_content();
    return name;
}

var _ask_user_modal_continue_callback = null;
function _ask_user_modal_continue() {
    var callback = _ask_user_modal_continue_callback;
    _ask_user_modal_continue_callback = null;
    if (callback) {
        callback();
    }
}

function _show_ask_user_modal(show_text, callback) {
    document.getElementById("zyn-ask-user-modal").style.display = "block";
    document.getElementById("zyn-ask-user-description").innerText = show_text;
    _ask_user_modal_continue_callback = callback;
}

function _handle_create_random_access_clicked() {
    var name = _handle_get_create_element_name();
    if (name === null) {
        return ;
    }

    var pwd = _current_workdir.clone();
    zyn_send_message(
        'create',
        {
            "type": "file",
            "file-type": "random-access",
            "name": name,
            "parent": pwd.to_str(),
        },
        new Transaction(function (rsp, _) {
            _fetch_and_set_folder_content(pwd.clone());
        })
    );
}

function _handle_create_direcotry_clicked() {
    var name = _handle_get_create_element_name();
    if (name === null) {
        return ;
    }

    var pwd = _current_workdir.clone();
    zyn_send_message(
        'create',
        {
            "type": "dir",
            "name": name,
            "parent": pwd.to_str(),
        },
        new Transaction(function (rsp, _) {
            _fetch_and_set_folder_content(pwd.clone());
        })
    );
}

function _show_loading_indication() {
    document.getElementById('zyn-loading-modal').style.display = 'block';
}

function _hide_loading_indication() {
    document.getElementById('zyn-loading-modal').style.display = 'none';
}

function _handle_delete_element_clicked(name, node_id) {
    var event = event || window.event;
    if (event.target.id === "zyn-delete-file-button") {
        event.stopPropagation();

        var text = 'Are you sure you want to delete file "'
        text += name;
        text += '" (Node Id: '
        text += node_id;
        text += ')?'

        var callback = function() {
            _delete_element(node_id);
        };
        _show_ask_user_modal(text, callback);
    }
}

function _delete_element(node_id) {
    zyn_send_message(
        'delete',
        {
            "node-id": node_id,
        },
        new Transaction(function (rsp, _) {
            _fetch_and_set_folder_content(_current_workdir.clone());
        })
    );
}

function _handle_open_in_edit_mode_button_clicked(event, name, node_id) {
    var event = event || window.event;
    if (event.target.id === "zyn-open-file-in-edit-mode") {
        event.stopPropagation();

        _show_loading_indication();
        zyn_load_file(
            node_id,
            name,
            ZYN_OPEN_FILE_MODE_WRITE,
            new Transaction(
                _handle_load_file_rsp,
                state={'mode': 'edit'}
            ));
    }
}

function _create_folder_list_element(row, element_data) {
    row.addEventListener('click', function() { _handle_folder_content_list_item_clicked(
        element_data['element-type'],
        element_data['name'],
        element_data['node-id']);
                                             });

    var t = ''
    if (element_data['element-type'] === 'file') {
        t = 'File'

        file_extension = zyn_get_file_extension(element_data['name']);
        if (file_extension !== null && is_editable_file(file_extension)) {
            var cell = row.insertCell();
            cell.innerHTML =
                '<button'
                + ' id="zyn-open-file-in-edit-mode"'
                + ' class="w3-button w3-border"'
                + ' onclick="(function() { _handle_open_in_edit_mode_button_clicked(event, \'' + element_data['name'] + '\', ' + element_data['node-id'] + '); }) ()"'
                + ' >Edit</button>';
        } else {
            row.insertCell();
        }
    } else if (element_data['element-type'] === 'dir') {
        t = 'Dir'
        row.insertCell();
    } else {
        _unhandled('_create_folder_list_element: element_data["element-type"]: ' + element_data['element-type'])
        return ;
    }

    row.insertCell().innerText = t;
    row.insertCell().innerText = element_data['node-id'];
    row.insertCell().innerText = element_data['name'];
    row.insertCell().innerHTML =
        '<button'
        + ' id="zyn-delete-file-button"'
        + ' class="w3-button"'
        + ' onclick="(function() { _handle_delete_element_clicked(\''
        + element_data['name'] + '\', '
        + element_data['node-id'] + '); }) ()"'
        + ' >Delete</button>';

    row.style.cursor = 'pointer';
    row.classList.add('w3-hover-light-gray');
}

function _fetch_and_set_folder_content(path)
{
    _show_loading_indication();
    zyn_load_folder_contents(path, new Transaction(function (rsp, _) {
        _hide_loading_indication();
        var workdir_content = document.getElementById("zyn-workdir-content");
        var elements = rsp['elements'];

        while (workdir_content.firstChild) {
            workdir_content.removeChild(workdir_content.firstChild);
        }

        var content = document.createElement('table');
        content.id = 'zyn-workdir-content-listing';
        content.classList.add('w3-table');
        content.classList.add('w3-bordered');

        var row = content.insertRow(0);

        var cell = document.createElement("th");
        cell.style.width = '10%';
        row.appendChild(cell)

        var cell = document.createElement("th");
        cell.innerText = 'Type';
        cell.style.width = '10%';
        row.appendChild(cell)

        var cell = document.createElement("th");
        cell.innerText = 'Node Id';
        cell.style.width = '20%';
        row.appendChild(cell)

        var cell = document.createElement("th");
        cell.innerText = 'Name';
        cell.style.width = '50%';
        row.appendChild(cell)

        var cell = document.createElement("th");
        cell.style.width = '10%';
        row.appendChild(cell)

        for (var i = 0; i < elements.length; i++) {
            var row = content.insertRow(i + 1);
            _create_folder_list_element(row, elements[i]);
        }

        workdir_content.appendChild(content);
        _sort_workdir_content(content, _sort_workdir_content_by_type);

        workdir_content.style.minHeight = String(workdir_content.scrollHeight + 500) + 'px';
    }));
}

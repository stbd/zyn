import { Client } from '../src/client.mjs';
import { Connection } from '../src/connection.mjs';
import sinon from 'sinon';

class Controller {
  constructor() {}
  show_loading_modal() {}
  unhandled_sittuation_modal() {}
  hide_modals() {}
  file_area_button_done() {}
  file_area_button_edit() {}
  file_area_button_save() {}
  file_area_button_cancel() {}
  set_file_content() {}
}

function init_client_stub() {
  var stub_controller = sinon.createStubInstance(Controller);
  var stub_connection = sinon.createStubInstance(Connection);
  var stub_client = sinon.createStubInstance(Client, {
    ui: sinon.stub().returns(stub_controller),
    connection: sinon.stub().returns(stub_connection),
  });

  return {
    'client': stub_client,
    'controller': stub_controller,
    'connection': stub_connection,
  };
}

export { init_client_stub }

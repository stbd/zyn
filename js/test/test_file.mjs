import {
  OpenRsp,
  ReadRsp,
  EditNotification,
} from '../src/messages.mjs';
import {
  ZynFileType,
  encode_to_bytes,
  decode_from_bytes,
  OpenMode,
} from '../src/common.mjs';
import {
  ReadState,
  Base,
} from '../src/file.mjs';
import {
  MarkdownFile,
} from '../src/file_markdown.mjs';
import {
  ListFile,
} from '../src/file_list.mjs';
import { init_client_stub } from './common.mjs';

import assert from 'assert';
import sinon from 'sinon';

const FILENAME_MD = 'file.md'
const FILE_TYPE_RA = 0;
const DEFAULT_NODE_ID = 45;
const DEFAULT_PAGE_SIZE = 1024;

function _create_open_req(
  file_size,
  page_size=DEFAULT_PAGE_SIZE,
  node_id=DEFAULT_NODE_ID,
  file_type=FILE_TYPE_RA,
  error_code=0
){
  return new OpenRsp(0, 1, error_code, node_id, 1, file_size, page_size, file_type);
}

function _create_read_rsp(
  bytes,
  offset=0,
  revision=0,
  error=0,
) {
  let rsp = new ReadRsp(0, 1, error, revision, offset, bytes.length);
  rsp.add_data(bytes);
  return rsp;
}

function _create_edit_notification(
  type_of_notification,
  offset,
  size,
  revision=0,
  node_id=DEFAULT_NODE_ID,
) {
  return new EditNotification(type_of_notification, node_id, revision, offset, size);
}

function _init_base(
  file_size,
  page_size=DEFAULT_PAGE_SIZE,
) {
  const open_rsp = _create_open_req(file_size, page_size);
  const stubs = init_client_stub();

  return {
    'open_rsp': open_rsp,
    'stubs': stubs,
  }
}

function _compare_arrays(a, b) {
  if (a.length != b.length) {
    return false;
  }
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) {
      return false;
    }
  }
  return true;
}


describe('ReadState', function () {
  it('Test single block', function () {
    const callback = sinon.stub();
    let s = new ReadState(0, 10, {_page_size: 10}, callback);
    const data = new Uint8Array([1, 2, 3, 4, 5, 6, 7, 8, 9, 0]);
    const b1 = s.next_block();
    assert.equal(b1.start, 0);
    assert.equal(b1.size, 10);
    s.add_response(_create_read_rsp(data, 0, 1));
    assert(s.is_complete())
    s.complete();
    assert(_compare_arrays(callback.getCall(0).args[0], data))
    assert(_compare_arrays(callback.getCall(0).args[1], 1));
  });

  it('Test single block with offset', function () {
    const callback = sinon.stub();
    let s = new ReadState(5, 2, {_page_size: 10}, callback);
    const data = new Uint8Array([5, 6]);
    const b1 = s.next_block();
    assert.equal(b1.start, 5);
    assert.equal(b1.size, 2);
    s.add_response(_create_read_rsp(data, 5, 2));
    assert(s.is_complete())
    s.complete();
    assert(_compare_arrays(callback.getCall(0).args[0], data))
    assert(_compare_arrays(callback.getCall(0).args[1], 2));
  });

  it('Test two blocks', function () {
    const callback = sinon.stub();
    let s = new ReadState(0, 20, {_page_size: 10}, callback);
    const data = new Uint8Array([
      1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
      11, 12, 13, 14, 15, 16, 17, 18, 19, 20
    ]);
    const b1 = s.next_block();
    assert.equal(b1.start, 0);
    assert.equal(b1.size, 10);
    s.add_response(_create_read_rsp(data.slice(0, 10), 0, 1));
    assert(!s.is_complete())
    const b2 = s.next_block();
    assert.equal(b2.start, 10);
    assert.equal(b2.size, 10);
    s.add_response(_create_read_rsp(data.slice(10, 20), 0, 2));
    assert(s.is_complete())
    s.complete();
    assert(_compare_arrays(callback.getCall(0).args[0], data))
    assert(_compare_arrays(callback.getCall(0).args[1], 2));
  });

  it('Test two blocks with second block not full page ', function () {
    const callback = sinon.stub();
    let s = new ReadState(0, 15, {_page_size: 10}, callback);
    const data = new Uint8Array([
      1, 2, 3, 4, 5, 6, 7, 8, 9, 0,
      11, 12, 13, 14, 15
    ]);
    const b1 = s.next_block();
    assert.equal(b1.start, 0);
    assert.equal(b1.size, 10);
    s.add_response(_create_read_rsp(data.slice(0, 10), 0, 1));
    assert(!s.is_complete());
    assert(!s.is_error());
    const b2 = s.next_block();
    assert.equal(b2.start, 10);
    assert.equal(b2.size, 5);
    s.add_response(_create_read_rsp(data.slice(10, 15), 0, 2));
    assert(s.is_complete())
    s.complete();
    assert(_compare_arrays(callback.getCall(0).args[0], data))
    assert(_compare_arrays(callback.getCall(0).args[1], 2));
  });

});

describe('Base', function () {
  describe('read_file_content', function () {
    it('should read the whole block when size less than page size', function () {
      const data = 'data';
      const offset = 0;
      const size = data.length;
      const stub = sinon.stub();
      const resources = _init_base(size);
      const revision = 1;
      let file = new Base(resources.open_rsp, resources.stubs.client, FILENAME_MD);

      file.read_file_content(offset, size, stub);
      assert.equal(resources.stubs.connection.read_file.getCall(0).args[1], offset);
      assert.equal(resources.stubs.connection.read_file.getCall(0).args[0], DEFAULT_NODE_ID);
      assert.equal(resources.stubs.connection.read_file.getCall(0).args[2], size);

      resources.stubs.connection.read_file.getCall(0).args[3](
        _create_read_rsp(encode_to_bytes(data), 0, revision)
      );
      assert.equal(decode_from_bytes(stub.getCall(0).args[0]), data)
      assert.equal(stub.getCall(0).args[1], revision)
    });

    it('should read the whole block when size is equal to page size', function () {
      const data = '1234567890';
      const offset = 0;
      const size = data.length;
      const page_size = 10;
      const stub = sinon.stub();
      const resources = _init_base(size, page_size);
      const revision = 1;
      let file = new Base(resources.open_rsp, resources.stubs.client, FILENAME_MD);

      file.read_file_content(offset, size, stub);
      assert.equal(resources.stubs.connection.read_file.getCall(0).args[1], offset);
      assert.equal(resources.stubs.connection.read_file.getCall(0).args[0], DEFAULT_NODE_ID);
      assert.equal(resources.stubs.connection.read_file.getCall(0).args[2], size);

      resources.stubs.connection.read_file.getCall(0).args[3](
        _create_read_rsp(encode_to_bytes(data), 0, revision)
      );
      assert.equal(decode_from_bytes(stub.getCall(0).args[0]), data)
      assert.equal(stub.getCall(0).args[1], revision)
    });

    it('should read the whole block when size is larger than page size', function () {
      const data = '1234567890';
      const offset = 0;
      const size = data.length;
      const page_size = 5;
      const stub = sinon.stub();
      const resources = _init_base(size, page_size);
      const revision = 1;
      let file = new Base(resources.open_rsp, resources.stubs.client, FILENAME_MD);

      file.read_file_content(offset, size, stub);
      assert.equal(resources.stubs.connection.read_file.getCall(0).args[1], offset);
      assert.equal(resources.stubs.connection.read_file.getCall(0).args[0], DEFAULT_NODE_ID);
      assert.equal(resources.stubs.connection.read_file.getCall(0).args[2], page_size);

      resources.stubs.connection.read_file.getCall(0).args[3](
        _create_read_rsp(encode_to_bytes(data.slice(0, 5)), 0, revision)
      );

      assert.equal(resources.stubs.connection.read_file.getCall(1).args[1], page_size);
      assert.equal(resources.stubs.connection.read_file.getCall(1).args[0], DEFAULT_NODE_ID);
      assert.equal(resources.stubs.connection.read_file.getCall(1).args[2], page_size);

      resources.stubs.connection.read_file.getCall(0).args[3](
        _create_read_rsp(encode_to_bytes(data.slice(5, 10)), 5, revision)
      );

      assert.equal(decode_from_bytes(stub.getCall(0).args[0]), data)
      assert.equal(stub.getCall(0).args[1], revision)
    });

  });
});

describe('Markdown', function () {
  describe('handle_notification', function () {
    it('should handle insert notification', function () {
      const data = 'data';
      const size = data.length;
      const stub = sinon.stub();
      const resources = _init_base(size);
      const edit_offset = 2;
      let file = new MarkdownFile(resources.open_rsp, resources.stubs.client, FILENAME_MD, OpenMode.read);

      resources.stubs.connection.read_file.getCall(0).args[3](
        _create_read_rsp(encode_to_bytes(data), 0, 1)
      );

      file.handle_notification(_create_edit_notification('insert', edit_offset, 2));
      assert.equal(resources.stubs.connection.read_file.getCall(1).args[1], edit_offset);
      assert.equal(resources.stubs.connection.read_file.getCall(1).args[2], 2);
      resources.stubs.connection.read_file.getCall(1).args[3](
        _create_read_rsp(encode_to_bytes('12'), edit_offset, 2)
      );

      assert.equal(decode_from_bytes(file._content), 'da12ta');
    });

    it('should handle delete notification', function () {
      const data = 'data1234';
      const size = data.length;
      const stub = sinon.stub();
      const resources = _init_base(size);
      const edit_offset = 2;
      let file = new MarkdownFile(resources.open_rsp, resources.stubs.client, FILENAME_MD, OpenMode.read);

      resources.stubs.connection.read_file.getCall(0).args[3](
        _create_read_rsp(encode_to_bytes(data), 0, 1)
      );

      file.handle_notification(_create_edit_notification('delete', edit_offset, 2));
      assert.equal(decode_from_bytes(file._content), 'da1234');
    });

    it('should handle modify notification', function () {
      const data = 'data';
      const size = data.length;
      const stub = sinon.stub();
      const resources = _init_base(size);
      const edit_offset = 1;
      let file = new MarkdownFile(resources.open_rsp, resources.stubs.client, FILENAME_MD, OpenMode.read);

      resources.stubs.connection.read_file.getCall(0).args[3](
        _create_read_rsp(encode_to_bytes(data), 0, 1)
      );

      file.handle_notification(_create_edit_notification('modify', edit_offset, 2));
      assert.equal(resources.stubs.connection.read_file.getCall(1).args[1], edit_offset);
      assert.equal(resources.stubs.connection.read_file.getCall(1).args[2], 2);
      resources.stubs.connection.read_file.getCall(1).args[3](
        _create_read_rsp(encode_to_bytes('12'), edit_offset, 2)
      );

      assert.equal(decode_from_bytes(file._content), 'd12a');
    });
  });
});

describe('ListFile', function () {
  it('should parse simple list content correctly', function () {
    const data = '"item 1";\n"item 2";\n';
    const resources = _init_base(data.length);
    let file = new ListFile(resources.open_rsp, resources.stubs.client, 'test.ls', OpenMode.edit);

    resources.stubs.connection.read_file.getCall(0).args[3](
      _create_read_rsp(encode_to_bytes(data), 0, 1)
    );

    assert.equal(file._list_content.size(), 2);
    assert.equal(file._list_content.element_at(0).text(), 'item 1');
    assert.equal(file._list_content.element_at(1).text(), 'item 2');
  });

  it('should handle move correctly', function () {
    const data = '"item 1";\n"item 2";\n"item 3";\n';
    const resources = _init_base(data.length);
    let file = new ListFile(resources.open_rsp, resources.stubs.client, 'test.ls', OpenMode.edit);

    resources.stubs.connection.read_file.getCall(0).args[3](
      _create_read_rsp(encode_to_bytes(data), 0, 1)
    );

    let e = file._list_content.element_at(0);
    e.move_to_index(1);
    file._list_content.save_element(e.id(), e.text());

    // Assert that system first deletes row 0, then adds it to second place
    const mods = resources.stubs.connection.apply_modifications.getCall(0).args[2];
    assert.equal(mods.length, 2)
    assert.equal(mods[0].type, 'delete')
    assert.equal(mods[0].offset, 0)
    assert.equal(mods[0].size, 10)
    assert.equal(mods[1].type, 'add')
    assert.equal(mods[1].offset, 10)
  });

  it('should save element with updated text', function () {
    const data = '"item 1";\n"item 2";\n';
    const resources = _init_base(data.length);
    let file = new ListFile(resources.open_rsp, resources.stubs.client, 'test.ls', OpenMode.edit);

    resources.stubs.connection.read_file.getCall(0).args[3](
      _create_read_rsp(encode_to_bytes(data), 0, 1)
    );

    const element = file._list_content.element_at(0);
    const updated = file._list_content.save_element(element.id(), 'updated text');

    assert.equal(updated.text(), 'updated text');
    assert.equal(file._list_content.element_at(0).text(), 'updated text');

    // Verify backend call
    const mods = resources.stubs.connection.apply_modifications.getCall(0).args[2];
    assert.equal(mods.length, 2);
    assert.equal(mods[0].type, 'delete');
    assert.equal(mods[1].type, 'add');
  });

  it('should not make backend call if text unchanged', function () {
    const data = '"item 1";\n"item 2";\n';
    const resources = _init_base(data.length);
    let file = new ListFile(resources.open_rsp, resources.stubs.client, 'test.ls', OpenMode.edit);

    resources.stubs.connection.read_file.getCall(0).args[3](
      _create_read_rsp(encode_to_bytes(data), 0, 1)
    );

    const element = file._list_content.element_at(0);
    file._list_content.save_element(element.id(), 'item 1');

    assert.equal(resources.stubs.connection.apply_modifications.callCount, 0);
  });

  it('should encode semicolons correctly when saving', function () {
    const resources = _init_base(0);
    let file = new ListFile(resources.open_rsp, resources.stubs.client, 'test.ls', OpenMode.edit);

    const element = file._list_content.add_new_empty_element();
    file._list_content.save_element(element.id(), 'text with ; semicolon');

    const mods = resources.stubs.connection.apply_modifications.getCall(0).args[2];
    assert.equal(decode_from_bytes(mods[0].bytes), '"text with %59 semicolon";\n');
    assert.equal(file._list_content.element_at(0).text(), 'text with ; semicolon');
  });
});

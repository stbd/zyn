const ElementType = Object.freeze({
    'file': 'file',
    'directory': 'dir',
});

const ZynFileType = Object.freeze({
    'ra': 'random-access',
    'blob': 'blob',
});

const OpenMode = Object.freeze({
    'read': 'read',
    'edit': 'edit',
})

class FilesystemElement {
    constructor() {
    }

    is_file() { throw 'not implemented'; }
    is_directory() { throw 'not implemented'; }
}

class FilesystemElementFile extends FilesystemElement {
    constructor(name, node_id, revision, file_type, size, is_open) {
        super();
        this.name = name;
        this.node_id = node_id;
        this.revision = revision;
        this.file_type = file_type;
        this.size = size;
        this.is_open = is_open;
    }

    is_file() { return true; }
    is_directory() { return false; }
}

class FilesystemElementDirectory extends FilesystemElement {
    constructor(name, node_id, authtority_read, authority_write) {
        super();
        this.name = name;
        this.node_id = node_id;
        this.authtority_read = authtority_read;
        this.authority_write = authority_write;
    }

    is_file() { return false; }
    is_directory() { return true; }
}

class Authority {
    constructor(type_of, name) {
        this._type_of = type_of;
        this.name = name;
    }
}


function encode_to_bytes(text) {
  return new TextEncoder().encode(text);
}


function decode_from_bytes(bytes) {
  return new TextDecoder().decode(bytes);
}


function log(msg) {
  console.log(msg)
}


export {
  Authority,
  ElementType,
  ZynFileType,
  OpenMode,
  FilesystemElementFile,
  FilesystemElementDirectory,
  encode_to_bytes,
  decode_from_bytes,
  log,
};

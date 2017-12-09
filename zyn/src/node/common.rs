use std::io::{ Error };
use std::str::{ Utf8Error };
use std::path::{ PathBuf };
use std::fmt::{ Display, Formatter, Result as FmtResult };

use chrono::{ UTC };

use node::user_authority::{ Id };

pub type NodeId = u64;
pub type Buffer = Vec<u8>;
pub type Timestamp = i64;
pub type FileRevision = u64;

pub static NODE_ID_ROOT: NodeId = 0;
pub static ADMIN_GROUP: Id = Id::Group(0);
pub static ADMIN_GROUP_NAME: & str = "admin";

pub fn utc_timestamp() -> Timestamp {
    UTC::now().timestamp()
}

pub fn log_crypto_context_error() {
    error!("Failed to create crypto context");
}

pub fn log_crypto_usage_error() {
    error!("Failed to encrypt/decrypt data");
}

pub fn log_io_error_to_unit_err(error_code: Error) {
    error!("IO error, error_code={}", error_code);
}

pub fn log_utf_error(error_code: Utf8Error) {
    error!("UTF8 error, error_code={}", error_code);
}

pub enum FileDescriptor {
    Path(PathBuf),
    NodeId(NodeId)
}

impl FileDescriptor {
    pub fn from_path(path: PathBuf) -> Result<FileDescriptor, ()> {
        if ! path.is_absolute() {
            return Err(());
        }
        Ok(FileDescriptor::Path(path))
    }

    pub fn from_node_id(id: NodeId) -> Result<FileDescriptor, ()> {
        Ok(FileDescriptor::NodeId(id))
    }
}

impl Display for FileDescriptor {
    fn fmt(& self, f: & mut Formatter) -> FmtResult {
        match *self {
            FileDescriptor::Path(ref path) => {
                write!(f, "Path:{}", path.display())
            },
            FileDescriptor::NodeId(ref id) => {
                write!(f, "NodeId:{}", id)
            },
        }
    }
}

#[derive(Clone)]
pub enum OpenMode {
    Read,
    ReadWrite,
}

#[derive(Clone, PartialEq)]
pub enum FileType {
    RandomAccess,
    Blob,
}

impl Display for FileType {
    fn fmt(& self, f: & mut Formatter) -> FmtResult {
        match *self {
            FileType::RandomAccess => {
                write!(f, "RandomAccess")
            },
            FileType::Blob => {
                write!(f, "Blob")
            },
        }
    }
}

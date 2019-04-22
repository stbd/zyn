use std::path::{ Path, PathBuf };
use std::slice::{ Iter };
use std::str;
use std::vec::{ Vec };

use serde::de::{ Error as SerdeError };
use serde::{ Deserialize, Deserializer, Serialize, Serializer };
use serde_json;

use node::crypto::{ Context };
use node::user_authority::{ Id };
use node::common::{ log_utf_error, Timestamp, FileRevision, NodeId, FileType };


fn path_with_version(path_basename: & Path, version_number: u32) -> PathBuf {
    path_basename.with_extension(format!("{}", version_number))
}

fn find_serialized_file_version(path_basename: & Path, latest_version: u32) -> Result<(u32, PathBuf), ()> {
    let mut version = latest_version;
    while version > 0 {
        let path = path_with_version(path_basename, version);
        if path.exists() {
            return Ok((version, path));
        }
        version -= 1;
    }
    Err(())
}

fn log_serde_error(error: serde_json::Error) {
    error!("Serde error, error={}", error);
}

#[derive(Serialize, Deserialize)]
struct SerializedId {
    id_type: u8,
    id_value: u64,
}

impl Serialize for Id {
    fn serialize<S>(& self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        let state = self.state();
        SerializedId { id_type: state.0, id_value: state.1 }.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Id {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        let serialized: SerializedId = Deserialize::deserialize(deserializer) ? ;
        let id = Id::from_state((serialized.id_type, serialized.id_value))
            .map_err(| () | SerdeError::custom("Failed to parse Id"))
            ? ;
        Ok(id)
    }
}

#[derive(Serialize, Deserialize)]
struct SerializedFileType {
    file_type: u8,
}

impl Serialize for FileType {
    fn serialize<S>(& self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        match *self {
            FileType::RandomAccess => SerializedFileType { file_type: 0 },
            FileType::Blob => SerializedFileType { file_type: 1 },
        }.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for FileType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        let serialized: SerializedFileType = Deserialize::deserialize(deserializer) ? ;

        let type_of = match serialized {
            SerializedFileType { file_type: 0 } => FileType::RandomAccess,
            SerializedFileType { file_type: 1 } => FileType::Blob,
            _ => return Err(SerdeError::custom("Failed to parse FileType")),
        };
        Ok(type_of)
    }
}

////////////////////////////////////////////////////////////////////

#[derive(Serialize, Deserialize)]
pub struct SerializedNode {
    pub client_input_buffer_size: u64,
    pub page_size_for_random_access_files: u64,
    pub page_size_for_blob_files: u64,
}

impl SerializedNode {
    fn current_version() -> u32 {
        1
    }

    pub fn write(& self, crypto_context: Context, path_basename: & Path)
                 -> Result<(), ()> {

        let path = path_with_version(path_basename, SerializedNode::current_version());

        debug!("Serializing node settings to path={}", path.display());


        let serialized = serde_json::to_string(& self)
            .map_err(log_serde_error)
            ? ;

        crypto_context.encrypt_to_file(& serialized.into_bytes(), & path)
    }

    pub fn read(crypto_context: Context, path_basename: & Path)
                -> Result<SerializedNode, ()> {


        let (version, path) = find_serialized_file_version(
            path_basename,
            SerializedNode::current_version()
        )
            .map_err(|()| error!("Failed to find any version of serialized node settings"))
            ? ;

        debug!("Deserializing node settings from version={}, path={}", version, path.display());

        let decrypted = crypto_context.decrypt_from_file(& path) ? ;
        if version == 1 {
            let serialized = str::from_utf8(& decrypted)
                .map_err(log_utf_error)
                .and_then(
                    |utf| serde_json::from_str::<SerializedNode>(utf)
                        .map_err(log_serde_error)
                )
                ? ;
            return Ok(serialized);
        } else {
            error!("Unhandeled node settings version, version={}", version);
        }
        Err(())
    }
}

////////////////////////////////////////////////////////////////////

pub type SerializedFilesystemFile = (
    NodeId,
    PathBuf,
    FileType,
    FileRevision,
    u64,
);

pub type SerializedFilesystemFolder = (
    NodeId,
    NodeId,
    Timestamp, // created
    Timestamp, // modified
    Id, // read
    Id, // write
    Vec<(NodeId, String)>
    );


#[derive(Serialize, Deserialize)]
pub struct SerializedFilesystem {
    pub capacity: usize,
    pub max_number_of_files_per_directory: usize,
    pub files: Vec<SerializedFilesystemFile>,
    pub folders: Vec<SerializedFilesystemFolder>,
}

impl SerializedFilesystem {
    fn current_version() -> u32 {
        1
    }

    pub fn new(capacity: usize, max_number_of_files_per_directory: usize) -> SerializedFilesystem {
        SerializedFilesystem {
            capacity: capacity,
            max_number_of_files_per_directory: max_number_of_files_per_directory,
            files: Vec::new(),
            folders: Vec::new(),
        }
    }

    pub fn write(& self, crypto_context: Context, path_basename: & Path)
                        -> Result<(), ()> {

        let path = path_with_version(path_basename, SerializedFilesystem::current_version());

        debug!("Serializing filesystem to path={}", path.display());

        let serialized = serde_json::to_string(& self)
            .map_err(log_serde_error)
            ? ;

        crypto_context.encrypt_to_file(& serialized.into_bytes(), & path)
    }

    pub fn read(crypto_context: Context, path_basename: & Path)
                          -> Result<SerializedFilesystem, ()> {


        let (version, path) = find_serialized_file_version(
            path_basename,
            SerializedFilesystem::current_version()
        )
            .map_err(|()| error!("Failed to find any version of serialized filesystem"))
            ? ;

        debug!("Deserializing filesystem from version={}, path={}", version, path.display());

        let decrypted = crypto_context.decrypt_from_file(& path) ? ;
        if version == 1 {
            let serialized = str::from_utf8(& decrypted)
                .map_err(log_utf_error)
                .and_then(
                    |utf| serde_json::from_str::<SerializedFilesystem>(utf)
                        .map_err(log_serde_error)
                )
                ? ;
            return Ok(serialized);
        } else {
            error!("Unhandeled filesystem version, version={}", version);
        }
        Err(())
    }
}

////////////////////////////////////////////////////////////////////

pub type UserState = (u64, u64, String, Vec<u8>, Option<Timestamp>);
pub type GroupState = (u64, String, Vec<(u8, u64)>, Option<Timestamp>);

#[derive(Serialize, Deserialize)]
pub struct SerializedUserAuthority {
    next_user_id: u64,
    next_group_id: u64,
    users: Vec<UserState>,
    groups: Vec<GroupState>,
}

impl SerializedUserAuthority {
    pub fn current_version() -> u32 {
        1
    }

    pub fn new(next_user_id: u64, next_group_id: u64) -> SerializedUserAuthority {
        SerializedUserAuthority {
            next_user_id: next_user_id,
            next_group_id: next_group_id,
            users: Vec::new(),
            groups: Vec::new(),
        }
    }

    pub fn add_user(& mut self, id: u64, salt: u64, name: String, password: Vec<u8>, expiration: Option<i64>) {
        self.users.push((id, salt, name, password, expiration));
    }

    pub fn add_group(& mut self, id: u64, name: String, members: Vec<(u8, u64)>, expiration: Option<i64>) {
        self.groups.push((id, name, members, expiration));
    }

    pub fn users_iter(& self) -> Iter<UserState> {
        self.users.iter()
    }

    pub fn groups_iter(& self) -> Iter<GroupState> {
        self.groups.iter()
    }

    pub fn state(& self) -> (u64, u64) {
        (self.next_user_id, self.next_group_id)
    }

    pub fn write(& self, crypto_context: Context, path_basename: & Path)
                 -> Result<(), ()> {

        let path = path_with_version(path_basename, SerializedUserAuthority::current_version());

        debug!("Serializing user authority to path={}", path.display());

        let serialized = serde_json::to_string(self)
            .map_err(log_serde_error)
            ? ;

        crypto_context.encrypt_to_file(& serialized.into_bytes(), & path)
    }

    pub fn read(crypto_context: Context, path_base: & Path)
                -> Result<SerializedUserAuthority, ()> {

        let (version, path) = find_serialized_file_version(path_base, SerializedUserAuthority::current_version())
            .map_err(|()| error!("Failed to find any version of serialized user authority, path_base={}", path_base.display()))
            ? ;

        debug!("Deserializing user authority from version={}, path={}", version, path.display());

        let decrypted = crypto_context.decrypt_from_file(& path) ? ;
        if version == 1 {
            let serialized = str::from_utf8(& decrypted)
                .map_err(log_utf_error)
                .and_then(
                    |utf| serde_json::from_str::<SerializedUserAuthority>(utf)
                        .map_err(log_serde_error)
                )
                ? ;
            return Ok(serialized);
        } else {
            error!("Unhandeled user authority version, version={}", version);
        }
        Err(())
    }

}

////////////////////////////////////////////////////////////////////


pub type FileSegment = (u64, u64, u32);

#[derive(Serialize, Deserialize)]
pub struct SerializedMetadata {
    pub created: Timestamp,
    pub created_by: Id,
    pub modified: Timestamp,
    pub modified_by: Id,
    pub revision: FileRevision,
    pub segments: Vec<FileSegment>,
    pub parent: NodeId,
    pub file_type: FileType,
    pub max_block_size: usize,
}

impl SerializedMetadata {
    pub fn current_version() -> u32 {
        1
    }

    pub fn new(
        created: Timestamp,
        created_by: Id,
        modified: Timestamp,
        modified_by: Id,
        revision: FileRevision,
        parent: NodeId,
        file_type: FileType,
        max_block_size: usize
    ) -> SerializedMetadata {

        SerializedMetadata {
            created: created,
            created_by: created_by,
            modified: modified,
            modified_by: modified_by,
            revision: revision,
            segments: Vec::new(),
            parent: parent,
            file_type: file_type,
            max_block_size: max_block_size,
        }
    }

    pub fn add_file_segment(& mut self, offset: u64, size: u64, file_number: u32) {
        self.segments.push((offset, size, file_number));
    }

    pub fn write(& self, crypto_context: & Context, path_basename: & Path)
                 -> Result<(), ()> {

        let path = path_with_version(path_basename, SerializedMetadata::current_version());

        debug!("Serializing file to path={}", path.display());

        let serialized = serde_json::to_string(self)
            .map_err(log_serde_error)
            ? ;

        crypto_context.encrypt_to_file(& serialized.into_bytes(), & path)
    }

    pub fn read(crypto_context: & Context, path_basename: & Path)
                -> Result<SerializedMetadata, ()> {

        let (version, path) = find_serialized_file_version(path_basename, SerializedMetadata::current_version())
            .map_err(|()| error!("Failed to find any version of serialized file, path_base={}", path_basename.display()))
            ? ;

        debug!("Deserializing file from version={}, path={}", version, path.display());

        let decrypted = crypto_context.decrypt_from_file(& path) ? ;
        if version == 1 {
            let serialized = str::from_utf8(& decrypted)
                .map_err(log_utf_error)
                .and_then(
                    |utf| serde_json::from_str::<SerializedMetadata>(utf)
                        .map_err(log_serde_error)
                )
                ? ;
            return Ok(serialized);
        } else {
            error!("Unhandeled file version, version={}", version);
        }
        Err(())
    }

}

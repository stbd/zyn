use std::cmp::{ min };
use std::fmt::{ Display, Formatter, Result as FmtResult } ;
use std::fs::{ OpenOptions, File };
use std::io::{ Read, Write };
use std::option::{ Option };
use std::path::{ PathBuf, Path, Display as PathDisplay };
use std::ptr::{ copy };
use std::sync::mpsc::{ channel, Sender, Receiver };
use std::vec::{ Vec };

use node::common::{ utc_timestamp, FileRevision, Buffer, NodeId, FileType, Timestamp };
use node::crypto::{ Context };
use node::file_handle::{ FileError, FileLock, FileAccess, Notification, OpenFileProperties };
use node::serialize::{ SerializedMetadata };
use node::user_authority::{ Id };

pub enum FileRequestProtocol {
    Close,
    RequestMetadata,
    RequestAccess { user: Id },
    Write { revision: FileRevision, offset: u64, buffer: Buffer },
    Insert { revision: FileRevision, offset: u64, buffer: Buffer },
    Delete { revision: FileRevision, offset: u64, size: u64 },
    Read { offset: u64, size: u64 },
    LockFile { revision: FileRevision, description: FileLock },
    UnlockFile { description: FileLock },
}

pub enum FileResponseProtocol {
    Access { access: FileAccess },
    Metadata { metadata: Metadata, open_file_properties: OpenFileProperties },
    Write { result: Result<FileRevision, FileError> },
    Insert { result: Result<FileRevision, FileError> },
    Delete { result: Result<FileRevision, FileError> },
    Read { result: Result<(Buffer, FileRevision), FileError> },
    LockFile { result: Result<(), FileError> },
    UnlockFile { result: Result<(), FileError> },
    Notification { notification: Notification },
}

struct ConnectedAccess {
    send_user: Sender<FileResponseProtocol>,
    receive_file: Receiver<FileRequestProtocol>,
    user: Option<Id>,
}

impl Display for ConnectedAccess {
    fn fmt(& self, f: & mut Formatter) -> FmtResult {
        if let Some(ref user) = self.user {
            write!(f, "{}", user)
        } else {
            write!(f, "root-handle")
        }
    }
}

#[derive(Clone)]
pub struct FileBlock {
    pub offset: u64,
    pub size: u64,
    pub block_number: u32,
}

#[derive(Clone, PartialEq)]
pub struct FileEvent {
    pub user: Id,
    pub timestamp: Timestamp,
}

#[derive(Clone)]
pub struct Metadata {
    pub created: FileEvent,
    pub modified: FileEvent,
    pub revision: FileRevision,
    pub read: Id, // todo: remove
    pub write: Id, // todo: remove
    pub parent: NodeId,
    pub max_block_size: usize,
    pub file_type: FileType,
    pub block_descriptions: Vec<FileBlock>,
}

impl Metadata {
    pub fn new_file(
        user: Id,
        parent: NodeId,
        file_type: FileType,
        max_block_size: usize
    ) -> Metadata {

        let timestamp = utc_timestamp();
        let mut metadata = Metadata {
            created: FileEvent {
                user: user.clone(),
                timestamp: timestamp.clone(),
            },
            modified: FileEvent {
                user: user.clone(),
                timestamp: timestamp,
            },
            revision: 0,
            read: user.clone(),
            write: user,
            parent: parent,
            file_type: file_type,
            max_block_size: max_block_size,
            block_descriptions: Vec::new(),
        };
        metadata.add_block();
        metadata
    }

    pub fn size(& self) -> u64 {
        self.block_descriptions.iter().fold(0, | acc, ref element | acc + element.size)
    }

    pub fn add_block(& mut self) -> u32 {

        let block_number = self.block_descriptions.len() as u32;
        self.block_descriptions.push(FileBlock {
            offset: block_number as u64 * self.max_block_size as u64,
            size: 0,
            block_number: block_number,
        });
        block_number
    }

    pub fn is_in_block(& self, block_number: u32, offset: u64, size: u64) -> bool {
        Metadata::_is_in_block(
            & self.block_descriptions[block_number as usize],
            offset,
            size,
            self.max_block_size as u64
        )
    }

    pub fn _is_in_block(block: & FileBlock, offset: u64, size: u64, max_block_size: u64)
                       -> bool {

        if offset >= block.offset && offset < (block.offset + max_block_size) {
            let end_of_modification = offset + size;
            if end_of_modification <= (block.offset + max_block_size) {
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    pub fn find_block(& self, offset: u64, size: u64) -> Result<u32, ()> {

        for ref desc in self.block_descriptions.iter() {
            if Metadata::_is_in_block(desc, offset, size, self.max_block_size as u64) {
                return Ok(desc.block_number);
            }
        }

        Err(())
    }

    pub fn find_or_allocate_block(& mut self, offset: u64, size: u64) -> Result<u32, ()> {

        let result = self.find_block(offset, size);
        if result.is_ok() {
            return result;
        }

        let last_offset = self.block_descriptions.last().unwrap().offset;
        if offset > (last_offset + self.max_block_size as u64 - 1)
            && offset < (last_offset + self.max_block_size as u64 * 2)
        {
            let block_number = self.add_block();
            return Ok(block_number);
        }
        Err(())
    }

    pub fn path(path_basename: & Path) -> PathBuf {
        path_basename.with_extension("metadata")
    }

    pub fn store(& self, path_basename: & Path, crypto_context: & Context) -> Result<(), ()> {

        let mut serialized = SerializedMetadata::new(
            self.created.timestamp,
            self.created.user.clone(),
            self.modified.timestamp,
            self.modified.user.clone(),
            self.revision,
            self.read.clone(),
            self.write.clone(),
            self.parent.clone(),
            self.file_type.clone(),
            self.max_block_size,
        );

        for ref desc in self.block_descriptions.iter() {
            serialized.segments.push((desc.offset, desc.size, desc.block_number));
        }

        serialized.write(& crypto_context, & Metadata::path(& path_basename))
            .map_err(| () | error!("Failed to write file metadata"))
    }

    pub fn load(path_basename: & PathBuf, crypto_context: & Context) -> Result<Metadata, ()> {

        let path_metadata = Metadata::path(& path_basename);
        let serialized = SerializedMetadata::read(& crypto_context, & path_metadata)
            .map_err(| () | error!("Failed to load metadata"))
            ? ;

        let mut metadata = Metadata {
            created: FileEvent {
                user: serialized.created_by,
                timestamp: serialized.created,
            },
            modified: FileEvent {
                user: serialized.modified_by,
                timestamp: serialized.modified,
            },
            revision: serialized.revision,
            block_descriptions: Vec::new(),
            read: serialized.read,
            write: serialized.write,
            parent: serialized.parent,
            file_type: serialized.file_type,
            max_block_size: serialized.max_block_size,
        };

        for (ref offset, ref size, ref block_number) in serialized.segments {
            metadata.block_descriptions.push(FileBlock {
                offset: offset.clone(),
                size: size.clone(),
                block_number: block_number.clone(),
            });
        }

        Ok(metadata)
    }
}

impl ConnectedAccess {
    fn is_root_handle(& self) -> bool {
        self.user.is_none()
    }
}

pub struct FileService {
    file: FileImpl,
    users: Vec<ConnectedAccess>,
}

fn send_response(user: & ConnectedAccess, response: FileResponseProtocol) -> Result<(), ()> {
    user.send_user.send(response)
        .map_err(| desc | error!("Failed to send response to user, desc=\"{}\", user=\"{}\"", desc, user))
}

impl FileService {
    pub fn create(
        path_basename: & PathBuf,
        crypto_context: Context,
        user: Id,
        parent: NodeId,
        file_type: FileType,
        max_block_size: usize,
    ) -> Result<(), ()> {
        FileImpl::create(& path_basename, crypto_context, user, parent, file_type, max_block_size)
    }

    pub fn open(path_basename: & PathBuf, crypto_context: Context, metadata: Metadata)
                -> Result<FileService, ()> {
        let file = FileImpl::open(path_basename, crypto_context, metadata) ? ;
        Ok(FileService {
            file: file,
            users: Vec::new(),
        })
    }

    pub fn create_access(& mut self, user: Option<Id>) -> FileAccess {
        let (tx_user, rx_user) = channel::<FileResponseProtocol>();
        let (tx_file, rx_file) = channel::<FileRequestProtocol>();

        self.users.push(ConnectedAccess {
            send_user: tx_user,
            receive_file: rx_file,
            user: user,
        });

        debug!("New access added, user={}, path={}",
               self.users.last().unwrap(), self.file.display());

        FileAccess {
            channel_send: tx_file,
            channel_receive: rx_user,
            unhandled_notitifications: Vec::with_capacity(5),
        }
    }

    pub fn process(& mut self) {
        let mut notifications: Vec<(
            bool, // Do not send notification to file handle
            Option<usize>, // Do not send notification to index
            Notification // Actual notification
        )> = Vec::with_capacity(5);

        loop {
            let mut exit: bool = false;
            let mut add_access: Option<(usize, Id)> = None;
            let mut remove_access: Option<usize> = None;

            notifications.truncate(0);

            let current_active_users_ids: Vec<Id> = self.users
                .iter()
                .filter(| user | ! user.is_root_handle() )
                .map(| user | user.user.as_ref().unwrap().clone() )
                .collect()
                ;

            for (i, user) in self.users.iter_mut().enumerate() {
                if let Ok(message) = user.receive_file.try_recv() {

                    let mut send_ok = true;
                    match message {

                        FileRequestProtocol::RequestAccess { user } => {
                            add_access = Some((i, user));
                        },

                        FileRequestProtocol::RequestMetadata => {
                            let lock = self.file.get_lock();
                            send_ok = send_response(& user, FileResponseProtocol::Metadata {
                                metadata: self.file.metadata.clone(),
                                open_file_properties: OpenFileProperties {
                                    active_users: current_active_users_ids.clone(),
                                    lock: lock,
                                },
                            }).is_ok();
                        },

                        FileRequestProtocol::Close => {
                            if user.is_root_handle() {
                                notifications.push(
                                    (true, None, Notification::FileClosing { })
                                );
                                exit = true;
                                break;
                            } else {
                                remove_access = Some(i);
                            }
                        },

                        FileRequestProtocol::Write { revision, offset, buffer } => {
                            let size = buffer.len() as u64;
                            match self.file.write(& user.user, revision, offset, buffer) {
                                Ok(revision) => {
                                    notifications.push(
                                        (true,
                                         Some(i),
                                         Notification::PartOfFileModified {
                                             revision: revision,
                                             offset: offset,
                                             size: size
                                         })
                                    );
                                    send_ok = send_response(& user, FileResponseProtocol::Write {
                                        result: Ok(revision),
                                    }).is_ok();
                                },
                                Err(status) => {
                                    send_ok = send_response(& user, FileResponseProtocol::Write {
                                        result: Err(status),
                                    }).is_ok();
                                }
                            };
                        },

                        FileRequestProtocol::Insert { revision, offset, buffer } => {
                            let size = buffer.len() as u64;
                            match self.file.insert(& user.user, revision, offset, buffer) {
                                Ok(revision) => {
                                    notifications.push(
                                        (true,
                                         Some(i),
                                         Notification::PartOfFileInserted {
                                             revision: revision,
                                             offset: offset,
                                             size: size
                                         })
                                    );
                                    send_ok = send_response(& user, FileResponseProtocol::Insert {
                                        result: Ok(revision),
                                    }).is_ok();
                                },
                                Err(status) => {
                                    send_ok = send_response(& user, FileResponseProtocol::Insert {
                                        result: Err(status),
                                    }).is_ok();
                                }
                            };
                        },

                        FileRequestProtocol::Delete { revision, offset, size } => {
                            match self.file.delete(& user.user, revision, offset, size) {
                                Ok(revision) => {
                                    notifications.push(
                                        (true,
                                         Some(i),
                                         Notification::PartOfFileDeleted {
                                             revision: revision,
                                             offset: offset,
                                             size: size
                                         })
                                    );
                                    send_ok = send_response(& user, FileResponseProtocol::Delete {
                                        result: Ok(revision),
                                    }).is_ok();
                                },
                                Err(status) => {
                                    send_ok = send_response(& user, FileResponseProtocol::Delete {
                                        result: Err(status),
                                    }).is_ok();
                                }
                            };
                        },

                        FileRequestProtocol::Read { offset, size } => {
                            match self.file.read(offset, size) {
                                Ok((buffer, revision)) => {
                                    send_ok = send_response(& user, FileResponseProtocol::Read {
                                        result: Ok((buffer, revision)),
                                    }).is_ok();
                                },
                                Err(status) => {
                                    send_ok = send_response(& user, FileResponseProtocol::Read {
                                        result: Err(status),
                                    }).is_ok();
                                }
                            };
                        },

                        FileRequestProtocol::LockFile { revision, description } => {
                            match self.file.lock(revision, description) {
                                Ok(()) => {
                                    send_ok = send_response(& user, FileResponseProtocol::LockFile {
                                        result: Ok(()),
                                    }).is_ok();
                                },
                                Err(status) => {
                                    send_ok = send_response(& user, FileResponseProtocol::LockFile {
                                        result: Err(status),
                                    }).is_ok();
                                }
                            };
                        },

                        FileRequestProtocol::UnlockFile { description } => {
                            match self.file.unlock(description) {
                                Ok(()) => {
                                    send_ok = send_response(& user, FileResponseProtocol::UnlockFile {
                                        result: Ok(()),
                                    }).is_ok();
                                },
                                Err(status) => {
                                    send_ok = send_response(& user, FileResponseProtocol::UnlockFile {
                                        result: Err(status),
                                    }).is_ok();
                                }
                            };
                        },
                    }

                    if ! send_ok {
                        debug!("Removing client due send error, index={}", i);
                        remove_access = Some(i);
                        break;
                    }
                }
            }

            if let Some(index) = remove_access {
                debug!("Removing access, user={}, file={}",
                       self.users[index], self.file.display());
                self.users.remove(index);
            }

            if let Some((index, user)) = add_access {
                let access = self.create_access(Some(user));
                if send_response(
                    & self.users[index],
                    FileResponseProtocol::Access { access: access }
                ).is_err() {
                    self.users.remove(index);
                }
            }

            if self.users.len() <= 1 {
                debug!("Closing file as it has no users, file={}", self.file.display());
                let _ = send_response(& self.users[0], FileResponseProtocol::Notification {
                    notification: Notification::FileClosing { }
                });
                exit = true;
            }

            if ! notifications.is_empty() {
                for (i, user) in self.users.iter().enumerate() {
                    for & (ref skip_handle, ref source_index, ref notification) in notifications.iter() {
                        if *skip_handle && user.is_root_handle() {
                            continue;
                        }

                        let mut send = true;
                        if let & Some(ref skip_index) = source_index {
                            if i == *skip_index {
                                send = false;
                            }
                        }
                        if send {
                            let _ = send_response(& user, FileResponseProtocol::Notification {
                                notification: notification.clone()
                            });
                        }
                    }
                }
            }

            if exit {
                self.file.store();
                break;
            }
        }
    }
}

pub struct FileImpl {
    metadata: Metadata,
    path_basename: PathBuf,
    buffer: Buffer,
    current_block_index: u32,
    crypto_context: Context,
    lock: Option<FileLock>,
}

static DEFAULT_BUFFER_SIZE_BYTES: u64 = 1024 * 1;

impl FileImpl {

    pub fn display(& self) -> PathDisplay {
        self.path_basename.display()
    }

    pub fn exists(path_basename: & Path) -> bool {
        let path = FileImpl::path_data(path_basename, 0);
        path.exists()
    }

    pub fn path_data(path_basename: & Path, block_number: u32) -> PathBuf {
        path_basename.with_extension(format!("block-{}", block_number))
    }

    pub fn create(
        path_basename: & PathBuf,
        crypto_context: Context,
        user: Id,
        parent: NodeId,
        file_type: FileType,
        max_block_size: usize,
    ) -> Result<(), ()> {

        let mut file = FileImpl {
            buffer: Buffer::with_capacity(DEFAULT_BUFFER_SIZE_BYTES as usize),
            current_block_index: 0,
            crypto_context: crypto_context,
            metadata: Metadata::new_file(user, parent, file_type, max_block_size),
            path_basename: path_basename.clone(),
            lock: None,
        };
        file.store();
        Ok(())
    }

    fn get_lock(& self) -> Option<FileLock> {
        self.lock.clone()
    }

    pub fn open(path_basename: & PathBuf, crypto_context: Context, metadata: Metadata)
                -> Result<FileImpl, ()> {

        let mut file = FileImpl {
            buffer: Buffer::with_capacity(DEFAULT_BUFFER_SIZE_BYTES as usize),
            current_block_index: 0,
            crypto_context: crypto_context,
            metadata: metadata,
            path_basename: path_basename.clone(),
            lock: None,
        };

        if file.metadata.block_descriptions.len() == 0 {
            error!("File has not data file descriptions, path_basename={}",
                   path_basename.display());
            return Err(());
        }

        file.load_block(0) ? ;
        Ok(file)
    }

    fn load_block(& mut self, block_index: u32) -> Result<(), ()> {

        trace!("Loading block: block_index={}", block_index);

        let ref block = self.metadata.block_descriptions[block_index as usize];
        let path = FileImpl::path_data(& self.path_basename, block.block_number);


        if path.exists() {
            let mut file = match OpenOptions::new().read(true).create(false).open(path.as_path()) {
                Ok(file) => file,
                Err(error_code) => {
                    error!("Failed to open file for reading: path={}, error_code=\"{}\"", path.display(), error_code);
                    return Err(());
                },
            };

            let mut data = Buffer::new();
            match file.read_to_end(& mut data) {
                Err(_) => {
                    warn!("Failed to write data, path_basename={}", self.path_basename.display());
                },
                _ => (),
            };
            if data.len() > 0 {
                self.buffer = self.crypto_context.decrypt(& data) ? ;
            }
        } else {
            trace!("Creating new page, path_basename={}, block_index={}", self.path_basename.display(), block_index);
            match OpenOptions::new().read(true).write(true).create(true).open(path.as_path()) {
                Ok(_) => (),
                Err(error_code) => {
                    error!("Failed to create data file: path={}, error_code=\"{}\"", path.display(), error_code);
                    return Err(());
                },
            };
            self.buffer = Buffer::with_capacity(DEFAULT_BUFFER_SIZE_BYTES as usize);
        }

        self.current_block_index = block_index;
        Ok(())
    }

    fn write_block(& mut self) -> Result<(), ()> {

        let path = FileImpl::path_data(& self.path_basename, self.current_block_index);
        let mut file = File::create(path.as_path())
            .map_err(| error | error!("Failed to open file for writing: path={}, error={}",
                                      path.display(), error))
            ? ;

        let encrypted = self.crypto_context.encrypt(& self.buffer) ? ;

        match file.write_all(& encrypted) {
            Err(_error_code) => {
                warn!("Failed to write data, path_basename={}", self.path_basename.display());
                Err(())
            },
            _ => {
                let ref mut desc = self.metadata.block_descriptions[self.current_block_index as usize];
                desc.size = self.buffer.len() as u64;
                Ok(())
            },
        }
    }

    fn swap_block(& mut self, block_index: u32) -> Result<(), ()> {
        debug!("Swapping current buffer, current_block_index={}, block_index={}, file={}",
               self.current_block_index, block_index, self.display());

        self.write_block() ? ;
        self.load_block(block_index) ? ;

        debug!("Swap completed, file={}, buffer_len={}", self.display(), self.buffer.len());
        Ok(())
    }

    fn store(& mut self) {
        let _ = self.write_block();
        let _ = self.metadata.store(& self.path_basename, & self.crypto_context);
    }

    fn update_current_block_size(& mut self) {
        let ref mut desc = self.metadata.block_descriptions[self.current_block_index as usize];
        desc.size = self.buffer.len() as u64;
    }

    fn lock(& mut self, revision: FileRevision, desc: FileLock) -> Result<(), FileError> {

        if revision != self.metadata.revision {
            return Err(FileError::RevisionTooOld);
        }

        // todo: check if current lock is the same and by same user
        if let Some(_) = self.lock {
            return Err(FileError::FileLockedByOtherUser);
        }

        self.lock = Some(desc);

        Ok(())
    }

    fn unlock(& mut self, desc: FileLock) -> Result<(), FileError> {

        if self.lock.is_none() {
            return Err(FileError::FileNotLocked);
        }

        let mut release_lock = false;
        if let Some(ref lock) = self.lock {
            if lock.is_locked_by(& desc) {
                release_lock = true;
            } else {
                return Err(FileError::FileLockedByOtherUser);
            }
        }

        if release_lock {
            self.lock = None;
        }

        Ok(())
    }

    fn is_edit_allowed(& self, user: & Option<Id>) -> Result<(), FileError> {

        if user.is_none() {
            error!("File edit when user is none");
            return Err(FileError::InternalError);
        }

        let user_id = user.as_ref().unwrap();

        match self.lock {
            None => Ok(()),
            Some(FileLock::LockedBySystemForBlobWrite { ref user }) => {
                if user_id != user {
                    return Err(FileError::FileLockedByOtherUser);
                }
                Ok(())
            },
        }
    }

    fn write(& mut self, user: & Option<Id>, revision: FileRevision, offset: u64, buffer: Buffer)
             -> Result<FileRevision, FileError> {

        if ! self.metadata.is_in_block(self.current_block_index, offset, buffer.len() as u64) {
            let block_number = self.metadata.find_or_allocate_block(offset, buffer.len() as u64)
                .map_err(| () | FileError::OffsetAndSizeDoNotMapToPartOfFile)
                ? ;

            self.swap_block(block_number)
                .map_err(| () | FileError::InternalError)
                ? ;
        }

        if revision != self.metadata.revision {
            return Err(FileError::RevisionTooOld);
        }

        self.is_edit_allowed(user) ? ;

        let min_buffer_size = offset as usize + buffer.len();
        if self.buffer.len() < min_buffer_size {
            self.buffer.resize(min_buffer_size, 0);
        }

        let p = self.buffer.as_mut_ptr();
        unsafe {
            copy(buffer.as_ptr(), p.offset(offset as isize), buffer.len());
        }

        self.metadata.revision += 1;
        self.update_current_block_size();
        Ok(self.metadata.revision)
    }

    fn insert(& mut self, user: & Option<Id>, revision: FileRevision, offset: u64, buffer: Buffer)
              -> Result<FileRevision, FileError> {

        if ! self.metadata.is_in_block(self.current_block_index, offset, buffer.len() as u64) {
            let block_number = self.metadata.find_or_allocate_block(offset, buffer.len() as u64)
                .map_err(| () | FileError::OffsetAndSizeDoNotMapToPartOfFile)
                ? ;

            self.swap_block(block_number)
                .map_err(| () | FileError::InternalError)
                ? ;
        }

        if revision != self.metadata.revision {
            return Err(FileError::RevisionTooOld);
        }

        self.is_edit_allowed(user) ? ;

        let min_buffer_size = self.buffer.len() + buffer.len();
        if self.buffer.len() < min_buffer_size {
            self.buffer.resize(min_buffer_size, 0);
        }

        let p = self.buffer.as_mut_ptr();
        unsafe {
            copy(
                p.offset(offset as isize),
                p.offset(offset as isize + buffer.len() as isize),
                self.buffer.len() - buffer.len() - offset as usize
            );
            copy(buffer.as_ptr(), p.offset(offset as isize), buffer.len());
        }

        self.metadata.revision += 1;
        self.update_current_block_size();
        Ok(self.metadata.revision)
    }

    pub fn read(& mut self, offset: u64, size: u64)
                -> Result<(Buffer, FileRevision), FileError> {
        if ! self.metadata.is_in_block(self.current_block_index, offset, size) {
            let file_number = self.metadata.find_block(offset, size)
                .map_err(| () | FileError::OffsetAndSizeDoNotMapToPartOfFile)
                ? ;

            self.swap_block(file_number)
                .map_err(| () | FileError::InternalError)
                ? ;
        }

        let end_offset = offset + size;
        let end_offset = min(end_offset, self.buffer.len() as u64);
        let fixed_sized_to_read = end_offset - offset;
        let mut buffer: Buffer = vec![0; fixed_sized_to_read as usize];
        let p = buffer.as_mut_ptr();

        unsafe {
            copy(self.buffer.as_ptr().offset(offset as isize), p, buffer.len());
        }

        Ok((buffer, self.metadata.revision))
    }

    fn delete(& mut self, user: & Option<Id>, revision: FileRevision, offset: u64, size: u64)
              -> Result<FileRevision, FileError> {

        if ! self.metadata.is_in_block(self.current_block_index, offset, size) {

            let block_number = self.metadata.find_block(offset, size)
                .map_err(| () | FileError::OffsetAndSizeDoNotMapToPartOfFile)
                ? ;

            if self.current_block_index != (self.metadata.block_descriptions.len() as u32 - 1) {
                return Err(FileError::DeleteIsonlyAllowedForLastPart);
            }

            self.swap_block(block_number)
                .map_err(| () | FileError::InternalError)
                ? ;
        } else {
            if self.current_block_index != (self.metadata.block_descriptions.len() as u32 - 1) {
                return Err(FileError::DeleteIsonlyAllowedForLastPart);
            }
        }

        if revision != self.metadata.revision {
            return Err(FileError::RevisionTooOld);
        }

        self.is_edit_allowed(user) ? ;

        let end_offset = offset + size;
        let end_offset = min(end_offset, self.buffer.len() as u64);
        let _: Buffer = self.buffer.drain(offset as usize .. end_offset as usize).collect();

        self.metadata.revision += 1;
        self.update_current_block_size();
        Ok(self.metadata.revision)
    }
}

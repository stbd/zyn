use std::option::{ Option };
use std::path::{ PathBuf, Path };
use std::sync::mpsc::{ Sender, Receiver, TryRecvError };
use std::thread::{ sleep };
use std::thread::{ spawn, JoinHandle };
use std::time::{ Duration };
use std::vec::{ Vec };

use node::common::{ log_crypto_context_error, FileRevision, Buffer, NodeId, FileType, Timestamp };
use node::crypto::{ Crypto, Context };
use node::file::{ FileService, FileImpl, Metadata, FileResponseProtocol, FileRequestProtocol };
use node::user_authority::{ Id };

#[derive(Debug)]
pub enum FileError {
    InternalCommunicationError,
    InternalError,
    RevisionTooOld,
    OffsetAndSizeDoNotMapToPartOfFile,
    DeleteIsonlyAllowedForLastPart,
    FileLockedByOtherUser,
    FileNotLocked,
}

#[derive(Clone, Debug)]
pub enum Notification {
    FileClosing {  }, // todo: Add reason
    PartOfFileModified { revision: FileRevision, offset: u64, size: u64 },
    PartOfFileInserted { revision: FileRevision, offset: u64, size: u64 },
    PartOfFileDeleted { revision: FileRevision, offset: u64, size: u64 },
}

pub struct OpenFileProperties {
    pub active_users: Vec<Id>,
    pub lock: Option<FileLock>,
}

pub struct FileProperties {
    pub created_at: Timestamp,
    pub created_by: Id,
    pub modified_at: Timestamp,
    pub modified_by: Id,
    pub revision: FileRevision,
    pub size: u64,
    pub parent: NodeId,
    pub file_type: FileType,
    pub page_size: u64,
    pub open_file_properties: Option<OpenFileProperties>,
}

impl FileProperties {
    fn create(metadata: Metadata, open_file_properties: Option<OpenFileProperties>) -> FileProperties {
        let size = metadata.size();
        FileProperties {
            created_at: metadata.created.timestamp,
            created_by: metadata.created.user,
            modified_at: metadata.modified.timestamp,
            modified_by: metadata.modified.user,
            revision: metadata.revision,
            size: size,
            parent: metadata.parent,
            file_type: metadata.file_type,
            page_size: metadata.max_block_size as u64,
            open_file_properties: open_file_properties,
        }
    }

    fn from_open_file(metadata: Metadata, open_file_properties: OpenFileProperties) -> FileProperties {
        FileProperties::create(metadata, Some(open_file_properties))
    }

    fn from_closed_file(metadata: Metadata) -> FileProperties {
        FileProperties::create(metadata, None)
    }
}

#[derive(Clone, PartialEq)]
pub enum FileLock {
    LockedBySystemForBlobWrite { user: Id },
}

impl FileLock {
    pub fn is_locked_by(& self, lock: & FileLock) -> bool {
        *self == *lock
    }
}

struct FileServiceHandle {
    thread: JoinHandle<()>,
    access: FileAccess,
}

pub struct FileHandle {
    path: PathBuf,
    file_service: Option<FileServiceHandle>,
}

impl FileHandle {
    pub fn state(& self) -> (PathBuf) {
        (self.path.clone())
    }

    pub fn path(& self) -> & Path {
        & self.path
    }

    pub fn create(
        path: PathBuf,
        crypto: & Crypto,
        user: Id,
        parent: NodeId,
        file_type: FileType,
        page_size: usize
    ) -> Result<FileHandle, ()> {

        let context = crypto.create_context()
            .map_err(| () | log_crypto_context_error())
            ? ;

        FileService::create(& path, context, user, parent, file_type, page_size) ? ;

        Ok(FileHandle{
            path: path,
            file_service: None,
        })
    }

    pub fn init(path: PathBuf) -> Result<FileHandle, ()> {
        if ! FileImpl::exists(& path) {
            error!("Physical file does not exist, path=\"{}\"", path.display());
            return Err(());
        }

        Ok(FileHandle{
            path: path,
            file_service: None,
        })
    }

    pub fn properties(& mut self, crypto: & Crypto) -> Result<FileProperties, ()> {

        self.update();

        if let Some(ref mut file_service) = self.file_service {
            let properties = file_service.access.properties()
                .map_err(| _ | error!("Failed to get metadata from file service"))
                ? ;

            Ok(properties)
        } else {
            let context = crypto.create_context()
                .map_err(| () | log_crypto_context_error())
                ? ;

            let metadata = Metadata::load(& self.path, & context) ? ;
            Ok(FileProperties::from_closed_file(metadata))
        }
    }

    pub fn open(& mut self, crypto: & Crypto, user: Id) -> Result<FileAccess, ()> {

        if let Some(ref mut file_service) = self.file_service {
            debug!("Opening file, service already running: path={}", self.path.display());
            file_service.access.channel_send.send(FileRequestProtocol::RequestAccess {
                user: user
            }).unwrap();

            match file_service.access.channel_receive.recv() {
                Ok(FileResponseProtocol::Access { access }) => return Ok(access),
                Ok(_) => return Err(()),
                Err(_) => return Err(()),
            }

        } else {
            let context = crypto.create_context()
                .map_err(| () | log_crypto_context_error())
                ? ;

            debug!("Opening file, starting service: path={}", self.path.display());
            let access = self.start_file_service(context, user) ? ;
            Ok(access)
        }
    }

    pub fn is_open(& mut self) -> bool {
        self.update();
        self.file_service.is_some()
    }

    pub fn close(& mut self) {
        if !self.is_open() {
            return ;
        }

        let file_service = self.file_service.take().unwrap();
        let _ = file_service.access.channel_send.send(FileRequestProtocol::Close);
        let _ = file_service.thread.join()  // todo: Somekind of timeout should be used here
            .map_err(| error | warn!("Failed to join file thread, error={:?}", error))
            ;
    }

    fn start_file_service(& mut self, crypto_context: Context, user: Id)
                         -> Result<FileAccess, ()>
    {
        let metadata = Metadata::load(& self.path, & crypto_context) ? ;
        let mut file = FileService::open(& self.path, crypto_context, metadata) ? ;

        let access_1 = file.create_access(None);
        let access_2 = file.create_access(Some(user));

        let handle = spawn( move || {
            file.process();
        });

        self.file_service = Some(FileServiceHandle {
            thread: handle,
            access: access_1,
        });

        Ok(access_2)
    }

    fn update(& mut self) {

        let mut close = false;
        if let Some(ref mut handle) = self.file_service {
            loop {
                let notification = handle.access.pop_notification();
                match notification {
                    Some(Notification::FileClosing {  }) => close = true,
                    Some(Notification::PartOfFileModified { .. }) => (),
                    Some(Notification::PartOfFileInserted { .. }) => (),
                    Some(Notification::PartOfFileDeleted { .. }) => (),
                    None => break,
                };
            }
        }

        if close {
            self.close();
        }
    }
}

impl Drop for FileHandle {
    fn drop(& mut self) {
        self.close();
    }
}

#[derive(Debug)]
pub struct FileAccess {
    pub channel_send: Sender<FileRequestProtocol>,
    pub channel_receive: Receiver<FileResponseProtocol>,
    pub unhandled_notitifications: Vec<Notification>,
}

impl FileAccess {
    pub fn has_notifications(& mut self) -> bool {
        ! self.unhandled_notitifications.is_empty()
    }

    pub fn pop_notification(& mut self) -> Option<Notification> {
        if self.unhandled_notitifications.is_empty() {
            if let Ok(msg) = self.channel_receive.try_recv() {
                self.handle_unexpected_message(msg);
            }
        }
        self.unhandled_notitifications.pop()
    }

    pub fn write(& mut self, revision: u64, offset: u64, buffer: Buffer)
                 -> Result<FileRevision, FileError> {

        self.channel_send.send(FileRequestProtocol::Write {
            revision: revision,
            offset: offset,
            buffer: buffer,
        }).map_err(| _ | FileError::InternalCommunicationError) ? ;

        file_receive::<FileRevision>(self, & | msg | {
            match msg {
                FileResponseProtocol::Write {
                    result: Ok(revision),
                } => (None, Some(Ok(revision))),
                FileResponseProtocol::Write {
                    result: Err(status),
                } => return (None, Some(Err(status))),
                other => (Some(other), None),
            }
        })
    }

    pub fn insert(& mut self, revision: u64, offset: u64, buffer: Buffer)
                  -> Result<FileRevision, FileError> {

        self.channel_send.send(FileRequestProtocol::Insert {
            revision: revision,
            offset: offset,
            buffer: buffer,
        }).map_err(| _ | FileError::InternalCommunicationError) ? ;

        file_receive::<FileRevision>(self, & | msg | {
            match msg {
                FileResponseProtocol::Insert {
                    result: Ok(revision),
                } => (None, Some(Ok(revision))),
                FileResponseProtocol::Insert {
                    result: Err(status),
                } => return (None, Some(Err(status))),
                other => (Some(other), None),
            }
        })
    }

    pub fn delete(& mut self, revision: u64, offset: u64, size: u64)
                 -> Result<FileRevision, FileError> {

        self.channel_send.send(FileRequestProtocol::Delete {
            revision: revision,
            offset: offset,
            size: size,
        }).map_err(| _ | FileError::InternalCommunicationError) ? ;

        file_receive::<FileRevision>(self, & | msg | {
            match msg {
                FileResponseProtocol::Delete {
                    result: Ok(revision),
                } => (None, Some(Ok(revision))),
                FileResponseProtocol::Delete {
                    result: Err(status),
                } => return (None, Some(Err(status))),
                other => (Some(other), None),
            }
        })
    }

    pub fn read(& mut self, offset: u64, size: u64)
                -> Result<(Buffer, FileRevision), FileError> {

        self.channel_send.send(FileRequestProtocol::Read {
            offset: offset,
            size: size,
        }).map_err(| _ | FileError::InternalCommunicationError) ? ;

        file_receive::<(Buffer, FileRevision)>(self, & | msg | {
            match msg {
                FileResponseProtocol::Read {
                    result: Ok((buffer, revision)),
                } => (None, Some(Ok((buffer, revision)))),
                FileResponseProtocol::Read {
                    result: Err(status),
                } => (None, Some(Err(status))),
                other => (Some(other), None),
            }
        })
    }

    pub fn lock(& mut self, revision: FileRevision, lock: & FileLock) -> Result<(), FileError> {

        self.channel_send.send(FileRequestProtocol::LockFile {
            revision: revision,
            description: lock.clone()
        }).map_err(| _ | FileError::InternalCommunicationError) ? ;

        file_receive::<()>(self, & | msg | {
            match msg {
                FileResponseProtocol::LockFile {
                    result: Ok(()),
                } => (None, Some(Ok(()))),
                FileResponseProtocol::LockFile {
                    result: Err(status),
                } => (None, Some(Err(status))),
                other => (Some(other), None),
            }
        })
    }

    pub fn unlock(& mut self, lock: & FileLock) -> Result<(), FileError> {

        self.channel_send.send(FileRequestProtocol::UnlockFile {
            description: lock.clone(),
        }).map_err(| _ | FileError::InternalCommunicationError) ? ;

        file_receive::<()>(self, & | msg | {
            match msg {
                FileResponseProtocol::UnlockFile {
                    result: Ok(()),
                } => (None, Some(Ok(()))),
                FileResponseProtocol::UnlockFile {
                    result: Err(status),
                } => (None, Some(Err(status))),
                other => (Some(other), None),
            }
        })
    }

    pub fn properties(& mut self) -> Result<FileProperties, FileError> {
        self.channel_send.send(FileRequestProtocol::RequestMetadata { })
            .map_err(| _ | FileError::InternalCommunicationError) ? ;

        let (metadata, open_file_properties) = file_receive::<(Metadata, OpenFileProperties)>(self, & | msg | {
            match msg {
                FileResponseProtocol::Metadata{ metadata, open_file_properties } =>
                    (None, Some(Ok((metadata, open_file_properties)))),
                other => (Some(other), None),
            }
        }) ? ;

        Ok(FileProperties::from_open_file(metadata, open_file_properties))
    }

    pub fn close(& mut self) -> Result<(), FileError> {
        self.channel_send.send(FileRequestProtocol::Close { })
            .map_err(| _ | FileError::InternalCommunicationError) ? ;
        Ok(())
    }

    fn handle_unexpected_message(& mut self, msg: FileResponseProtocol) {
        match msg {
            FileResponseProtocol::Notification { notification } => {
                self.unhandled_notitifications.push(notification);
            },
            _other => {
                warn!("Received unexpedted message");
            },
        }
    }
}

static MAX_WAIT_DURATION_PER_MESSAGE_MS: u64 = 500;
static MAX_NUMBER_OF_ITERATIONS_PER_MESSAGE: u64 = 5;

fn file_receive<OkType>(
    access: & mut FileAccess,
    handler: & Fn(FileResponseProtocol) -> (Option<FileResponseProtocol>, Option<Result<OkType, FileError>>)
)
    -> Result<OkType, FileError> {

    let sleep_duration = MAX_WAIT_DURATION_PER_MESSAGE_MS / MAX_NUMBER_OF_ITERATIONS_PER_MESSAGE;
    for _ in 0..MAX_NUMBER_OF_ITERATIONS_PER_MESSAGE {
        let msg = match access.channel_receive.try_recv() {
            Ok(msg) => msg,
            Err(TryRecvError::Disconnected) => {
                return Err(FileError::InternalCommunicationError);
            },
            Err(TryRecvError::Empty) => {
                sleep(Duration::from_millis(sleep_duration));
                continue;
            },
        };

        let msg = match handler(msg) {
            (None, Some(result)) => return result,
            (Some(msg), None) => msg,
            _ => panic!(),
        };

        access.handle_unexpected_message(msg);
    }

    warn!("Received too many unxpected messages");
    Err(FileError::InternalCommunicationError)
}

impl Drop for FileAccess {
    fn drop(& mut self) {
        let _ = self.close();
    }
}

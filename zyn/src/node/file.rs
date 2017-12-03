use std::cmp::{ min };
use std::fmt::{ Display, Formatter, Result as FmtResult } ;
use std::fs::{ OpenOptions, File };
use std::io::{ Read, Write };
use std::option::{ Option };
use std::path::{ PathBuf, Path, Display as PathDisplay };
use std::ptr::{ copy };
use std::str;
use std::sync::mpsc::{ channel, Sender, Receiver, TryRecvError };
use std::thread::{ sleep };
use std::thread::{ spawn, JoinHandle };
use std::time::{ Duration };
use std::vec::{ Vec };

use node::crypto::{ Crypto, Context };
use node::serialize::{ SerializedMetadata };
use node::user_authority::{ Id };
use node::common::{ utc_timestamp, log_crypto_context_error,
                    FileRevision, Timestamp, Buffer, NodeId, FileType };

static MAX_WAIT_DURATION_PER_MESSAGE_MS: u64 = 500;
static MAX_NUMBER_OF_ITERATIONS_PER_MESSAGE: u64 = 5;

struct FileThreadHandle {
    thread: JoinHandle<()>,
    access: FileAccess,
}

pub struct FileHandle {
    path: PathBuf,
    file_thread: Option<FileThreadHandle>,
    metadata: Option<Metadata>,
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
        max_part_of_file_size: usize
    ) -> Result<FileHandle, ()> {
        let context = crypto.create_context()
            .map_err(| () | log_crypto_context_error())
            ? ;

        FileImpl::create(& path, context, user, parent, file_type, max_part_of_file_size) ? ;

        Ok(FileHandle{
            path: path,
            file_thread: None,
            metadata: None,
        })
    }

    pub fn init(path: PathBuf) -> Result<FileHandle, ()> {
        if ! FileImpl::exists(& path) {
            error!("Physical file does not exist, path=\"{}\"", path.display());
            return Err(());
        }

        Ok(FileHandle{
            path: path,
            file_thread: None,
            metadata: None,
        })
    }

    pub fn metadata(& mut self, crypto: & Crypto) -> Result<Metadata, ()> {

        self.update();

        if let Some(ref mut file_service) = self.file_thread {
            let metadata = file_service.access.metadata()
                .map_err(| _ | error!("Failed to get metadata from file service"))
                ? ;
            return Ok(metadata);
        }

        if let Some(ref metadata) = self.metadata {
            return Ok(metadata.clone())
        }

        let context = crypto.create_context()
            .map_err(| () | log_crypto_context_error())
            ? ;

        let metadata = Metadata::load(& self.path, & context) ? ;
        self.metadata = Some(metadata.clone());
        Ok(metadata)
    }

    pub fn open(& mut self, crypto: & Crypto, user: Id) -> Result<FileAccess, ()> {

        if let Some(ref mut file_thread) = self.file_thread {
            debug!("Opening file, service already running: path={}", self.path.display());
            file_thread.access.channel_send.send(Request::RequestAccess {
                user: user
            }).unwrap();

            match file_thread.access.channel_receive.recv() {
                Ok(Response::Access { access }) => return Ok(access),
                Ok(_) => return Err(()),
                Err(_) => return Err(()),
            }

        } else {
            let context = crypto.create_context()
                .map_err(| () | log_crypto_context_error())
                ? ;

            debug!("Opening file, starting service: path={}", self.path.display());
            let access = self.start_file_thread(context, user) ? ;
            Ok(access)
        }
    }

    pub fn is_open(& mut self) -> bool {
        self.update();
        self.file_thread.is_some()
    }

    pub fn close(& mut self) {
        if !self.is_open() {
            return ;
        }

        let file_thread = self.file_thread.take().unwrap();
        let _ = file_thread.access.channel_send.send(Request::Close);
        let _ = file_thread.thread.join()  // todo: Somekind of timeout should be used here
            .map_err(| error | warn!("Failed to join file thread, error={:?}", error))
            ;
    }

    fn start_file_thread(& mut self, crypto_context: Context, user: Id)
                         -> Result<FileAccess, ()>
    {
        let metadata = {
            if self.metadata.is_some() {
                self.metadata.take().unwrap()
            } else {
                Metadata::load(& self.path, & crypto_context) ?
            }
        };

        let mut file = FileThread::open(& self.path, crypto_context, metadata) ? ;
        let access_1 = file.create_access(None);
        let access_2 = file.create_access(Some(user));

        let handle = spawn( move || {
            file.process();
        });

        self.file_thread = Some(FileThreadHandle {
            thread: handle,
            access: access_1,
        });

        Ok(access_2)
    }

    fn update(& mut self) {

        let mut close = false;
        if let Some(ref mut handle) = self.file_thread {
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
pub enum FileError {
    InternalCommunicationError,
    InternalError,
    RevisionTooOld,
    OffsetAndSizeDoNotMapToPartOfFile,
    DeleteIsonlyAllowedForLastPart,
}

#[derive(Clone, Debug)]
pub enum Notification {
    FileClosing {  }, // todo: Add reason
    PartOfFileModified { revision: FileRevision, offset: u64, size: u64 },
    PartOfFileInserted { revision: FileRevision, offset: u64, size: u64 },
    PartOfFileDeleted { revision: FileRevision, offset: u64, size: u64 },
}

#[derive(Debug)]
pub struct FileAccess {
    channel_send: Sender<Request>,
    channel_receive: Receiver<Response>,
    unhandled_notitifications: Vec<Notification>,
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

        self.channel_send.send(Request::Write {
            revision: revision,
            offset: offset,
            buffer: buffer,
        }).map_err(| _ | FileError::InternalCommunicationError) ? ;

        file_receive::<FileRevision>(self, & | msg | {
            match msg {
                Response::Write {
                    result: Ok(revision),
                } => (None, Some(Ok(revision))),
                Response::Write {
                    result: Err(status),
                } => return (None, Some(Err(status))),
                other => (Some(other), None),
            }
        })
    }

    pub fn insert(& mut self, revision: u64, offset: u64, buffer: Buffer)
                  -> Result<FileRevision, FileError> {

        self.channel_send.send(Request::Insert {
            revision: revision,
            offset: offset,
            buffer: buffer,
        }).map_err(| _ | FileError::InternalCommunicationError) ? ;

        file_receive::<FileRevision>(self, & | msg | {
            match msg {
                Response::Insert {
                    result: Ok(revision),
                } => (None, Some(Ok(revision))),
                Response::Insert {
                    result: Err(status),
                } => return (None, Some(Err(status))),
                other => (Some(other), None),
            }
        })
    }

    pub fn delete(& mut self, revision: u64, offset: u64, size: u64)
                 -> Result<FileRevision, FileError> {

        self.channel_send.send(Request::Delete {
            revision: revision,
            offset: offset,
            size: size,
        }).map_err(| _ | FileError::InternalCommunicationError) ? ;

        file_receive::<FileRevision>(self, & | msg | {
            match msg {
                Response::Delete {
                    result: Ok(revision),
                } => (None, Some(Ok(revision))),
                Response::Delete {
                    result: Err(status),
                } => return (None, Some(Err(status))),
                other => (Some(other), None),
            }
        })
    }

    pub fn read(& mut self, offset: u64, size: u64)
                -> Result<(Buffer, FileRevision), FileError> {

        self.channel_send.send(Request::Read {
            offset: offset,
            size: size,
        }).map_err(| _ | FileError::InternalCommunicationError) ? ;

        file_receive::<(Buffer, FileRevision)>(self, & | msg | {
            match msg {
                Response::Read {
                    result: Ok((buffer, revision)),
                } => (None, Some(Ok((buffer, revision)))),
                Response::Read {
                    result: Err(status),
                } => (None, Some(Err(status))),
                other => (Some(other), None),
            }
        })
    }

    pub fn lock(& mut self, revision: FileRevision, lock: & LockDescription) -> Result<(), FileError> {

        self.channel_send.send(Request::LockFile {
            revision: revision,
            description: lock.clone()
        }).map_err(| _ | FileError::InternalCommunicationError) ? ;

        file_receive::<()>(self, & | msg | {
            match msg {
                Response::LockFile {
                    result: Ok(()),
                } => (None, Some(Ok(()))),
                Response::LockFile {
                    result: Err(status),
                } => (None, Some(Err(status))),
                other => (Some(other), None),
            }
        })
    }

    pub fn unlock(& mut self, lock: & LockDescription) -> Result<(), FileError> {

        self.channel_send.send(Request::UnlockFile {
            description: lock.clone(),
        }).map_err(| _ | FileError::InternalCommunicationError) ? ;

        file_receive::<()>(self, & | msg | {
            match msg {
                Response::UnlockFile {
                    result: Ok(()),
                } => (None, Some(Ok(()))),
                Response::UnlockFile {
                    result: Err(status),
                } => (None, Some(Err(status))),
                other => (Some(other), None),
            }
        })
    }

    pub fn metadata(& mut self) -> Result<Metadata, FileError> {
        self.channel_send.send(Request::RequestMetadata { })
            .map_err(| _ | FileError::InternalCommunicationError) ? ;

        file_receive::<Metadata>(self, & | msg | {
            match msg {
                Response::Metadata{ metadata } => (None, Some(Ok(metadata))),
                other => (Some(other), None),
            }
        })
    }

    pub fn close(& mut self) -> Result<(), FileError> {
        self.channel_send.send(Request::Close { })
            .map_err(| _ | FileError::InternalCommunicationError) ? ;
        Ok(())
    }

    fn handle_unexpected_message(& mut self, msg: Response) {
        match msg {
            Response::Notification { notification } => {
                self.unhandled_notitifications.push(notification);
            },
            _other => {
                warn!("Received unexpedted message");
            },
        }
    }
}

fn file_receive<OkType>(
    access: & mut FileAccess,
    handler: & Fn(Response) -> (Option<Response>, Option<Result<OkType, FileError>>)
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

#[derive(Clone)]
struct PartOfFile {
    offset: u64,
    size: u64,
    file_number: u32,
}

#[derive(Clone, PartialEq)]
pub struct FileEvent {
    pub user: Id,
    pub timestamp: Timestamp,
}

#[derive(Clone, PartialEq)]
pub enum LockDescription {
    LockedBySystemForBlobWrite { user: Id },
}

impl LockDescription {
    fn is_locked_by(& self, lock: & LockDescription) -> bool {
        *self == *lock
    }
}

#[derive(Clone)]
pub struct Metadata {
    pub created: FileEvent,
    pub modified: FileEvent,
    pub revision: FileRevision,
    pub read: Id,
    pub write: Id,
    pub parent: NodeId,
    pub max_part_of_file_size: usize,
    pub file_type: FileType,
    part_of_file_descriptions: Vec<PartOfFile>,
}

impl Metadata {
    pub fn new_file(user: Id, parent: NodeId, file_type: FileType, max_part_of_file_size: usize) -> Metadata {
        let mut metadata = Metadata {
            created: FileEvent {
                user: user.clone(),
                timestamp: utc_timestamp(),
            },
            modified: FileEvent {
                user: user.clone(),
                timestamp: utc_timestamp(),
            },
            revision: 0,
            read: user.clone(),
            write: user,
            parent: parent,
            file_type: file_type,
            max_part_of_file_size: max_part_of_file_size,
            part_of_file_descriptions: Vec::new(),
        };
        metadata.add_part_of_file();
        metadata
    }

    pub fn size(& self) -> u64 {
        self.part_of_file_descriptions.iter().fold(0, | acc, ref element | acc + element.size)
    }

    pub fn add_part_of_file(& mut self) -> u32 {

        let file_number = self.part_of_file_descriptions.len() as u32;
        self.part_of_file_descriptions.push(PartOfFile {
            offset: file_number as u64 * self.max_part_of_file_size as u64,
            size: 0,
            file_number: file_number,
        });
        file_number
    }

    fn is_in_part(& self, file_number: u32, offset: u64, size: u64) -> bool {
        Metadata::is_in_part_description(
            & self.part_of_file_descriptions[file_number as usize],
            offset,
            size,
            self.max_part_of_file_size as u64
        )
    }

    fn is_in_part_description(file: & PartOfFile, offset: u64, size: u64, max_part_of_file_size: u64)
                              -> bool {

        if offset >= file.offset && offset < (file.offset + max_part_of_file_size) {
            let end_of_modification = offset + size;
            if end_of_modification <= (file.offset + max_part_of_file_size) {
                true
            } else {
                false
            }

        } else {
            false
        }
    }

    fn find_part_of_file(& self, offset: u64, size: u64) -> Result<u32, ()> {

        for ref desc in self.part_of_file_descriptions.iter() {
            if Metadata::is_in_part_description(desc, offset, size, self.max_part_of_file_size as u64) {
                return Ok(desc.file_number);
            }
        }
        Err(())
    }

    fn find_or_allocate_part_of_file(& mut self, offset: u64, size: u64) -> Result<u32, ()> {

        let result = self.find_part_of_file(offset, size);
        if result.is_ok() {
            return result;
        }

        let last_offset = self.part_of_file_descriptions.last().unwrap().offset;
        if offset > (last_offset + self.max_part_of_file_size as u64 - 1)
            && offset < (last_offset + self.max_part_of_file_size as u64 * 2)
        {

            let file_number = self.add_part_of_file();
            return Ok(file_number);
        }
        Err(())
    }

    fn path(path_basename: & Path) -> PathBuf {
        path_basename.with_extension("metadata")
    }

    fn store(& self, path_basename: & Path, crypto_context: & Context) -> Result<(), ()> {
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
            self.max_part_of_file_size,
        );

        for ref desc in self.part_of_file_descriptions.iter() {
            serialized.segments.push((desc.offset, desc.size, desc.file_number));
        }

        serialized.write(& crypto_context, & Metadata::path(& path_basename))
            .map_err(| () | error!("Failed to write file metadata"))
    }

    fn load(path_basename: & PathBuf, crypto_context: & Context) -> Result<Metadata, ()> {

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
            part_of_file_descriptions: Vec::new(),
            read: serialized.read,
            write: serialized.write,
            parent: serialized.parent,
            file_type: serialized.file_type,
            max_part_of_file_size: serialized.max_part_of_file_size,
        };

        for (ref offset, ref size, ref file_number) in serialized.segments {
            metadata.part_of_file_descriptions.push(PartOfFile {
                offset: offset.clone(),
                size: size.clone(),
                file_number: file_number.clone(),
            });
        }

        Ok(metadata)
    }
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////

enum Request {
    Close,
    RequestMetadata,
    RequestAccess { user: Id },
    Write { revision: FileRevision, offset: u64, buffer: Buffer },
    Insert { revision: FileRevision, offset: u64, buffer: Buffer },
    Delete { revision: FileRevision, offset: u64, size: u64 },
    Read { offset: u64, size: u64 },
    LockFile { revision: FileRevision, description: LockDescription },
    UnlockFile { description: LockDescription },
}

enum Response {
    Access { access: FileAccess },
    Metadata { metadata: Metadata },
    Write { result: Result<FileRevision, FileError> },
    Insert { result: Result<FileRevision, FileError> },
    Delete { result: Result<FileRevision, FileError> },
    Read { result: Result<(Buffer, FileRevision), FileError> },
    LockFile { result: Result<(), FileError> },
    UnlockFile { result: Result<(), FileError> },
    Notification { notification: Notification },
}

struct ConnectedAccess {
    send_user: Sender<Response>,
    receive_file: Receiver<Request>,
    user: Option<Id>,
}

impl Display for ConnectedAccess {
    fn fmt(& self, f: & mut Formatter) -> FmtResult {
        if let Some(ref user) = self.user {
            write!(f, "{}", user)
        } else {
            write!(f, "FileHandle")
        }
    }
}

impl ConnectedAccess {
    fn is_handle(& self) -> bool {
        self.user.is_none()
    }
}

struct FileThread {
    file: FileImpl,
    users: Vec<ConnectedAccess>,
}

fn send_response(user: & ConnectedAccess, response: Response) -> Result<(), ()> {
    user.send_user.send(response)
        .map_err(| desc | warn!("Failed to send response to user, desc=\"{}\"", desc))
}

impl FileThread {
    pub fn open(path_basename: & PathBuf, crypto_context: Context, metadata: Metadata)
                -> Result<FileThread, ()> {
        let file = FileImpl::open(path_basename, crypto_context, metadata) ? ;
        Ok(FileThread {
            file: file,
            users: Vec::new(),
        })
    }

    pub fn create_access(& mut self, user: Option<Id>) -> FileAccess {
        let (tx_user, rx_user) = channel::<Response>();
        let (tx_file, rx_file) = channel::<Request>();

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

            for (i, user) in self.users.iter_mut().enumerate() {
                if let Ok(message) = user.receive_file.try_recv() {

                    let mut send_ok = true;
                    match message {

                        Request::RequestAccess { user } => {
                            add_access = Some((i, user));
                        },

                        Request::RequestMetadata => {
                            send_ok = send_response(& user, Response::Metadata {
                                metadata: self.file.metadata.clone()
                            }).is_ok();
                        },

                        Request::Close => {
                            if user.is_handle() {
                                notifications.push(
                                    (true, None, Notification::FileClosing { })
                                );
                                exit = true;
                                break;
                            } else {
                                remove_access = Some(i);
                            }
                        },

                        Request::Write { revision, offset, buffer } => {
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
                                    send_ok = send_response(& user, Response::Write {
                                        result: Ok(revision),
                                    }).is_ok();
                                },
                                Err(status) => {
                                    send_ok = send_response(& user, Response::Write {
                                        result: Err(status),
                                    }).is_ok();
                                }
                            };
                        },

                        Request::Insert { revision, offset, buffer } => {
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
                                    send_ok = send_response(& user, Response::Insert {
                                        result: Ok(revision),
                                    }).is_ok();
                                },
                                Err(status) => {
                                    send_ok = send_response(& user, Response::Insert {
                                        result: Err(status),
                                    }).is_ok();
                                }
                            };
                        },

                        Request::Delete { revision, offset, size } => {
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
                                    send_ok = send_response(& user, Response::Delete {
                                        result: Ok(revision),
                                    }).is_ok();
                                },
                                Err(status) => {
                                    send_ok = send_response(& user, Response::Delete {
                                        result: Err(status),
                                    }).is_ok();
                                }
                            };
                        },

                        Request::Read { offset, size } => {
                            match self.file.read(offset, size) {
                                Ok((buffer, revision)) => {
                                    send_ok = send_response(& user, Response::Read {
                                        result: Ok((buffer, revision)),
                                    }).is_ok();
                                },
                                Err(status) => {
                                    send_ok = send_response(& user, Response::Read {
                                        result: Err(status),
                                    }).is_ok();
                                }
                            };
                        },

                        Request::LockFile { revision, description } => {
                            match self.file.lock(revision, description) {
                                Ok(()) => {
                                    send_ok = send_response(& user, Response::LockFile {
                                        result: Ok(()),
                                    }).is_ok();
                                },
                                Err(status) => {
                                    send_ok = send_response(& user, Response::LockFile {
                                        result: Err(status),
                                    }).is_ok();
                                }
                            };
                        },

                        Request::UnlockFile { description } => {
                            match self.file.unlock(description) {
                                Ok(()) => {
                                    send_ok = send_response(& user, Response::UnlockFile {
                                        result: Ok(()),
                                    }).is_ok();
                                },
                                Err(status) => {
                                    send_ok = send_response(& user, Response::UnlockFile {
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
                    Response::Access { access: access }
                ).is_err() {
                    self.users.remove(index);
                }
            }

            if self.users.len() <= 1 {
                debug!("Closing file as it has no users, file={}", self.file.display());
                let _ = send_response(& self.users[0], Response::Notification {
                    notification: Notification::FileClosing { }
                });
                exit = true;
            }

            if ! notifications.is_empty() {
                for (i, user) in self.users.iter().enumerate() {
                    for & (ref skip_handle, ref source_index, ref notification) in notifications.iter() {
                        if *skip_handle && user.is_handle() {
                            continue;
                        }

                        let mut send = true;
                        if let & Some(ref skip_index) = source_index {
                            if i == *skip_index {
                                send = false;
                            }
                        }
                        if send {
                            let _ = send_response(& user, Response::Notification {
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

struct FileImpl {
    metadata: Metadata,
    path_basename: PathBuf,
    buffer: Buffer,
    current_part_of_file_index: u32,
    crypto_context: Context,
    lock: Option<LockDescription>,
}

static DEFAULT_BUFFER_SIZE_BYTES: u64 = 1024 * 1;

impl FileImpl {

    fn display(& self) -> PathDisplay {
        self.path_basename.display()
    }

    fn exists(path_basename: & Path) -> bool {
        let path = FileImpl::path_data(path_basename, 0);
        path.exists()
    }

    fn path_data(path_basename: & Path, file_number: u32) -> PathBuf {
        path_basename.with_extension(format!("{}", file_number))
    }

    pub fn create(
        path_basename: & PathBuf,
        crypto_context: Context,
        user: Id,
        parent: NodeId,
        file_type: FileType,
        max_part_of_file_size: usize,
    ) -> Result<(), ()> {

        let mut file = FileImpl {
            buffer: Buffer::with_capacity(DEFAULT_BUFFER_SIZE_BYTES as usize),
            current_part_of_file_index: 0,
            crypto_context: crypto_context,
            metadata: Metadata::new_file(user, parent, file_type, max_part_of_file_size),
            path_basename: path_basename.clone(),
            lock: None,
        };
        file.store();
        Ok(())
    }

    pub fn open(path_basename: & PathBuf, crypto_context: Context, metadata: Metadata)
                -> Result<FileImpl, ()> {

        let mut file = FileImpl {
            buffer: Buffer::with_capacity(DEFAULT_BUFFER_SIZE_BYTES as usize),
            current_part_of_file_index: 0,
            crypto_context: crypto_context,
            metadata: metadata,
            path_basename: path_basename.clone(),
            lock: None,
        };

        if file.metadata.part_of_file_descriptions.len() == 0 {
            error!("File has not data file descriptions, path_basename={}",
                   path_basename.display());
            return Err(());
        }

        file.load_part_of_file(0) ? ;
        Ok(file)
    }

    fn load_part_of_file(& mut self, index_of_part: u32) -> Result<(), ()> {
        let ref part_of_file = self.metadata.part_of_file_descriptions[index_of_part as usize];
        let path = FileImpl::path_data(& self.path_basename, part_of_file.file_number);
        let mut file = match OpenOptions::new().read(true).create(false).open(path.as_path()) {
            Ok(file) => file,
            Err(_error_code) => {
                error!("Failed to open file for reading: path={}", path.display());
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
        self.current_part_of_file_index = index_of_part;
        if data.len() > 0 {
            self.buffer = self.crypto_context.decrypt(& data) ? ;
        }
        Ok(())
    }

    fn write_part_of_file(& mut self) -> Result<(), ()> {

        let path = FileImpl::path_data(& self.path_basename, self.current_part_of_file_index);
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
                let ref mut desc = self.metadata.part_of_file_descriptions[self.current_part_of_file_index as usize];
                desc.size = self.buffer.len() as u64;
                Ok(())
            },
        }
    }

    fn swap_part_of_file(& mut self, file_index_of_new_file: u32) -> Result<(), ()> {
        debug!("Swapping current buffer, current_part_of_file_index={}, new_index={}, file={}",
               self.current_part_of_file_index, file_index_of_new_file, self.display());

        self.write_part_of_file() ? ;
        self.load_part_of_file(file_index_of_new_file)
    }

    fn store(& mut self) {
        let _ = self.write_part_of_file();
        let _ = self.metadata.store(& self.path_basename, & self.crypto_context);
    }

    fn update_current_part_of_file_size(& mut self) {
        let ref mut desc = self.metadata.part_of_file_descriptions[self.current_part_of_file_index as usize];
        desc.size = self.buffer.len() as u64;
    }

    fn lock(& mut self, revision: FileRevision, desc: LockDescription) -> Result<(), FileError> {

        if revision != self.metadata.revision {
            return Err(FileError::RevisionTooOld);
        }

        if let Some(_) = self.lock {
            return Err(FileError::RevisionTooOld); // todo: fix error
        }

        self.lock = Some(desc);

        Ok(())
    }

    fn unlock(& mut self, desc: LockDescription) -> Result<(), FileError> {

        if self.lock.is_none() {
            return Err(FileError::RevisionTooOld); // todo: fix error
        }

        let mut release_lock = false;
        if let Some(ref lock) = self.lock {
            if lock.is_locked_by(& desc) {
                release_lock = true;
            } else {
                return Err(FileError::RevisionTooOld); // todo: fix error
            }
        }

        if release_lock {
            self.lock = None;
        }

        Ok(())
    }

    fn is_edit_allowed(& self, user: & Option<Id>) -> Result<(), FileError> {

        if user.is_none() {
            return Err(FileError::RevisionTooOld); // todo: replace error
        }

        let user_id = user.as_ref().unwrap();

        match self.lock {
            None => Ok(()),
            Some(LockDescription::LockedBySystemForBlobWrite { ref user }) => {
                if user_id != user {
                    return Err(FileError::RevisionTooOld); // todo: replace error
                }
                Ok(())
            },
        }
    }

    fn write(& mut self, user: & Option<Id>, revision: FileRevision, offset: u64, buffer: Buffer)
             -> Result<FileRevision, FileError> {

        if ! self.metadata.is_in_part(self.current_part_of_file_index, offset, buffer.len() as u64) {
            let file_number = self.metadata.find_or_allocate_part_of_file(offset, buffer.len() as u64)
                .map_err(| () | FileError::OffsetAndSizeDoNotMapToPartOfFile)
                ? ;

            self.swap_part_of_file(file_number)
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
        self.update_current_part_of_file_size();
        Ok(self.metadata.revision)
    }

    fn insert(& mut self, user: & Option<Id>, revision: FileRevision, offset: u64, buffer: Buffer)
              -> Result<FileRevision, FileError> {

        if ! self.metadata.is_in_part(self.current_part_of_file_index, offset, buffer.len() as u64) {
            let file_number = self.metadata.find_or_allocate_part_of_file(offset, buffer.len() as u64)
                .map_err(| () | FileError::OffsetAndSizeDoNotMapToPartOfFile)
                ? ;

            self.swap_part_of_file(file_number)
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
        self.update_current_part_of_file_size();
        Ok(self.metadata.revision)
    }

    pub fn read(& mut self, offset: u64, size: u64)
                -> Result<(Buffer, FileRevision), FileError> {
        if ! self.metadata.is_in_part(self.current_part_of_file_index, offset, size) {
            let file_number = self.metadata.find_part_of_file(offset, size)
                .map_err(| () | FileError::OffsetAndSizeDoNotMapToPartOfFile)
                ? ;

            self.swap_part_of_file(file_number)
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

        if ! self.metadata.is_in_part(self.current_part_of_file_index, offset, size) {

            let file_number = self.metadata.find_part_of_file(offset, size)
                .map_err(| () | FileError::OffsetAndSizeDoNotMapToPartOfFile)
                ? ;

            if self.current_part_of_file_index != (self.metadata.part_of_file_descriptions.len() as u32 - 1) {
                return Err(FileError::DeleteIsonlyAllowedForLastPart);
            }

            self.swap_part_of_file(file_number)
                .map_err(| () | FileError::InternalError)
                ? ;
        } else {
            if self.current_part_of_file_index != (self.metadata.part_of_file_descriptions.len() as u32 - 1) {
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
        self.update_current_part_of_file_size();
        Ok(self.metadata.revision)
    }
}

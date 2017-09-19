use std::path::{ PathBuf };
use std::result::{ Result };
use std::fmt::{ Display, Formatter, Result as FmtResult };
use std::sync::mpsc::{ Receiver, Sender, TryRecvError };
use std::thread::{ sleep };
use std::time::{ Duration };
use std::vec::{ Vec };
use std::{ str };

use node::connection::{ Connection };
use node::file::{ FileAccess, FileError, Notification };
use node::filesystem::{ FilesystemError };
use node::node::{ ClientProtocol, NodeProtocol, FilesystemElement,
                  ErrorResponse, NodeError };
use node::common::{ FileDescriptor, NodeId, Buffer, OpenMode, FileType };
use node::user_authority::{ Id };

/*
# Protocol definition
[] - Required
() - Optional
| - Or
.. - Repeated

[tag]:[content];

## Fields

Uint (>= 0):
U:[number];

Version:
V:[number];

Transaction Id
T:[Uint];

Bytes:
B:[bytes];

String:
S:[Uint: size][Bytes: utf8 content];

Path (always absolute)
P:[String];

Node-Id:
N:[Uint];

File Descriptor:
F:[Path|Node-Id];

Block:
BL:[Uint: offset][Uint: size];

Key-value-pair-of-TYPE:
KVP:[String: key][TYPE: value];

List-of-TYPE:
L:[uint: list length] LE:[TYPE];..;

End: End of message
E:;
*/

/*
## Notifications:

Part of file modified:
PF-MOD:[Node-Id][Uint: revision][Block];

Part of file inserted:
PF-INS:[Node-Id][Uint: revision][Block];

Part of file deleted:
PF-DEL:[Node-Id][Uint: revision][Block];

File-closed:
F-CLOSED:[Node-Id];

Connection-closing:
DISCONNECTED:;[String: description]

Notification:
[Version]NOTIFICATION:[
  File-changed
  | File-closed
  | Connection-closing
];[End]
 */

enum CommonErrorCodes {
    NoError = 0,
    ErrorMalformedMessage = 1,
    InternalCommunicationError = 2,
    ErrorFileIsNotOpen = 3,
    ErrorFileOpenedInReadMode = 4,
}

fn map_node_error_to_uint(error: ErrorResponse) -> u32 {
    match error {
        ErrorResponse::NodeError { error } => {
            match error {
                NodeError::InvalidUsernamePassword => 100,
                NodeError::ParentIsNotFolder => 101,
                NodeError::UnauthorizedOperation => 102,
                NodeError::InternalCommunicationError => 103,
                NodeError::InternalError => 104,
                NodeError::UnknownFile => 105,
            }
        },
        ErrorResponse::FilesystemError { error } => {
            match error {
                FilesystemError::InvalidNodeId => 200,
                FilesystemError::FolderIsNotEmpty => 201,
                FilesystemError::InvalidPathSize => 202,
                FilesystemError::InvalidPath => 203,
                FilesystemError::HostFilesystemError => 204,
                FilesystemError::AllNodesInUse => 205,
                FilesystemError::ParentIsNotFolder => 206,
                FilesystemError::NodeIsNotFile => 207,
                FilesystemError::NodeIsNotFolder => 208,
            }
        }
    }
}

fn map_file_error_to_uint(result: FileError) -> u32 {
    match result {
        FileError::InternalCommunicationError => 300,
        FileError::InternalError => 301,
        FileError::RevisionTooOld => 302,
        FileError::OffsetAndSizeDoNotMapToPartOfFile => 303,
        FileError::DeleteIsonlyAllowedForLastPart => 304,
    }
}

const READ: u64 = 0;
const READ_WRITE: u64 = 1;
const FILE: u64 = 0;
const FOLDER: u64 = 1;
const RANDOM_ACCESS_FILE: u64 = 0;

/*
## Messages:

Authenticate:
<- [Version]A:[Transaction Id][String: username][String: password];[End]
-> [Version]RSP:[Transaction Id][Uint: error code];[End]
*/

/*
Create file
<- [Version]CREATE-FILE:[Transaction Id][FileDescriptor: parent][String: name][Uint: type];[End]
 * Type: 0: random access,
-> [Version]RSP:[Transaction Id][Uint: error code];(Node-Id)[End]
*/

/*
Create folder
<- [Version]CREATE-FOLDER:[Transaction Id][FileDescriptor: parent][String: name];[End]
-> [Version]RSP:[Transaction Id][Uint: error code];(Node-Id)[End]
*/

/*
Open file:
<- [Version]C:[Transaction Id][FileDescriptor][Uint: type];[End]
 * Type: 0: read, 1: read-write
-> [Version]RSP:[Transaction Id][Uint: error code](Node-Id)(Uint: revision)(Uint: size)(Uint: type);[End]
 * Type: 0: random access
*/

/*
Close file:
<- [Version]C:[Transaction Id][NodeId];[End]
-> [Version]RSP:[Transaction Id][Uint: error code];[End]
*/

/*
Write:
<- [Version]RA-W:[Transaction Id][Node-Id: opened file][Block];[End]
-> ([Version]RSP:[Transaction Id][Uint: error code];[End]) Only if there is an error
<- [data]
-> [Version]RSP:[Transaction Id][Uint: error code](Uint: revision);[End]
*/

/*
Insert:
<- [Version]RA-I:[Transaction Id][Node-Id: opened file][Block];[End]
-> ([Version]RSP:[Transaction Id][Uint: error code];[End]) Only if there is an error
<- [data]
-> [Version]RSP:[Transaction Id][Uint: error code](Uint: revision);[End]
*/

/*
Delete:
<- [Version]RA-D:[Transaction Id][Node-Id: opened file][Block];[End]
-> [Version]RSP:[Transaction Id][Uint: error code](Uint: revision);[End]
*/

/*
Read:
<- [Version]R:[Transaction Id][Node-Id: opened file][Block];[End]
-> [Version]RSP:[Transaction Id][Uint: error code](Uint: revision)(Block: of what will be sent);[End]
-> [data]
 * Data is only sent if file is not empty
*/

/*
Query: counters
<- [Version]Q-COUNTERS:[Transaction Id];[End]
-> [Version]RSP:[Transaction Id][List-of-Key-value-pair-of-u32];;[End]
*/

/*
Query: list
<- [Version]Q-LIST:[Transaction Id][FileDescriptor];[End]
-> [Version]RSP:[Transaction Id][List-of-String-NodeId-Uint];[End]
 * Uint type: 0: file, 1: directory
*/

/*
Query: fileystem element
<- [Version]Q-FILESYSTEM:[Transaction Id][FileDescriptor];[End]
-> [Version]RSP:[Transaction Id][Uint: type][List-of-key-value-pairs];[End]
 * Uint type: 0: file, 1: directory
*/

/*
Delete:
<- [Version]DELETE:[Transaction Id][FileDescriptor];[End]
-> [Version]RSP:[Transaction Id][Uint: error code];[End]
 */

// todo

/*
Configure: Add user/group:
<- [Version]C:AUG:[Transaction Id][Uint: type][String: name];[End]
 * type: 0: user, 1: group
-> [Version]RSP:[Transaction Id][Uint: error code];[End]
*/

/*
Configure: Modify user/group:
<- [Version]C:MUG:[Transaction Id][uint: type][String: name][Key-value-list];[End]
 * type: 0: user, 1: group
-> [Version]RSP:[Transaction Id][Uint: error code];[End]
*/

/*
Query: User/group:
<- [Version]Q:UG:[Transaction Id][Uint: type][String: name];[End]
 * type: 0: user, 1: group
-> [Version]RSP:[Transaction Id][Uint: error code](Key-value-list);[End]
*/

/*
Configure: Remove user/group:
<- [Version]C:DUG:[Transaction Id][Uint: type][String: name];[End]
 * type: 0: user, 1:group
-> [Version]RSP:[Transaction Id][Uint: error code];[End]
*/

/*
Query: file/folder properties:
<- [Version]Q:F:[Transaction Id][FileDescriptor];[End]
-> [Version]RSP:[Transaction Id][Uint: error code](Key-value-list: properties);[End]
*/

/*
Configure: Set file/folder properties:
<- [Version]C:F:[Transaction Id][FileDescriptor][Key-value-list];[End]
-> [Version]RSP:[Transaction Id][Uint: error code];[End]
*/

const DEFAULT_BUFFER_SIZE: usize = 1024 * 4;
static EMPTY: & 'static str = "";
static FIELD_END_MARKER: & 'static str = "E:;";

macro_rules! try_parse {
    ($result:expr, $class:expr, $transaction_id:expr) => {{
        match $result {
            Ok(value) => value,
            Err(_) => {
                try_send!(
                    $class,
                    Client::send_response(
                        & mut $class.connection,
                        $transaction_id,
                        CommonErrorCodes::ErrorMalformedMessage as u32,
                        EMPTY
                    ));
                return Err(());
            }
        }
    }}
}

macro_rules! try_send_notification {
    ($class:expr, $content:expr) => {{
        try_send!(
            $class,
            Client::send_notification(
                & mut $class.connection,
                & $content
            ));
    }}
}

macro_rules! try_send_rsp {
    ($class:expr, $transaction_id:expr, $error_code:expr, $additional_fields:expr) => {{
        try_send!(
            $class,
            Client::send_response(
                & mut $class.connection,
                $transaction_id,
                $error_code,
                $additional_fields
            ));
    }}
}

macro_rules! try_send {
    ($class:expr, $result:expr) => {{
        if $result.is_err() {
            $class.status = Status::FailedToSendToClient;
        }
    }}
}

enum Status {
    Ok,
    AuthenticationError { trial: u8 },
    FailedToSendToNode,
    FailedToreceiveFromNode,
    FailedToSendToClient,
    ClientNotAuthenticated,
    ClientAlreadyAuthenticated,
}

impl Display for Status {
    fn fmt(& self, f: & mut Formatter) -> FmtResult {
        match *self {
            Status::Ok =>
                write!(f, "Ok"),
            Status::FailedToSendToNode =>
                write!(f, "FailedToSendToNode"),
            Status::FailedToreceiveFromNode =>
                write!(f, "FailedToreceiveFromNode"),
            Status::AuthenticationError { ref trial } =>
                write!(f, "AuthenticationError, trial={}", trial),
            Status::FailedToSendToClient =>
                write!(f, "FailedToSendToClient"),
            Status::ClientNotAuthenticated =>
                write!(f, "ClientNotAuthenticated"),
            Status::ClientAlreadyAuthenticated =>
                write!(f, "ClientAlreadyAuthenticated"),
        }
    }
}

impl Status {
    fn is_in_error_state(& self) -> bool {
        match *self {
            Status::Ok => false,
            Status::AuthenticationError { ref trial } => {
                *trial > 2
            },
            _ => true,
        }
    }
}

struct ClientBuffer {
    buffer: Buffer,
    buffer_index: usize,
}

impl ClientBuffer {
    fn with_capacity(size: usize) -> ClientBuffer {
        ClientBuffer {
            buffer: Vec::with_capacity(size),
            buffer_index: 0,
        }
    }

    fn take(& mut self, output: & mut Buffer) {
        let requested_max = self.buffer_index + output.len();
        let used_max = {
            if requested_max > self.buffer.len() {
                self.buffer.len()
            } else {
                requested_max
            }
        };
        output.extend(& self.buffer[self.buffer_index .. used_max]);
        self.buffer_index = used_max;
    }

    fn drop_consumed_buffer(& mut self) {
        self.buffer.drain(0..self.buffer_index);
        self.buffer_index = 0;
    }

    fn debug_buffer(& self) {
        let buffer = self.get_buffer();
        let size = buffer.len();
        debug!("Buffer ({}): {}", size, String::from_utf8_lossy(buffer));
    }

    fn get_mut_buffer(& mut self) -> & mut Vec<u8> {
        & mut self.buffer
    }

    fn get_buffer(& self) -> & [u8] {
        & self.buffer[self.buffer_index .. ]
    }

    fn get_buffer_length(& self) -> usize {
        self.buffer.len() - self.buffer_index + 1
    }

    fn get_buffer_with_length(& self, size: usize) -> & [u8] {
        & self.buffer[self.buffer_index .. self.buffer_index + size]
    }
}

pub struct Client {
    connection: Connection,
    buffer: ClientBuffer,
    node_receive: Receiver<ClientProtocol>,
    node_send: Sender<NodeProtocol>,
    open_files: Vec<(NodeId, OpenMode, FileType, FileAccess,)>,
    user: Option<Id>,
    status: Status,
    node_message_buffer: Vec<ClientProtocol>,
}

impl Display for Client {
    fn fmt(& self, f: & mut Formatter) -> FmtResult {
        if let Some(ref user) = self.user {
            write!(f, "{}", user)
        } else {
            write!(f, "None")
        }
    }
}

impl Client {
    pub fn new(
        connection: Connection,
        node_receive: Receiver<ClientProtocol>,
        node_send: Sender<NodeProtocol>
    ) -> Client {

        Client {
            connection: connection,
            buffer: ClientBuffer::with_capacity(DEFAULT_BUFFER_SIZE),
            node_receive: node_receive,
            node_send: node_send,
            open_files: Vec::with_capacity(5),
            user: None,
            status: Status::Ok,
            node_message_buffer: Vec::with_capacity(5),
        }
    }

    pub fn process(& mut self) {

        loop {

            // Warning about variable below is probably a Rust bug
            // https://github.com/rust-lang/rust/issues/28570
            let mut is_processing: bool = false;

            let mut remove: Option<usize> = None;
            for (index, & mut (ref node_id, _, _, ref mut access)) in self.open_files
                .iter_mut()
                .enumerate() {
                    loop {
                        match access.pop_notification() {
                            Some(Notification::FileClosing {  }) => {
                                is_processing = true;
                                try_send_notification!(
                                    self,
                                    Client::notification_closed(node_id)
                                );
                                remove = Some(index);
                                break;
                            },
                            Some(Notification::PartOfFileModified { revision, offset, size }) => {
                                is_processing = true;
                                try_send_notification!(
                                    self,
                                    Client::notification_modified(
                                        node_id, & revision, & offset, & size)
                                );
                            },
                            Some(Notification::PartOfFileInserted { revision, offset, size }) => {
                                is_processing = true;
                                try_send_notification!(
                                    self,
                                    Client::notification_inserted(
                                        node_id, & revision, & offset, & size)
                                );
                            },
                            Some(Notification::PartOfFileDeleted { revision, offset, size }) => {
                                is_processing = true;
                                try_send_notification!(
                                    self,
                                    Client::notification_deleted(
                                        node_id, & revision, & offset, & size)
                                );
                            },
                            None => break,
                        }
                    }
                    if remove.is_some() {
                        break ;
                    }
                }
            if let Some(ref index) = remove {
                self.open_files.remove(*index);
            }

            match self.receive_from_node() {
                Err(()) => {
                    error!("Error receiving from node");
                    // todo: send notification
                    break;
                }
                Ok(None) => (),
                Ok(Some(message)) => {
                    is_processing = true;

                    match message {
                        ClientProtocol::Shutdown { reason } => {

                            debug!("Received shutdown order from node");

                            for (_, _, _, mut access) in self.open_files.drain(..) {
                                let _ = access.close();
                            }

                            try_send_notification!(
                                self,
                                Client::notification_disconnected(& reason)
                            );
                            break;
                        },
                        ClientProtocol::Quit => {
                            info!("Thread received quit command");
                            panic!("Unahdled");
                        },
                        _ => {
                            panic!("Unahdled");
                        },
                    }
                },
            }

            match self.connection.read(& mut self.buffer.get_mut_buffer()) {
                Ok(false) => (),
                Ok(true) => is_processing = true,
                Err(()) => {
                    warn!("Error receiving from socket, client={}", self);
                    break;
                }
            }

            if ! is_processing {
                sleep(Duration::from_millis(100));
                continue ;
            }

            if ! self.is_complete_message() {
                continue ;
            }

            self.buffer.debug_buffer();

            if self.expect("V:1;").is_err() {
                try_send_rsp!(self, 0, CommonErrorCodes::ErrorMalformedMessage as u32, EMPTY);
                continue ;
            }

            if self.expect("A:").is_ok() {
                if self.is_authenticated() {
                    self.status = Status::ClientAlreadyAuthenticated;
                    break ;
                }
                let _ = self.handle_authentication_req();
            } else {
                if ! self.is_authenticated() {
                    self.status = Status::ClientNotAuthenticated;
                    break ;
                }

                if self.expect("CREATE-FILE:").is_ok() {
                    let _ = self.handle_create_file_req();
                } else if self.expect("CREATE-FOLDER:").is_ok() {
                    let _ = self.handle_create_folder_req();
                } else if self.expect("O:").is_ok() {
                    let _ = self.handle_open_req();
                } else if self.expect("CLOSE:").is_ok() {
                    let _ = self.handle_close_req();
                } else if self.expect("RA-W:").is_ok() {
                    let _ = self.handle_write_req();
                } else if self.expect("RA-I:").is_ok() {
                    let _ = self.handle_insert_req();
                } else if self.expect("RA-D:").is_ok() {
                    let _ = self.handle_delete_req();
                } else if self.expect("R:").is_ok() {
                    let _ = self.handle_read_req();
                } else if self.expect("DELETE:").is_ok() {
                    let _ = self.handle_delete_fs_element_req();
                } else if self.expect("Q-COUNTERS:").is_ok() {
                    let _ = self.handle_query_counters_req();
                } else if self.expect("Q-LIST:").is_ok() {
                    let _ = self.handle_query_list_req();
                } else if self.expect("Q-FILESYSTEM:").is_ok() {
                    let _ = self.handle_query_fs_req();
                } else {
                    warn!("Unhandled message, client={}", self);
                }
            }

            self.buffer.drop_consumed_buffer();

            if self.status.is_in_error_state() {
                break ;
            }
        }

        info!("Closing connection to client, client={}, status={}", self, self.status);
    }

    fn is_authenticated(& self) -> bool {
        self.user.is_some()
    }

    fn is_complete_message(& mut self) -> bool {
        let end_marker_bytes = FIELD_END_MARKER.as_bytes();
        self.buffer.get_buffer()
            .windows(end_marker_bytes.len())
            .position(| window | window == end_marker_bytes)
            .is_some()
    }

    fn receive_from_node(& mut self) -> Result<Option<ClientProtocol>, ()> {
        if ! self.node_message_buffer.is_empty() {
            return Ok(Some(self.node_message_buffer.pop().unwrap()));
        }

        match self.node_receive.try_recv() {
            Ok(msg) => Ok(Some(msg)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(()),
        }
    }

    fn handle_unexpected_message_from_node(& mut self, msg: ClientProtocol) {
        match msg {
            ClientProtocol::Shutdown { .. } => self.node_message_buffer.push(msg),
            _ => {
                self.status = Status::FailedToreceiveFromNode;
                debug!("Unexpected message from node");
            },
        };
    }

    fn send_to_node(& mut self, transaction_id: u32, msg: NodeProtocol) -> Result<(), ()> {
        if let Err(desc) = self.node_send.send(msg) {
            warn!("Failed to send message to node, id={}, desc={}", self, desc);
            let _ = Client::send_response(
                & mut self.connection,
                transaction_id,
                CommonErrorCodes::InternalCommunicationError as u32,
                EMPTY
            );
            self.status = Status::FailedToSendToNode;
            return Err(());
        }
        Ok(())
    }

    fn handle_authentication_req(& mut self) -> Result<(), ()> {

        let transaction_id = try_parse!(self.parse_transaction_id(), self, 0);
        let username = try_parse!(self.parse_string(), self, transaction_id);
        let password = try_parse!(self.parse_string(), self, transaction_id);
        try_parse!(self.expect(";"), self, transaction_id);
        try_parse!(self.parse_end_of_message(), self, transaction_id);

        debug!("Authenticate, username=\"{}\"", username);

        self.send_to_node(transaction_id, NodeProtocol::AuthenticateRequest {
            username: username.clone(),
            password: password
        }) ? ;

        let id = node_receive::<Id>(
            self,
            & | msg, client | {
                match msg {
                    ClientProtocol::AuthenticateResponse { result: Ok(id) } => {
                        try_send_rsp!(
                            client,
                            transaction_id,
                            CommonErrorCodes::NoError as u32,
                            EMPTY
                        );
                        (None, Some(Ok(id)))
                    },

                    ClientProtocol::AuthenticateResponse { result: Err(error) } => {
                        try_send_rsp!(
                            client,
                            transaction_id,
                            map_node_error_to_uint(error),
                            EMPTY
                        );

                        if let Status::AuthenticationError { ref mut trial } = client.status {
                            *trial += 1;
                        } else {
                            client.status = Status::AuthenticationError { trial: 1 };
                        }

                        (None, Some(Err(())))
                    },

                    other => {
                        (Some(other), None)
                    }
                }
            })
            ? ;

        debug!("Authentication ok, username=\"{}\", id={}", username, id);

        self.user = Some(id);
        Ok(())
    }

    fn handle_create_file_req(& mut self) -> Result<(), ()> {

        let transaction_id = try_parse!(self.parse_transaction_id(), self, 0);
        let parent = try_parse!(self.parse_file_descriptor(), self, transaction_id);
        let name = try_parse!(self.parse_string(), self, transaction_id);
        let type_uint = try_parse!(self.parse_unsigned(), self, transaction_id);
        try_parse!(self.expect(";"), self, transaction_id);
        try_parse!(self.parse_end_of_message(), self, transaction_id);

        let type_enum = match type_uint {
            RANDOM_ACCESS_FILE => FileType::RandomAccess,
            _ => {
                try_send_rsp!(
                    self,
                    transaction_id,
                    CommonErrorCodes::ErrorMalformedMessage as u32,
                    EMPTY
                );
                return Err(());
            },
        };

        trace!("Create file: user={}, parent=\"{}\", name=\"{}\", type={}",
               self, parent, name, type_enum);

        let user = self.user.as_ref().unwrap().clone();
        self.send_to_node(transaction_id, NodeProtocol::CreateFileRequest {
            parent: parent,
            type_of_file: type_enum,
            name: name,
            user: user,
        }) ? ;

        node_receive::<()>(
            self,
            & | msg, client | {
                match msg {
                    ClientProtocol::CreateFilesystemElementResponse {
                        result: Ok(node_id),
                    } => {
                        try_send_rsp!(
                            client,
                            transaction_id,
                            CommonErrorCodes::NoError as u32,
                            & Client::node_id_to_string(node_id)
                        );
                        (None, Some(Ok(())))
                    },

                    ClientProtocol::CreateFilesystemElementResponse {
                        result: Err(error),
                    } => {
                        try_send_rsp!(
                            client,
                            transaction_id,
                            map_node_error_to_uint(error),
                            EMPTY
                        );
                        (None, Some(Err(())))
                    },

                    other => {
                        (Some(other), None)
                    }
                }
            })
    }

    fn handle_create_folder_req(& mut self) -> Result<(), ()> {

        let transaction_id = try_parse!(self.parse_transaction_id(), self, 0);
        let parent = try_parse!(self.parse_file_descriptor(), self, transaction_id);
        let name = try_parse!(self.parse_string(), self, transaction_id);
        try_parse!(self.expect(";"), self, transaction_id);
        try_parse!(self.parse_end_of_message(), self, transaction_id);

        trace!("Create folder: user={}, parent=\"{}\",  name=\"{}\"",
               self, parent, name);

        let user = self.user.as_ref().unwrap().clone();
        self.send_to_node(transaction_id, NodeProtocol::CreateFolderRequest {
            parent: parent,
            name: name,
            user: user,
        }) ? ;

        node_receive::<()>(
            self,
            & | msg, client | {
                match msg {
                    ClientProtocol::CreateFilesystemElementResponse {
                        result: Ok(node_id),
                    } => {
                        try_send_rsp!(
                            client,
                            transaction_id,
                            CommonErrorCodes::NoError as u32,
                            & Client::node_id_to_string(node_id)
                        );
                        (None, Some(Ok(())))
                    },

                    ClientProtocol::CreateFilesystemElementResponse {
                        result: Err(error),
                    } => {
                        try_send_rsp!(
                            client,
                            transaction_id,
                            map_node_error_to_uint(error),
                            EMPTY
                        );
                        (None, Some(Err(())))
                    },

                    other => {
                        (Some(other), None)
                    }
                }
            })
    }

    fn handle_open_req(& mut self) -> Result<(), ()> {

        let transaction_id = try_parse!(self.parse_transaction_id(), self, 0);
        let fd = try_parse!(self.parse_file_descriptor(), self, transaction_id);
        let mode_uint = try_parse!(self.parse_unsigned(), self, transaction_id);
        try_parse!(self.expect(";"), self, transaction_id);
        try_parse!(self.parse_end_of_message(), self, transaction_id);

        let mode = match mode_uint {
            READ => OpenMode::Read,
            READ_WRITE => OpenMode::ReadWrite,
            _ => {
                try_send_rsp!(
                    self,
                    transaction_id,
                    CommonErrorCodes::ErrorMalformedMessage as u32,
                    EMPTY
                );
                return Err(());
            },
        };

        trace!("Open file, user={}, fd=\"{}\"", self, fd);

        let user = self.user.as_ref().unwrap().clone();
        self.send_to_node(transaction_id, NodeProtocol::OpenFileRequest {
            mode: mode.clone(),
            file_descriptor: fd,
            user: user,
        }) ? ;

        let (access, node_id, file_type) = node_receive::<(FileAccess, NodeId, FileType)>(
            self,
            & | msg, client | {
                match msg {
                    ClientProtocol::OpenFileResponse {
                        result: Ok((access, node_id, revision, file_type, size)),
                    } => {

                        let mut fields = Client::node_id_to_string(node_id.clone());
                        fields.push_str(& Client::unsigned_to_string(revision));
                        fields.push_str(& Client::unsigned_to_string(size));

                        match file_type {
                            FileType::RandomAccess =>
                                fields.push_str(& Client::unsigned_to_string(RANDOM_ACCESS_FILE)),
                        };

                        try_send_rsp!(
                            client,
                            transaction_id,
                            CommonErrorCodes::NoError as u32,
                            & fields
                        );
                        (None, Some(Ok((access, node_id, file_type))))
                    },

                    ClientProtocol::OpenFileResponse {
                        result: Err(error),
                    } => {
                        try_send_rsp!(
                            client,
                            transaction_id,
                            map_node_error_to_uint(error),
                            EMPTY
                        );
                        (None, Some(Err(())))
                    },

                    other => {
                        (Some(other), None)
                    }
                }
            })
            ? ;

        self.open_files.push((node_id, mode, file_type, access));
        Ok(())
    }

    fn handle_close_req(& mut self) -> Result<(), ()> {

        let transaction_id = try_parse!(self.parse_transaction_id(), self, 0);
        let node_id = try_parse!(self.parse_node_id(), self, transaction_id);
        try_parse!(self.expect(";"), self, transaction_id);
        try_parse!(self.parse_end_of_message(), self, transaction_id);

        trace!("Close: user={}, node_id={}", self, node_id);

        let position = self.open_files.iter().position(| ref e | {
            e.0 == node_id
        }) ;

        if let Some(index) = position {
            let (_, _, _, mut access) = self.open_files.remove(index);

            match access.close() {
                Ok(()) => {
                    try_send_rsp!(
                        self,
                        transaction_id,
                        CommonErrorCodes::NoError as u32,
                        EMPTY
                    );
                    Ok(())
                },
                Err(error) => {
                    try_send_rsp!(
                        self,
                        transaction_id,
                        map_file_error_to_uint(error),
                        EMPTY
                    );
                    Err(())
                }
            }
        } else {
            try_send_rsp!(
                self,
                transaction_id,
                CommonErrorCodes::ErrorFileIsNotOpen as u32,
                EMPTY
            );
            Err(())
        }
    }

    fn handle_write_req(& mut self) -> Result<(), ()> {

        let transaction_id = try_parse!(self.parse_transaction_id(), self, 0);
        let node_id = try_parse!(self.parse_node_id(), self, transaction_id);
        let revision = try_parse!(self.parse_unsigned(), self, transaction_id);
        let offset = try_parse!(self.parse_unsigned(), self, transaction_id);
        let size = try_parse!(self.parse_unsigned(), self, transaction_id);
        try_parse!(self.expect(";"), self, transaction_id);
        try_parse!(self.parse_end_of_message(), self, transaction_id);

        trace!("Write: user={}, node_id={}, revision={}, offset={}, size={}",
               self, node_id, revision, offset, size);

        let (mode, _file_type, ref mut access) = match find_open_file(& mut self.open_files, & node_id) {
            Err(()) => {
                try_send_rsp!(
                    self,
                    transaction_id,
                    CommonErrorCodes::ErrorFileIsNotOpen as u32,
                    EMPTY
                );
                return Err(());
            },
            Ok(v) => v,
        };

        match *mode {
            OpenMode::Read => {
                try_send_rsp!(
                    self,
                    transaction_id,
                    CommonErrorCodes::ErrorFileOpenedInReadMode as u32,
                    EMPTY
                );
                return Err(());
            },
            OpenMode::ReadWrite => (),
        }

        let mut data = Buffer::with_capacity(size as usize);
        self.buffer.take(& mut data);
        if data.len() < data.capacity() {
            Client::fill_buffer(& mut self.connection, & mut data);
        }

        match access.write(revision, offset, data) {
            Ok(revision) => {
                try_send_rsp!(
                    self,
                    transaction_id,
                    CommonErrorCodes::NoError as u32,
                    & Client::revision_to_string(revision)
                );
                Ok(())
            },
            Err(error) => {
                try_send_rsp!(
                    self,
                    transaction_id,
                    map_file_error_to_uint(error),
                    EMPTY
                );
                Err(())
            }
        }
    }

    fn handle_insert_req(& mut self) -> Result<(), ()> {

        let transaction_id = try_parse!(self.parse_transaction_id(), self, 0);
        let node_id = try_parse!(self.parse_node_id(), self, transaction_id);
        let revision = try_parse!(self.parse_unsigned(), self, transaction_id);
        let offset = try_parse!(self.parse_unsigned(), self, transaction_id);
        let size = try_parse!(self.parse_unsigned(), self, transaction_id);
        try_parse!(self.expect(";"), self, transaction_id);
        try_parse!(self.parse_end_of_message(), self, transaction_id);

        trace!("Insert: user={}, node_id={}, revision={}, offset={}, size={}",
               self, node_id, revision, offset, size);

        let (mode, _file_type, ref mut access) = match find_open_file(& mut self.open_files, & node_id) {
            Err(()) => {
                try_send_rsp!(
                    self,
                    transaction_id,
                    CommonErrorCodes::ErrorFileIsNotOpen as u32,
                    EMPTY
                );
                return Err(());
            },
            Ok(v) => v,
        };

        match *mode {
            OpenMode::Read => {
                try_send_rsp!(
                    self,
                    transaction_id,
                    CommonErrorCodes::ErrorFileOpenedInReadMode as u32,
                    EMPTY
                );
                return Err(());
            },
            OpenMode::ReadWrite => (),
        }

        let mut data = Buffer::with_capacity(size as usize);
        self.buffer.take(& mut data);
        if data.len() < data.capacity() {
            Client::fill_buffer(& mut self.connection, & mut data);
        }

        match access.insert(revision, offset, data) {
            Ok(revision) => {
                try_send_rsp!(
                    self,
                    transaction_id,
                    CommonErrorCodes::NoError as u32,
                    & Client::revision_to_string(revision)
                );
                Ok(())
            },
            Err(error) => {
                try_send_rsp!(
                    self,
                    transaction_id,
                    map_file_error_to_uint(error),
                    EMPTY
                );
                Err(())
            }
        }
    }

    fn handle_delete_req(& mut self) -> Result<(), ()> {

        let transaction_id = try_parse!(self.parse_transaction_id(), self, 0);
        let node_id = try_parse!(self.parse_node_id(), self, transaction_id);
        let revision = try_parse!(self.parse_unsigned(), self, transaction_id);
        let offset = try_parse!(self.parse_unsigned(), self, transaction_id);
        let size = try_parse!(self.parse_unsigned(), self, transaction_id);
        try_parse!(self.expect(";"), self, transaction_id);
        try_parse!(self.parse_end_of_message(), self, transaction_id);

        trace!("Delete: user={}, node_id={}, revision={}, offset={}, size={}",
               self, node_id, revision, offset, size);

        let (mode, _file_type, ref mut access) = match find_open_file(& mut self.open_files, & node_id) {
            Err(()) => {
                try_send_rsp!(
                    self,
                    transaction_id,
                    CommonErrorCodes::ErrorFileIsNotOpen as u32,
                    EMPTY
                );
                return Err(());
            },
            Ok(v) => v,
        };

        match *mode {
            OpenMode::Read => {
                try_send_rsp!(
                    self,
                    transaction_id,
                    CommonErrorCodes::ErrorFileOpenedInReadMode as u32,
                    EMPTY
                );
                return Err(());
            },
            OpenMode::ReadWrite => (),
        }

        match access.delete(revision, offset, size) {
            Ok(revision) => {
                try_send_rsp!(
                    self,
                    transaction_id,
                    CommonErrorCodes::NoError as u32,
                    & Client::revision_to_string(revision)
                );
                Ok(())
            },
            Err(error) => {
                try_send_rsp!(
                    self,
                    transaction_id,
                    map_file_error_to_uint(error),
                    EMPTY
                );
                Err(())
            }
        }
    }

    fn handle_read_req(& mut self) -> Result<(), ()> {

        let transaction_id = try_parse!(self.parse_transaction_id(), self, 0);
        let node_id = try_parse!(self.parse_node_id(), self, transaction_id);
        let offset = try_parse!(self.parse_unsigned(), self, transaction_id);
        let size = try_parse!(self.parse_unsigned(), self, transaction_id);
        try_parse!(self.expect(";"), self, transaction_id);
        try_parse!(self.parse_end_of_message(), self, transaction_id);

        trace!("Read: user={}, node_id={}, offset={}, size={}", self, node_id, offset, size);

        let (_, _, ref mut access) = match find_open_file(& mut self.open_files, & node_id) {
            Err(()) => {
                try_send_rsp!(
                    self,
                    transaction_id,
                    CommonErrorCodes::ErrorFileIsNotOpen as u32,
                    EMPTY
                );
                return Err(());
            },
            Ok(v) => v,
        };

        match access.read(offset, size) {
            Ok((data, revision)) => {

                let mut fields = Client::revision_to_string(revision);
                fields.push_str(& Client::block_to_string(offset, data.len() as u64));
                try_send_rsp!(
                    self,
                    transaction_id,
                    CommonErrorCodes::NoError as u32,
                    & fields
                );

                if data.len() > 0 {
                    try_send!(self, self.connection.write_with_sleep(& data));
                }

                Ok(())
            },
            Err(error) => {
                try_send_rsp!(
                    self,
                    transaction_id,
                    map_file_error_to_uint(error),
                    EMPTY
                );
                Err(())
            }
        }
    }

    fn handle_delete_fs_element_req(& mut self) -> Result<(), ()> {

        let transaction_id = try_parse!(self.parse_transaction_id(), self, 0);
        let fd = try_parse!(self.parse_file_descriptor(), self, transaction_id);
        try_parse!(self.expect(";"), self, transaction_id);
        try_parse!(self.parse_end_of_message(), self, transaction_id);

        trace!("Delete: user={}, fd={}", self, fd);

        let user = self.user.as_ref().unwrap().clone();
        self.send_to_node(transaction_id, NodeProtocol::DeleteRequest {
            user: user,
            fd: fd,
        }) ? ;

        node_receive::<()>(
            self,
            & | msg, client | {
                match msg {
                    ClientProtocol::DeleteResponse {
                        result: Ok(()),
                    } => {
                        try_send_rsp!(
                            client,
                            transaction_id,
                            CommonErrorCodes::NoError as u32,
                            EMPTY
                        );
                        (None, Some(Ok(())))
                    },

                    ClientProtocol::CountersResponse {
                        result: Err(error),
                    } => {
                        try_send_rsp!(
                            client,
                            transaction_id,
                            map_node_error_to_uint(error),
                            EMPTY
                        );
                        (None, Some(Err(())))
                    },

                    other => {
                        (Some(other), None)
                    }
                }
            })
            ? ;

        Ok(())
    }

    fn handle_query_counters_req(& mut self) -> Result<(), ()> {
        let transaction_id = try_parse!(self.parse_transaction_id(), self, 0);
        try_parse!(self.expect(";"), self, transaction_id);
        try_parse!(self.parse_end_of_message(), self, transaction_id);

        trace!("Query counters: user={}", self);

        let user = self.user.as_ref().unwrap().clone();
        self.send_to_node(transaction_id, NodeProtocol::CountersRequest {
            user: user,
        }) ? ;

        node_receive::<()>(
            self,
            & | msg, client | {
                match msg {
                    ClientProtocol::CountersResponse {
                        result: Ok(counters),
                    } => {
                        let mut number_of_items = 0;
                        let mut data = String::with_capacity(1024 * 2);

                        data += & Client::list_element_to_string(
                            & Client::key_u32_value_to_string(
                                "active-connections",
                                counters.active_connections
                            ));
                        number_of_items += 1;

                        let formated_counters = Client::list_to_string(
                            number_of_items,
                            & data
                        );

                        try_send_rsp!(
                            client,
                            transaction_id,
                            CommonErrorCodes::NoError as u32,
                            & formated_counters
                        );
                        (None, Some(Ok(())))
                    },

                    ClientProtocol::CountersResponse {
                        result: Err(error),
                    } => {
                        try_send_rsp!(
                            client,
                            transaction_id,
                            map_node_error_to_uint(error),
                            EMPTY
                        );
                        (None, Some(Err(())))
                    },

                    other => {
                        (Some(other), None)
                    }
                }
            })
            ? ;

        Ok(())
    }

    fn handle_query_list_req(& mut self) -> Result<(), ()> {
        let transaction_id = try_parse!(self.parse_transaction_id(), self, 0);
        let fd = try_parse!(self.parse_file_descriptor(), self, 0);
        try_parse!(self.expect(";"), self, transaction_id);
        try_parse!(self.parse_end_of_message(), self, transaction_id);

        trace!("Query list: user={}, fd={}", self, fd);

        let user = self.user.as_ref().unwrap().clone();
        self.send_to_node(transaction_id, NodeProtocol::QueryListRequest {
            user: user,
            fd: fd,
        }) ? ;

        node_receive::<()>(
            self,
            & | msg, client | {
                match msg {
                    ClientProtocol::QueryListResponse {
                        result: Ok(list_of_elements),
                    } => {

                        let size = list_of_elements.len();
                        let mut desc = String::with_capacity(1024 * 2);

                        for (name, node_id, type_of) in list_of_elements.into_iter() {

                            let type_int = match type_of {
                                FilesystemElement::Folder => FOLDER,
                                FilesystemElement::File => FILE,
                            };

                            desc += & Client::list_element_to_string(
                                & format!("S:U:{};B:{};;N:U:{};;U:{};",
                                          name.len(), name, node_id, type_int)
                            );
                        }

                        try_send_rsp!(
                            client,
                            transaction_id,
                            CommonErrorCodes::NoError as u32,
                            & Client::list_to_string(size, & desc)
                        );
                        (None, Some(Ok(())))
                    },

                    ClientProtocol::QueryListResponse {
                        result: Err(error),
                    } => {
                        try_send_rsp!(
                            client,
                            transaction_id,
                            map_node_error_to_uint(error),
                            EMPTY
                        );
                        (None, Some(Err(())))
                    },

                    other => {
                        (Some(other), None)
                    }
                }
            })
            ? ;

        Ok(())
    }

    fn handle_query_fs_req(& mut self) -> Result<(), ()> {
        let transaction_id = try_parse!(self.parse_transaction_id(), self, 0);
        let fd = try_parse!(self.parse_file_descriptor(), self, 0);
        try_parse!(self.expect(";"), self, 0);
        try_parse!(self.parse_end_of_message(), self, transaction_id);

        trace!("Query fs: user={}, fd={}", self, fd);

        let user = self.user.as_ref().unwrap().clone();
        self.send_to_node(transaction_id, NodeProtocol::QueryFilesystemRequest {
            user: user,
            fd: fd,
        }) ? ;

        node_receive::<()>(
            self,
            & | msg, client | {
                match msg {
                    ClientProtocol::QueryFilesystemResponse {
                        result: Ok(desc),
                    } => {

                        let mut data = String::with_capacity(1024 * 4);
                        let type_int = match desc.element_type {
                            FilesystemElement::Folder => 1,
                            FilesystemElement::File => 0,
                        };

                        let mut number_of_items = 0;
                        data += & Client::list_element_to_string(
                            & Client::key_u32_value_to_string("type", type_int));
                        number_of_items += 1;
                        data += & Client::list_element_to_string(
                            & Client::key_u64_value_to_string("created", desc.created_at as u64));
                        number_of_items += 1;
                        data += & Client::list_element_to_string(
                            & Client::key_u64_value_to_string("modified", desc.modified_at as u64));
                        number_of_items += 1;
                        data += & Client::list_element_to_string(
                            & Client::key_string_value_to_string("read-access", & desc.read_access));
                        number_of_items += 1;
                        data += & Client::list_element_to_string(
                            & Client::key_string_value_to_string("write-access", & desc.write_access));
                        number_of_items += 1;

                        try_send_rsp!(
                            client,
                            transaction_id,
                            CommonErrorCodes::NoError as u32,
                            & Client::list_to_string(number_of_items, & data)
                        );
                        (None, Some(Ok(())))
                    },

                    ClientProtocol::QueryFilesystemResponse {
                        result: Err(error),
                    } => {
                        try_send_rsp!(
                            client,
                            transaction_id,
                            map_node_error_to_uint(error),
                            EMPTY
                        );
                        (None, Some(Err(())))
                    },

                    other => {
                        (Some(other), None)
                    }
                }
            })
            ? ;

        Ok(())
    }

    fn fill_buffer(connection: & mut Connection, buffer: & mut Buffer) {
        while buffer.len() != buffer.capacity() {
            if ! connection.read(buffer).unwrap() {
                sleep(Duration::from_millis(500));
            }
        }
    }

    fn send_response(
        connection: & mut Connection,
        transaction_id: u32,
        response_code: u32,
        additional_fields: & str,
    ) -> Result<(), ()> {
        let mut msg = Vec::from(format!("V:1;RSP:T:U:{};;U:{};;",
                                        transaction_id, response_code).as_bytes());
        msg.extend_from_slice(additional_fields.as_bytes());
        msg.extend_from_slice(& format!("E:;").as_bytes());
        connection.write_with_sleep(& msg)
            .map(| _: usize | ())
    }

    fn send_notification(
        connection: & mut Connection,
        notification: & str
    ) -> Result<(), ()> {
        let msg = Vec::from(format!("V:1;NOTIFICATION:;{}E:;", notification).as_bytes());
        connection.write_with_sleep(& msg)
            .map(| _: usize | ())
    }

    fn unsigned_to_string(value: u64) -> String {
        format!("U:{};", value)
    }

    fn node_id_to_string(node_id: NodeId) -> String {
        format!("N:U:{};;", node_id)
    }

    fn revision_to_string(revision: u64) -> String {
        format!("U:{};", revision)
    }

    fn block_to_string(offset: u64, size: u64) -> String {
        format!("BL:U:{};U:{};;", offset, size)
    }

    fn key_string_value_to_string(key: & str, value: & str) -> String {
        format!("KVP:S:U:{};B:{};;S:U:{};B:{};;;", key.len(), key, value.len(), value)
    }

    fn key_u64_value_to_string(key: & str, value: u64) -> String {
        format!("KVP:S:U:{};B:{};;U:{};;", key.len(), key, value)
    }

    fn key_u32_value_to_string(key: & str, value: u32) -> String {
        format!("KVP:S:U:{};B:{};;U:{};;", key.len(), key, value)
    }

    fn list_to_string(size: usize, content: & str) -> String {
        format!("L:U:{};{};", size, content)
    }

    fn list_element_to_string(content: & str) -> String {
        format!("LE:{};", content)
    }

    fn notification_closed(node_id: & NodeId) -> String {
        format!("F-CLOSED:N:U:{};;;", node_id)
    }

    fn notification_modified(node_id: & NodeId, revision: & u64, offset: & u64, size: & u64)
                            -> String {
        format!("PF-MOD:;N:U:{};;U:{};BL:U:{};U:{};;", node_id, revision, offset, size)
    }

    fn notification_inserted(node_id: & NodeId, revision: & u64, offset: & u64, size: & u64)
                            -> String {
        format!("PF-INS:;N:U:{};;U:{};BL:U:{};U:{};;", node_id, revision, offset, size)
    }

    fn notification_deleted(node_id: & NodeId, revision: & u64, offset: & u64, size: & u64)
                            -> String {
        format!("PF-DEL:;N:U:{};;U:{};BL:U:{};U:{};;", node_id, revision, offset, size)
    }

    fn notification_disconnected(description: & str) -> String {
        format!("DISCONNECTED:;S:U:{};B:{};;", description.len(), description)
    }

    fn expect(& mut self, expected: & str) -> Result<(), ()> {

        let expected_bytes = expected.as_bytes();
        {
            if self.buffer.get_buffer_length() < expected_bytes.len() {
                return Err(())
            }

            let elements = self.buffer.get_buffer_with_length(expected_bytes.len());
            if expected_bytes != elements {
                return Err(())
            }
        }
        self.buffer.buffer_index += expected_bytes.len();
        return Ok(())
    }

    fn parse_end_of_message(& mut self) -> Result<(), ()> {
        self.expect(FIELD_END_MARKER)
    }

    fn parse_transaction_id(& mut self) -> Result<u32, ()> {
        self.expect("T:") ? ;
        let id = self.parse_unsigned() ? ;
        self.expect(";") ? ;
        Ok(id as u32)
    }

    fn parse_node_id(& mut self) -> Result<NodeId, ()> {
        self.expect("N:") ? ;
        let id = self.parse_unsigned() ? ;
        self.expect(";") ? ;
        Ok(id as NodeId)
    }

    fn parse_file_descriptor(& mut self) -> Result<FileDescriptor, ()> {
        self.expect("F:") ? ;
        let result_path = self.parse_path();
        let desc = {
            if result_path.is_ok() {
                let fd = FileDescriptor::from_path(result_path.unwrap()) ? ;
                fd
            } else {
                let id = self.parse_node_id() ? ;
                let fd = FileDescriptor::from_node_id(id as u64) ? ;
                fd
            }
        };
        self.expect(";") ? ;
        Ok(desc)
    }

    fn parse_path(& mut self) -> Result<PathBuf, ()> {
        self.expect("P:") ? ;
        let string = self.parse_string() ? ;
        self.expect(";") ? ;

        let path = PathBuf::from(string);
        if ! path.is_absolute() {
            return Err(())
        }
        Ok(path)
    }

    fn parse_string(& mut self) -> Result<String, ()> {
        self.expect("S:") ? ;
        let length = self.parse_unsigned() ? ;
        self.expect("B:") ? ;
        let value: String;
        {
            value = String::from_utf8_lossy(& self.buffer.get_buffer()[0 .. length as usize])
                .into_owned();
        }
        self.buffer.buffer_index += length as usize;
        self.expect(";") ? ;
        self.expect(";") ? ;
        Ok(value)
    }

    fn parse_unsigned(& mut self) -> Result<u64, ()> {

        self.expect("U:") ? ;
        let mut value: Option<u64> = None;
        let mut size: usize = 0;
        {
            let limiter = ";".as_bytes();
            let limiter_length = limiter.len();
            let buffer = self.buffer.get_buffer();
            let buffer_length = buffer.len();

            let mut i = 0;
            while i < buffer_length {
                if limiter == & buffer[i .. i + limiter_length] {
                    match String::from_utf8_lossy(& buffer[0 .. i])
                        .parse::<u64>() {
                        Ok(v) => {
                            value = Some(v);
                            size = i + limiter_length;
                        }
                        _ => (),
                    };
                    break;
                }
                i += limiter_length;
            }
        }
        match value {
            Some(value) => {
                self.buffer.buffer_index += size;
                Ok(value)
            },
            None => {
                Err(())
            }
        }
    }
}

fn find_open_file<'vec>(open_files: &'vec mut Vec<(NodeId, OpenMode, FileType, FileAccess,)>, node_id: & NodeId)
                  -> Result<(&'vec OpenMode, &'vec FileType, &'vec mut FileAccess), ()> {

    let mut index: usize = 0;
    open_files.iter().enumerate().find(| & (ref i, ref e) | {
        index = i.clone();
        e.0 == *node_id
    })
        .ok_or(())
        ? ;

    open_files.get_mut(index)
        .ok_or(())
        .and_then(| & mut (_, ref mode, ref file_type, ref mut access) | {
            Ok((mode, file_type, access))
        })
}

static MAX_WAIT_DURATION_FOR_NODE_RESPONSE_MS: u64 = 1000;
static MAX_NUMBER_OF_MESSAGES_FROM_NODE: u64 = 5;

fn node_receive<OkType>(
    client: & mut Client,
    handler: & Fn(ClientProtocol, & mut Client) -> (Option<ClientProtocol>, Option<Result<OkType, ()>>)
) -> Result<OkType, ()> {

    let sleep_duration = MAX_WAIT_DURATION_FOR_NODE_RESPONSE_MS / MAX_NUMBER_OF_MESSAGES_FROM_NODE;
    for _ in 0..MAX_NUMBER_OF_MESSAGES_FROM_NODE {

        let msg = match client.node_receive.try_recv() {
            Ok(msg) => msg,
            Err(TryRecvError::Disconnected) => {
                client.status = Status::FailedToreceiveFromNode;
                return Err(())
            },
            Err(TryRecvError::Empty) => {
                sleep(Duration::from_millis(sleep_duration));
                continue;
            },
        };

        match handler(msg, client) {
            (None, Some(result)) => return result,
            (Some(msg), None) => {
                client.handle_unexpected_message_from_node(msg);
            },
            (_, _) => panic!()
        };
    }

    warn!("No response received from node in time, client={}", client);
    client.status = Status::FailedToreceiveFromNode;
    Err(())
}

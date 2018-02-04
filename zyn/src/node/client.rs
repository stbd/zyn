use std::fmt::{ Display, Formatter, Result as FmtResult };
use std::result::{ Result };
use std::sync::mpsc::{ Receiver, Sender, TryRecvError };
use std::thread::{ sleep };
use std::time::{ Duration };
use std::vec::{ Vec };
use std::{ str };

use node::client_protocol_buffer::{ ReceiveBuffer, SendBuffer, KeyValueMap2, Value2 };
use node::common::{ NodeId, Buffer, OpenMode, FileType };
use node::connection::{ Connection };
use node::file_handle::{ FileAccess, FileError, Notification, FileLock };
use node::filesystem::{ FilesystemError };
use node::node::{ ClientProtocol, NodeProtocol, FilesystemElement, ErrorResponse, NodeError };
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

List-element-of-TYPE
LE:[TYPE: element];

List-of-TYPE:
L:[uint: list length][List-element-of-TYPE]...;

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
    OperationNotPermitedFotFileType = 5,
    BlockSizeIsTooLarge = 6,
}

fn map_node_error_to_uint(error: ErrorResponse) -> u64 {
    match error {
        ErrorResponse::NodeError { error } => {
            match error {
                NodeError::InvalidUsernamePassword => 100,
                NodeError::ParentIsNotFolder => 101,
                NodeError::UnauthorizedOperation => 102,
                NodeError::InternalCommunicationError => 103,
                NodeError::InternalError => 104,
                NodeError::UnknownFile => 105,
                NodeError::UnknownAuthority => 106,
                NodeError::AuthorityError => 107,

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

fn map_file_error_to_uint(result: FileError) -> u64 {
    match result {
        FileError::InternalCommunicationError => 300,
        FileError::InternalError => 301,
        FileError::RevisionTooOld => 302,
        FileError::OffsetAndSizeDoNotMapToPartOfFile => 303,
        FileError::DeleteIsonlyAllowedForLastPart => 304,
        FileError::FileLockedByOtherUser => 305,
        FileError::FileNotLocked => 306,
    }
}

const READ: u64 = 0;
const READ_WRITE: u64 = 1;
const FILE: u64 = 0;
const FOLDER: u64 = 1;
const FILE_TYPE_RANDOM_ACCESS: u64 = 0;
const FILE_TYPE_BLOB: u64 = 1;
const TYPE_USER: u64 = 0;
const TYPE_GROUP: u64 = 1;

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

/*
Configure: Add user/group:
<- [Version]ADD-USER-GROUP:[Transaction Id][Uint: type][String: name];[End]
 * type: 0: user, 1: group
-> [Version]RSP:[Transaction Id][Uint: error code];[End]
*/

/*
Configure: Modify user/group:
<- [Version]MOD-USER-GROUP:[Transaction Id][uint: type][String: name][Key-value-list];[End]
 * type: 0: user, 1: group
-> [Version]RSP:[Transaction Id][Uint: error code];[End]
*/

// todo

/*
Configure: Remove user/group:
<- [Version]REMOVE-USER-GROUP:[Transaction Id][Uint: type][String: name];[End]
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
Configure: Set file/folder rights:
<- [Version]F:[Transaction Id][FileDescriptor][Key-value-list];[End]
-> [Version]RSP:[Transaction Id][Uint: error code];[End]
*/

const DEFAULT_BUFFER_SIZE: usize = 1024 * 4;

macro_rules! try_parse {
    ($result:expr, $class:expr, $transaction_id:expr) => {{
        match $result {
            Ok(value) => value,
            Err(_) => {
                match Client::send_response_without_fields(
                    & mut $class.connection,
                    0,
                    CommonErrorCodes::ErrorMalformedMessage as u64)
                {
                    Ok(()) => (),
                    Err(status) => {
                        $class.status = status;
                    },
                }
                return Err(());
            }
        }
    }}
}

macro_rules! try_with_set_error_state {
    ($class:expr, $operation:expr, $error_code:expr) => {{
        let result = $operation;
        if result.is_err() {
            $class.status = $error_code;
            return Err(());
        }
        result.unwrap()
    }}
}

macro_rules! try_write_buffer {
    ($class:expr, $operation:expr) => {{
        let result = $operation;
        try_with_set_error_state!($class, result, Status::FailedToWriteToSendBuffer)
    }}
}

macro_rules! try_send_response_without_fields {
    ($class:expr, $transaction_id:expr, $rsp_error_code:expr) => {{
        match Client::send_response_without_fields(& mut $class.connection, $transaction_id, $rsp_error_code) {
            Ok(()) => (),
            Err(status) => {
                $class.status = status;
                return Err(())
            }
        }
    }}
}

macro_rules! try_in_receive_loop {
    ($class:expr, $operation:expr, $error_code:expr) => {{
        let result = $operation;
        if result.is_err() {
            $class.status = $error_code;
            return (None, Some(Err(())));
        }
        result.unwrap()
    }}
}

macro_rules! try_in_receive_loop_to_create_buffer {
    ($class:expr, $transaction_id:expr, $rsp_error_code:expr) => {{
        try_in_receive_loop!(
            $class,
            Client::create_response_buffer(
                $transaction_id as u64,
                $rsp_error_code as u64
            ),
            Status::FailedToWriteToSendBuffer
        )
    }}
}

macro_rules! try_in_receive_loop_to_send_response_without_fields {
    ($class:expr, $transaction_id:expr, $rsp_error_code:expr) => {{
        match Client::send_response_without_fields(& mut $class.connection, $transaction_id, $rsp_error_code) {
            Ok(()) => (),
            Err(status) => {
                $class.status = status;
                return (None, Some(Err(())));
            }
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
    FailedToWriteToSendBuffer,
    ShutdownOrderedByNode,
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
            Status::FailedToWriteToSendBuffer =>
                write!(f, "FailedToWriteToSendBuffer"),
            Status::ShutdownOrderedByNode =>
                write!(f, "ShutdownOrderedByNode"),
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

struct OpenFile {
    node_id: NodeId,
    open_mode: OpenMode,
    file_type: FileType,
    access: FileAccess,
}

pub struct Client {
    connection: Connection,
    buffer: ReceiveBuffer,
    node_receive: Receiver<ClientProtocol>,
    node_send: Sender<NodeProtocol>,
    open_files: Vec<OpenFile>,
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
            buffer: ReceiveBuffer::with_capacity(DEFAULT_BUFFER_SIZE),
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

            let mut is_processing: bool = false;

            if self.handle_notifications_from_open_files()
                .and_then(| number_of_processed_notification | {
                    if number_of_processed_notification > 0 {
                        is_processing = true;
                    }
                    Ok(())
                })
                .is_err() {
                    error!("Error while processing notification from files");
                    break ;
                }

            if self.handle_messages_from_node()
                .is_err() {
                    error!("Error while processing notification from node");
                    break ;
                }

            if self.status.is_in_error_state() {
                break ;
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

            if ! self.buffer.is_complete_message() {
                continue ;
            }

            self.buffer.debug_buffer();

            if self.buffer.expect("V:1;").is_err() {
                match Client::send_response_without_fields(& mut self.connection, 0, CommonErrorCodes::ErrorMalformedMessage as u64) {
                    Ok(()) => (),
                    Err(status) => {
                        self.status = status;
                        break;
                    }
                }
                // todo: set status
                continue ;
            }

            if self.buffer.expect("A:").is_ok() {
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

                if self.buffer.expect("CREATE-FILE:").is_ok() {
                    let _ = self.handle_create_file_req();
                } else if self.buffer.expect("CREATE-FOLDER:").is_ok() {
                    let _ = self.handle_create_folder_req();
                } else if self.buffer.expect("O:").is_ok() {
                    let _ = self.handle_open_req();
                } else if self.buffer.expect("CLOSE:").is_ok() {
                    let _ = self.handle_close_req();
                } else if self.buffer.expect("RA-W:").is_ok() {
                    let _ = self.handle_write_random_access_req();
                } else if self.buffer.expect("RA-I:").is_ok() {
                    let _ = self.handle_random_access_insert_req();
                } else if self.buffer.expect("RA-D:").is_ok() {
                    let _ = self.handle_random_access_delete_req();
                } else if self.buffer.expect("BLOB-W:").is_ok() {
                    let _ = self.handle_blob_write_req();
                } else if self.buffer.expect("R:").is_ok() {
                    let _ = self.handle_read_req();
                } else if self.buffer.expect("DELETE:").is_ok() {
                    let _ = self.handle_delete_fs_element_req();
                } else if self.buffer.expect("Q-COUNTERS:").is_ok() {
                    let _ = self.handle_query_counters_req();
                } else if self.buffer.expect("Q-LIST:").is_ok() {
                    let _ = self.handle_query_list_req();
                } else if self.buffer.expect("Q-FILESYSTEM:").is_ok() {
                    let _ = self.handle_query_fs_req();
                } else if self.buffer.expect("ADD-USER-GROUP:").is_ok() {
                    let _ = self.handle_add_user_group();
                } else if self.buffer.expect("MOD-USER-GROUP:").is_ok() {
                    let _ = self.handle_mod_user_group();
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

    fn handle_notifications_from_open_files(& mut self) -> Result<usize, ()> {

        let mut number_of_processed_notifications: usize = 0;
        let mut remove: Option<usize> = None;

        for (index, ref mut open_file) in self.open_files
            .iter_mut()
            .enumerate() {

                loop {
                    let mut buffer = try_write_buffer!(self, Client::create_notification_buffer());

                    match open_file.access.pop_notification() {
                        Some(Notification::FileClosing {  }) => {
                            try_write_buffer!(self, buffer.write_notification_closed(& open_file.node_id));
                            remove = Some(index);
                        },
                        Some(Notification::PartOfFileModified { revision, offset, size }) => {
                            try_write_buffer!(self, buffer.write_notification_modified(& open_file.node_id, & revision, & offset, & size));
                        },
                        Some(Notification::PartOfFileInserted { revision, offset, size }) => {
                            try_write_buffer!(self, buffer.write_notification_inserted(& open_file.node_id, & revision, & offset, & size));
                        },
                        Some(Notification::PartOfFileDeleted { revision, offset, size }) => {
                            try_write_buffer!(self, buffer.write_notification_deleted(& open_file.node_id, & revision, & offset, & size));
                        },
                        None => break,
                    }

                    if self.connection.write_with_sleep(buffer.as_bytes()).is_err() {
                        self.status = Status::FailedToSendToClient;
                        return Err(());
                    }

                    number_of_processed_notifications += 1;

                    if remove.is_some() {
                        break;
                    }
                }

                if remove.is_some() {
                    break ;
                }
            }

        if let Some(ref index) = remove {
            self.open_files.remove(*index);
        }

        Ok(number_of_processed_notifications)
    }

    fn handle_messages_from_node(& mut self) -> Result<(), ()> {

        match self.receive_from_node() {
            Err(()) => {
                error!("Error receiving from node");
                // todo: send notification
                return Err(());
            }

            Ok(None) => (),
            Ok(Some(message)) => {

                match message {
                    ClientProtocol::Shutdown { reason } => {

                        debug!("Received shutdown order from node");

                        for mut open_file in self.open_files.drain(..) {
                            let _ = open_file.access.close();
                        }

                        let mut buffer = try_write_buffer!(self, Client::create_notification_buffer());
                        try_write_buffer!(self, buffer.write_notification_disconnected(& reason));

                        if self.connection.write_with_sleep(buffer.as_bytes()).is_err() {
                            self.status = Status::FailedToSendToClient;
                            return Err(());
                        }

                        self.status = Status::ShutdownOrderedByNode;
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

        Ok(())
    }

    fn is_authenticated(& self) -> bool {
        self.user.is_some()
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

    fn create_response_buffer(transaction_id: u64, error_code: u64) -> Result<SendBuffer, ()> {
        let mut buffer = SendBuffer::with_capacity(1024);
        buffer.write_message_namespace(1) ? ;
        buffer.write_response(transaction_id, error_code) ? ;
        Ok(buffer)
    }

    fn create_notification_buffer() -> Result<SendBuffer, ()> {
        let mut buffer = SendBuffer::with_capacity(1024);
        buffer.write_message_namespace(1) ? ;
        Ok(buffer)
    }

    fn send_response_without_fields(connection: & mut Connection, transaction_id: u64, error_code: u64) -> Result<(), Status> {
        let mut buffer = Client::create_response_buffer(transaction_id, error_code)
            .map_err(| () | Status::FailedToWriteToSendBuffer)
            ? ;

        buffer.write_end_of_message()
            .map_err(| () | Status::FailedToWriteToSendBuffer)
            ? ;

        connection.write_with_sleep(buffer.as_bytes())
            .map_err(| () | Status::FailedToSendToClient)
            .map(| _: usize | () )
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

    fn send_to_node(& mut self, transaction_id: u64, msg: NodeProtocol) -> Result<(), ()> {
        if let Err(desc) = self.node_send.send(msg) {
            warn!("Failed to send message to node, id={}, desc={}", self, desc);
            try_send_response_without_fields!(self, transaction_id, CommonErrorCodes::InternalCommunicationError as u64);
            self.status = Status::FailedToSendToNode;
            return Err(());
        }
        Ok(())
    }

    fn fill_buffer(connection: & mut Connection, buffer: & mut Buffer) {
        while buffer.len() != buffer.capacity() {
            if ! connection.read(buffer).unwrap() {
                sleep(Duration::from_millis(500));
            }
        }
    }

    fn handle_authentication_req(& mut self) -> Result<(), ()> {

        let transaction_id = try_parse!(self.buffer.parse_transaction_id(), self, 0);
        let username = try_parse!(self.buffer.parse_string(), self, transaction_id);
        let password = try_parse!(self.buffer.parse_string(), self, transaction_id);
        try_parse!(self.buffer.expect(";"), self, transaction_id);
        try_parse!(self.buffer.parse_end_of_message(), self, transaction_id);

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
                        try_in_receive_loop_to_send_response_without_fields!(client, transaction_id, CommonErrorCodes::NoError as u64);
                        (None, Some(Ok(id)))
                    },

                    ClientProtocol::AuthenticateResponse { result: Err(error) } => {
                        try_in_receive_loop_to_send_response_without_fields!(client, transaction_id, map_node_error_to_uint(error));
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

        let transaction_id = try_parse!(self.buffer.parse_transaction_id(), self, 0);
        let parent = try_parse!(self.buffer.parse_file_descriptor(), self, transaction_id);
        let name = try_parse!(self.buffer.parse_string(), self, transaction_id);
        let type_uint = try_parse!(self.buffer.parse_unsigned(), self, transaction_id);
        try_parse!(self.buffer.expect(";"), self, transaction_id);
        try_parse!(self.buffer.parse_end_of_message(), self, transaction_id);

        let type_enum = match type_uint {
            FILE_TYPE_RANDOM_ACCESS => FileType::RandomAccess,
            FILE_TYPE_BLOB => FileType::Blob,
            _ => {
                try_send_response_without_fields!(self, transaction_id, CommonErrorCodes::ErrorMalformedMessage as u64);
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
                        let mut buffer = try_in_receive_loop_to_create_buffer!(client, transaction_id, CommonErrorCodes::NoError);
                        try_in_receive_loop!(client, buffer.write_node_id(node_id), Status::FailedToWriteToSendBuffer);
                        try_in_receive_loop!(client, buffer.write_end_of_message(), Status::FailedToWriteToSendBuffer);
                        try_in_receive_loop!(client, client.connection.write_with_sleep(buffer.as_bytes()), Status::FailedToSendToClient);
                        (None, Some(Ok(())))
                    },

                    ClientProtocol::CreateFilesystemElementResponse {
                        result: Err(error),
                    } => {
                        try_in_receive_loop_to_send_response_without_fields!(client, transaction_id, map_node_error_to_uint(error));
                        (None, Some(Err(())))
                    },

                    other => {
                        (Some(other), None)
                    }
                }
            })
    }

    fn handle_create_folder_req(& mut self) -> Result<(), ()> {

        let transaction_id = try_parse!(self.buffer.parse_transaction_id(), self, 0);
        let parent = try_parse!(self.buffer.parse_file_descriptor(), self, transaction_id);
        let name = try_parse!(self.buffer.parse_string(), self, transaction_id);
        try_parse!(self.buffer.expect(";"), self, transaction_id);
        try_parse!(self.buffer.parse_end_of_message(), self, transaction_id);

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
                        let mut buffer = try_in_receive_loop_to_create_buffer!(client, transaction_id, CommonErrorCodes::NoError);
                        try_in_receive_loop!(client, buffer.write_node_id(node_id), Status::FailedToWriteToSendBuffer);
                        try_in_receive_loop!(client, buffer.write_end_of_message(), Status::FailedToWriteToSendBuffer);
                        try_in_receive_loop!(client, client.connection.write_with_sleep(buffer.as_bytes()), Status::FailedToSendToClient);
                        (None, Some(Ok(())))
                    },

                    ClientProtocol::CreateFilesystemElementResponse {
                        result: Err(error),
                    } => {
                        try_in_receive_loop_to_send_response_without_fields!(client, transaction_id, map_node_error_to_uint(error));
                        (None, Some(Err(())))
                    },

                    other => {
                        (Some(other), None)
                    }
                }
            })
    }

    fn handle_open_req(& mut self) -> Result<(), ()> {

        let transaction_id = try_parse!(self.buffer.parse_transaction_id(), self, 0);
        let fd = try_parse!(self.buffer.parse_file_descriptor(), self, transaction_id);
        let mode_uint = try_parse!(self.buffer.parse_unsigned(), self, transaction_id);
        try_parse!(self.buffer.expect(";"), self, transaction_id);
        try_parse!(self.buffer.parse_end_of_message(), self, transaction_id);

        let mode = match mode_uint {
            READ => OpenMode::Read,
            READ_WRITE => OpenMode::ReadWrite,
            _ => {
                try_send_response_without_fields!(self, transaction_id, CommonErrorCodes::ErrorMalformedMessage as u64);
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
                        let mut buffer = try_in_receive_loop_to_create_buffer!(client, transaction_id, CommonErrorCodes::NoError);
                        try_in_receive_loop!(client, buffer.write_node_id(node_id), Status::FailedToWriteToSendBuffer);
                        try_in_receive_loop!(client, buffer.write_unsigned(revision), Status::FailedToWriteToSendBuffer);
                        try_in_receive_loop!(client, buffer.write_unsigned(size), Status::FailedToWriteToSendBuffer);

                        try_in_receive_loop!(
                            client,
                            buffer.write_unsigned(
                                match file_type {
                                    FileType::RandomAccess => FILE_TYPE_RANDOM_ACCESS,
                                    FileType::Blob => FILE_TYPE_BLOB,
                                }),
                            Status::FailedToWriteToSendBuffer
                        );

                        try_in_receive_loop!(client, buffer.write_end_of_message(), Status::FailedToWriteToSendBuffer);
                        try_in_receive_loop!(client, client.connection.write_with_sleep(buffer.as_bytes()), Status::FailedToSendToClient);
                        (None, Some(Ok((access, node_id, file_type))))
                    },

                    ClientProtocol::OpenFileResponse {
                        result: Err(error),
                    } => {
                        try_in_receive_loop_to_send_response_without_fields!(client, transaction_id, map_node_error_to_uint(error));
                        (None, Some(Err(())))
                    },

                    other => {
                        (Some(other), None)
                    }
                }
            })
            ? ;

        self.open_files.push(OpenFile {
            node_id: node_id,
            open_mode: mode,
            file_type: file_type,
            access: access
        });
        Ok(())
    }

    fn handle_close_req(& mut self) -> Result<(), ()> {

        let transaction_id = try_parse!(self.buffer.parse_transaction_id(), self, 0);
        let node_id = try_parse!(self.buffer.parse_node_id(), self, transaction_id);
        try_parse!(self.buffer.expect(";"), self, transaction_id);
        try_parse!(self.buffer.parse_end_of_message(), self, transaction_id);

        trace!("Close: user={}, node_id={}", self, node_id);

        let position = self.open_files.iter().position(| ref open_file | {
            open_file.node_id == node_id
        }) ;

        if let Some(index) = position {
            let mut open_file = self.open_files.remove(index);

            match open_file.access.close() {
                Ok(()) => {
                    try_send_response_without_fields!(self, transaction_id, CommonErrorCodes::NoError as u64);
                    Ok(())
                },
                Err(error) => {
                    try_send_response_without_fields!(self, transaction_id, map_file_error_to_uint(error));
                    Err(())
                }
            }
        } else {
            try_send_response_without_fields!(self, transaction_id, CommonErrorCodes::ErrorFileIsNotOpen as u64);
            Err(())
        }
    }

    fn handle_write_random_access_req(& mut self) -> Result<(), ()> {

        let transaction_id = try_parse!(self.buffer.parse_transaction_id(), self, 0);
        let node_id = try_parse!(self.buffer.parse_node_id(), self, transaction_id);
        let revision = try_parse!(self.buffer.parse_unsigned(), self, transaction_id);
        let offset = try_parse!(self.buffer.parse_unsigned(), self, transaction_id);
        let size = try_parse!(self.buffer.parse_unsigned(), self, transaction_id);
        try_parse!(self.buffer.expect(";"), self, transaction_id);
        try_parse!(self.buffer.parse_end_of_message(), self, transaction_id);

        trace!("Write: user={}, node_id={}, revision={}, offset={}, size={}",
               self, node_id, revision, offset, size);

        let ref mut open_file = match find_open_file(& mut self.open_files, & node_id) {
            Err(()) => {
                try_send_response_without_fields!(self, transaction_id, CommonErrorCodes::ErrorFileIsNotOpen as u64);
                return Err(());
            },
            Ok(v) => v,
        };

        match open_file.open_mode {
            OpenMode::Read => {
                try_send_response_without_fields!(self, transaction_id, CommonErrorCodes::ErrorFileOpenedInReadMode as u64);
                return Err(());
            },
            OpenMode::ReadWrite => (),
        }

        let mut data = Buffer::with_capacity(size as usize);
        self.buffer.take(& mut data);
        if data.len() < data.capacity() {
            Client::fill_buffer(& mut self.connection, & mut data);
        }

        match open_file.access.write(revision, offset, data) {
            Ok(revision) => {
                let mut buffer = try_write_buffer!(self, Client::create_response_buffer(transaction_id, CommonErrorCodes::NoError as u64));
                try_write_buffer!(self, buffer.write_unsigned(revision));
                try_write_buffer!(self, buffer.write_end_of_message());
                try_with_set_error_state!(self, self.connection.write_with_sleep(buffer.as_bytes()), Status::FailedToSendToClient);
                Ok(())
            },
            Err(error) => {
                try_send_response_without_fields!(self, transaction_id, map_file_error_to_uint(error));
                Err(())
            }
        }
    }

    fn handle_random_access_insert_req(& mut self) -> Result<(), ()> {

        let transaction_id = try_parse!(self.buffer.parse_transaction_id(), self, 0);
        let node_id = try_parse!(self.buffer.parse_node_id(), self, transaction_id);
        let revision = try_parse!(self.buffer.parse_unsigned(), self, transaction_id);
        let offset = try_parse!(self.buffer.parse_unsigned(), self, transaction_id);
        let size = try_parse!(self.buffer.parse_unsigned(), self, transaction_id);
        try_parse!(self.buffer.expect(";"), self, transaction_id);
        try_parse!(self.buffer.parse_end_of_message(), self, transaction_id);

        trace!("Insert: user={}, node_id={}, revision={}, offset={}, size={}",
               self, node_id, revision, offset, size);

        let ref mut open_file = match find_open_file(& mut self.open_files, & node_id) {
            Err(()) => {
                try_send_response_without_fields!(self, transaction_id, CommonErrorCodes::ErrorFileIsNotOpen as u64);
                return Err(());
            },
            Ok(v) => v,
        };

        match open_file.open_mode {
            OpenMode::Read => {
                try_send_response_without_fields!(self, transaction_id, CommonErrorCodes::ErrorFileOpenedInReadMode as u64);
                return Err(());
            },
            OpenMode::ReadWrite => (),
        }

        let mut data = Buffer::with_capacity(size as usize);
        self.buffer.take(& mut data);
        if data.len() < data.capacity() {
            Client::fill_buffer(& mut self.connection, & mut data);
        }

        match open_file.access.insert(revision, offset, data) {
            Ok(revision) => {
                let mut buffer = try_write_buffer!(self, Client::create_response_buffer(transaction_id, CommonErrorCodes::NoError as u64));
                try_write_buffer!(self, buffer.write_unsigned(revision));
                try_write_buffer!(self, buffer.write_end_of_message());
                try_with_set_error_state!(self, self.connection.write_with_sleep(buffer.as_bytes()), Status::FailedToSendToClient);
                Ok(())
            },
            Err(error) => {
                try_send_response_without_fields!(self, transaction_id, map_file_error_to_uint(error));
                Err(())
            }
        }
    }

    fn handle_random_access_delete_req(& mut self) -> Result<(), ()> {

        let transaction_id = try_parse!(self.buffer.parse_transaction_id(), self, 0);
        let node_id = try_parse!(self.buffer.parse_node_id(), self, transaction_id);
        let revision = try_parse!(self.buffer.parse_unsigned(), self, transaction_id);
        let offset = try_parse!(self.buffer.parse_unsigned(), self, transaction_id);
        let size = try_parse!(self.buffer.parse_unsigned(), self, transaction_id);
        try_parse!(self.buffer.expect(";"), self, transaction_id);
        try_parse!(self.buffer.parse_end_of_message(), self, transaction_id);

        trace!("Delete: user={}, node_id={}, revision={}, offset={}, size={}",
               self, node_id, revision, offset, size);

        let ref mut open_file = match find_open_file(& mut self.open_files, & node_id) {
            Err(()) => {
                try_send_response_without_fields!(self, transaction_id, CommonErrorCodes::ErrorFileIsNotOpen as u64);
                return Err(());
            },
            Ok(v) => v,
        };

        match open_file.open_mode {
            OpenMode::Read => {
                try_send_response_without_fields!(self, transaction_id, CommonErrorCodes::ErrorFileOpenedInReadMode as u64);
                return Err(());
            },
            OpenMode::ReadWrite => (),
        }

        match open_file.access.delete(revision, offset, size) {
            Ok(revision) => {
                let mut buffer = try_write_buffer!(self, Client::create_response_buffer(transaction_id, CommonErrorCodes::NoError as u64));
                try_write_buffer!(self, buffer.write_unsigned(revision));
                try_write_buffer!(self, buffer.write_end_of_message());
                try_with_set_error_state!(self, self.connection.write_with_sleep(buffer.as_bytes()), Status::FailedToSendToClient);
                Ok(())
            },
            Err(error) => {
                try_send_response_without_fields!(self, transaction_id, map_file_error_to_uint(error));
                Err(())
            }
        }
    }

    fn handle_blob_write_req(& mut self) -> Result<(), ()> {

        let transaction_id = try_parse!(self.buffer.parse_transaction_id(), self, 0);
        let node_id = try_parse!(self.buffer.parse_node_id(), self, transaction_id);
        let mut revision = try_parse!(self.buffer.parse_unsigned(), self, transaction_id);
        let size = try_parse!(self.buffer.parse_unsigned(), self, transaction_id);
        let block_size = try_parse!(self.buffer.parse_unsigned(), self, transaction_id);

        try_parse!(self.buffer.expect(";"), self, transaction_id);
        try_parse!(self.buffer.parse_end_of_message(), self, transaction_id);

        trace!("Write blob: user={}, node_id={}, revision={}, size={}, block_size={}",
               self, node_id, revision, size, block_size);

        let ref mut open_file = match find_open_file(& mut self.open_files, & node_id) {
            Err(()) => {
                try_send_response_without_fields!(self, transaction_id, CommonErrorCodes::ErrorFileIsNotOpen as u64);
                return Err(());
            },
            Ok(v) => v,
        };

        match open_file.file_type {
            FileType::Blob => (),
            _ => {
                try_send_response_without_fields!(self, transaction_id, CommonErrorCodes::OperationNotPermitedFotFileType as u64);
                return Err(());
            },
        };

        match open_file.open_mode {
            OpenMode::Read => {
                try_send_response_without_fields!(self, transaction_id, CommonErrorCodes::ErrorFileOpenedInReadMode as u64);
                return Err(());
            },
            OpenMode::ReadWrite => (),
        }

        let user = self.user.as_ref().unwrap();
        let lock = FileLock::LockedBySystemForBlobWrite {
            user: user.clone(),
        };

        if let Err(error) = open_file.access.lock(revision, & lock) {
            try_send_response_without_fields!(self, transaction_id, map_file_error_to_uint(error));
            return Err(());
        }

        let max_block_size: u64 = 1024 * 1024 * 10;
        let mut bytes_read: u64 = 0;

        if block_size > max_block_size {
            try_send_response_without_fields!(self, transaction_id, CommonErrorCodes::BlockSizeIsTooLarge as u64);

            // todo: this is error case, but since file is locked, return is not possible
        }

        while bytes_read < size {

            let bytes_left = size - bytes_read;
            let buffer_size = {
                if bytes_left < max_block_size {
                    bytes_left
                } else {
                    max_block_size
                }
            };

            let mut buffer = Buffer::with_capacity(buffer_size as usize);
            self.buffer.take(& mut buffer);
            if buffer.len() < buffer.capacity() {
                Client::fill_buffer(& mut self.connection, & mut buffer);
            }

            revision = match open_file.access.write(revision, bytes_read, buffer) {
                Ok(r) => r,
                Err(error) => {
                    try_send_response_without_fields!(self, transaction_id, map_file_error_to_uint(error));
                    return Err(());
                }
            };

            bytes_read += buffer_size;
        }

        if let Err(error) = open_file.access.unlock(& lock) {
            error!("Failed to unlock file, user={}, error={}", user, map_file_error_to_uint(error));
            return Err(());
        }

        let mut buffer = try_write_buffer!(self, Client::create_response_buffer(transaction_id, CommonErrorCodes::NoError as u64));
        try_write_buffer!(self, buffer.write_unsigned(revision));
        try_write_buffer!(self, buffer.write_end_of_message());
        try_with_set_error_state!(self, self.connection.write_with_sleep(buffer.as_bytes()), Status::FailedToSendToClient);
        Ok(())
    }

    fn handle_read_req(& mut self) -> Result<(), ()> {

        let transaction_id = try_parse!(self.buffer.parse_transaction_id(), self, 0);
        let node_id = try_parse!(self.buffer.parse_node_id(), self, transaction_id);
        let offset = try_parse!(self.buffer.parse_unsigned(), self, transaction_id);
        let size = try_parse!(self.buffer.parse_unsigned(), self, transaction_id);
        try_parse!(self.buffer.expect(";"), self, transaction_id);
        try_parse!(self.buffer.parse_end_of_message(), self, transaction_id);

        trace!("Read: user={}, node_id={}, offset={}, size={}", self, node_id, offset, size);

        let ref mut open_file = match find_open_file(& mut self.open_files, & node_id) {
            Err(()) => {
                try_send_response_without_fields!(self, transaction_id, CommonErrorCodes::ErrorFileIsNotOpen as u64);
                return Err(());
            },
            Ok(v) => v,
        };

        match open_file.access.read(offset, size) {
            Ok((data, revision)) => {
                let mut buffer = try_write_buffer!(self, Client::create_response_buffer(transaction_id, CommonErrorCodes::NoError as u64));
                try_write_buffer!(self, buffer.write_unsigned(revision));
                try_write_buffer!(self, buffer.write_block(offset, data.len()));
                try_write_buffer!(self, buffer.write_end_of_message());
                try_with_set_error_state!(self, self.connection.write_with_sleep(buffer.as_bytes()), Status::FailedToSendToClient);

                if data.len() > 0 {
                    try_with_set_error_state!(self, self.connection.write_with_sleep(& data), Status::FailedToSendToClient);
                }

                Ok(())
            },
            Err(error) => {
                try_send_response_without_fields!(self, transaction_id, map_file_error_to_uint(error));
                Err(())
            }
        }
    }

    fn handle_delete_fs_element_req(& mut self) -> Result<(), ()> {

        let transaction_id = try_parse!(self.buffer.parse_transaction_id(), self, 0);
        let fd = try_parse!(self.buffer.parse_file_descriptor(), self, transaction_id);
        try_parse!(self.buffer.expect(";"), self, transaction_id);
        try_parse!(self.buffer.parse_end_of_message(), self, transaction_id);

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
                        try_in_receive_loop_to_send_response_without_fields!(client, transaction_id, CommonErrorCodes::NoError as u64);
                        (None, Some(Ok(())))
                    },

                    ClientProtocol::CountersResponse {
                        result: Err(error),
                    } => {
                        try_in_receive_loop_to_send_response_without_fields!(client, transaction_id, map_node_error_to_uint(error));
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
        let transaction_id = try_parse!(self.buffer.parse_transaction_id(), self, 0);
        try_parse!(self.buffer.expect(";"), self, transaction_id);
        try_parse!(self.buffer.parse_end_of_message(), self, transaction_id);

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

                        let mut map = KeyValueMap2::new();
                        map.insert(String::from("active-connections"), Value2::Unsigned {
                            value: counters.active_connections as u64
                        });

                        let mut buffer = try_in_receive_loop_to_create_buffer!(client, transaction_id, CommonErrorCodes::NoError);
                        try_in_receive_loop!(client, buffer.write_key_value_list(map), Status::FailedToWriteToSendBuffer);
                        try_in_receive_loop!(client, buffer.write_end_of_message(), Status::FailedToWriteToSendBuffer);
                        try_in_receive_loop!(client, client.connection.write_with_sleep(buffer.as_bytes()), Status::FailedToSendToClient);
                        (None, Some(Ok(())))
                    },

                    ClientProtocol::CountersResponse {
                        result: Err(error),
                    } => {
                        try_in_receive_loop_to_send_response_without_fields!(client, transaction_id, map_node_error_to_uint(error));
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
        let transaction_id = try_parse!(self.buffer.parse_transaction_id(), self, 0);
        let fd = try_parse!(self.buffer.parse_file_descriptor(), self, 0);
        try_parse!(self.buffer.expect(";"), self, transaction_id);
        try_parse!(self.buffer.parse_end_of_message(), self, transaction_id);

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

                        let mut buffer = try_in_receive_loop_to_create_buffer!(client, transaction_id, CommonErrorCodes::NoError);

                        try_in_receive_loop!(client, buffer.write_list_start(list_of_elements.len()), Status::FailedToWriteToSendBuffer);
                        for (name, node_id, type_of) in list_of_elements.into_iter() {

                            try_in_receive_loop!(client, buffer.write_list_element_start(), Status::FailedToWriteToSendBuffer);

                            try_in_receive_loop!(client, buffer.write_string(name), Status::FailedToWriteToSendBuffer);
                            try_in_receive_loop!(client, buffer.write_node_id(node_id), Status::FailedToWriteToSendBuffer);
                            try_in_receive_loop!(client, buffer.write_unsigned(
                                match type_of {
                                    FilesystemElement::Folder => FOLDER,
                                    FilesystemElement::File => FILE,
                                }
                            ), Status::FailedToWriteToSendBuffer);

                            try_in_receive_loop!(client, buffer.write_list_element_end(), Status::FailedToWriteToSendBuffer);
                        }

                        try_in_receive_loop!(client, buffer.write_list_end(), Status::FailedToWriteToSendBuffer);
                        try_in_receive_loop!(client, buffer.write_end_of_message(), Status::FailedToWriteToSendBuffer);
                        try_in_receive_loop!(client, client.connection.write_with_sleep(buffer.as_bytes()), Status::FailedToSendToClient);
                        (None, Some(Ok(())))
                    },

                    ClientProtocol::QueryListResponse {
                        result: Err(error),
                    } => {
                        try_in_receive_loop_to_send_response_without_fields!(client, transaction_id, map_node_error_to_uint(error));
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
        let transaction_id = try_parse!(self.buffer.parse_transaction_id(), self, 0);
        let fd = try_parse!(self.buffer.parse_file_descriptor(), self, 0); //  todo: user transaction id, check other usages as well
        try_parse!(self.buffer.expect(";"), self, 0);
        try_parse!(self.buffer.parse_end_of_message(), self, transaction_id);

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

                        let type_int = match desc.element_type {
                            FilesystemElement::Folder => 1,
                            FilesystemElement::File => 0,
                        };

                        let mut map = KeyValueMap2::new();
                        map.insert(String::from("type"), Value2::Unsigned { value: type_int as u64 });
                        map.insert(String::from("created"), Value2::Unsigned { value: desc.created_at as u64 });
                        map.insert(String::from("modified"), Value2::Unsigned { value: desc.modified_at as u64 });
                        map.insert(String::from("read-access"), Value2::String { value: desc.read_access });
                        map.insert(String::from("write-access"), Value2::String { value: desc.write_access });

                        let mut buffer = try_in_receive_loop_to_create_buffer!(client, transaction_id, CommonErrorCodes::NoError);
                        try_in_receive_loop!(client, buffer.write_key_value_list(map), Status::FailedToWriteToSendBuffer);
                        try_in_receive_loop!(client, buffer.write_end_of_message(), Status::FailedToWriteToSendBuffer);
                        try_in_receive_loop!(client, client.connection.write_with_sleep(buffer.as_bytes()), Status::FailedToSendToClient);
                        (None, Some(Ok(())))
                    },

                    ClientProtocol::QueryFilesystemResponse {
                        result: Err(error),
                    } => {
                        try_in_receive_loop_to_send_response_without_fields!(client, transaction_id, map_node_error_to_uint(error));
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

    fn handle_add_user_group(& mut self) -> Result<(), ()> {
        let transaction_id = try_parse!(self.buffer.parse_transaction_id(), self, 0);
        let type_of = try_parse!(self.buffer.parse_unsigned(), self, transaction_id);
        let name = try_parse!(self.buffer.parse_string(), self, transaction_id);
        try_parse!(self.buffer.expect(";"), self, transaction_id);
        try_parse!(self.buffer.parse_end_of_message(), self, transaction_id);

        trace!("Create user/group, user={}, name={}, type_of={}", self, name, type_of);

        let user = self.user.as_ref().unwrap().clone();
        let msg = match type_of {
            TYPE_USER => NodeProtocol::AddUserRequest {
                user: user,
                name: name,
            },
            TYPE_GROUP => NodeProtocol::AddGroupRequest {
                user: user,
                name: name,
            },
            _ => {
                try_send_response_without_fields!(self, transaction_id, CommonErrorCodes::ErrorMalformedMessage as u64);
                return Err(());
            },
        };

        self.send_to_node(transaction_id, msg) ? ;

        node_receive::<()>(
            self,
            & | msg, client | {
                match msg {
                    ClientProtocol::AddUserGroupResponse {
                        result: Ok(()),
                    } => {
                        try_in_receive_loop_to_send_response_without_fields!(client, transaction_id, CommonErrorCodes::NoError as u64);
                        (None, Some(Ok(())))
                    },
                    ClientProtocol::AddUserGroupResponse {
                        result: Err(error),
                    } => {
                        try_in_receive_loop_to_send_response_without_fields!(client, transaction_id, map_node_error_to_uint(error));
                        (None, Some(Err(())))
                    },
                    other => {
                        (Some(other), None)
                    },
                }
            })
            ? ;

        Ok(())
    }

    fn handle_mod_user_group(& mut self) -> Result<(), ()> {

        let transaction_id = try_parse!(self.buffer.parse_transaction_id(), self, 0);
        let type_of = try_parse!(self.buffer.parse_unsigned(), self, transaction_id);
        let name = try_parse!(self.buffer.parse_string(), self, transaction_id);
        let mut key_value_map = self.buffer.parse_key_value_list_() ? ;
        try_parse!(self.buffer.expect(";"), self, transaction_id);
        try_parse!(self.buffer.parse_end_of_message(), self, transaction_id);

        let user = self.user.as_ref().unwrap().clone();
        let msg = match type_of {
            TYPE_USER => {

                let password = key_value_map.remove("password")
                    .ok_or(())
                    .map(| value |
                         value.to_string()
                         .ok()
                    )
                    ? ;

                let expiration = key_value_map.remove("expiration")
                    .ok_or(())
                    .map(| value |
                         value.to_unsigned()
                         .ok()
                         .map(| value | {
                             if value != 0 {
                                 Some(value as i64)
                             } else {
                                 None
                             }
                         })
                    )
                    ? ;

                NodeProtocol::ModifyUser {
                    user: user,
                    name: name,
                    password: password,
                    expiration: expiration,
                }
            },
            TYPE_GROUP => {

                let expiration = key_value_map.remove("expiration")
                    .ok_or(())
                    .map(| value |
                         value.to_unsigned()
                         .ok()
                         .map(| value | {
                             if value != 0 {
                                 Some(value as i64)
                             } else {
                                 None
                             }
                         })
                    )
                    ? ;

                NodeProtocol::ModifyGroup {
                    user: user,
                    name: name,
                    expiration: expiration,
                }
            },
            _ => {
                try_send_response_without_fields!(self, transaction_id, CommonErrorCodes::ErrorMalformedMessage as u64);
                return Err(());
            },
        };

        self.send_to_node(transaction_id, msg) ? ;

        node_receive::<()>(
            self,
            & | msg, client | {
                match msg {
                    ClientProtocol::AddUserGroupResponse {
                        result: Ok(()),
                    } => {
                        try_in_receive_loop_to_send_response_without_fields!(client, transaction_id, CommonErrorCodes::NoError as u64);
                        (None, Some(Ok(())))
                    },
                    ClientProtocol::AddUserGroupResponse {
                        result: Err(error),
                    } => {
                        try_in_receive_loop_to_send_response_without_fields!(client, transaction_id, map_node_error_to_uint(error));
                        (None, Some(Err(())))
                    },
                    other => {
                        (Some(other), None)
                    },
                }
            })
            ? ;

        Ok(())
    }
}

fn find_open_file<'vec>(open_files: &'vec mut Vec<OpenFile>, searched_node_id: & NodeId)
                         -> Result<&'vec mut OpenFile, ()> {

    let mut searched_item_index: usize = 0;
    open_files
        .iter()
        .enumerate()
        .find(| & (ref index, ref element) | {
            searched_item_index = index.clone();
            element.node_id == *searched_node_id
        })
        .ok_or(())
        ? ;

    open_files
        .get_mut(searched_item_index)
        .ok_or(())
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

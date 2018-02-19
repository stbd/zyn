use std::fmt::{ Display, Formatter, Result as FmtResult };
use std::result::{ Result };
use std::sync::mpsc::{ Receiver, Sender, TryRecvError };
use std::thread::{ sleep };
use std::time::{ Duration };
use std::vec::{ Vec };
use std::{ str };

use node::client_protocol_buffer::{ ReceiveBuffer, SendBuffer };
use node::common::{ NodeId, Buffer, OpenMode, FileType, Timestamp };
use node::connection::{ Connection };
use node::file_handle::{ FileAccess, FileError, Notification, FileLock, FileProperties };
use node::filesystem::{ FilesystemError };
use node::node::{ ClientProtocol, NodeProtocol, FilesystemElement, FilesystemElementType, ErrorResponse, NodeError, ShutdownReason };
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
    InvalidEdit = 7,
    FailedToReceiveData = 8,
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

const DISCONNET_REASON_INTERNAL_ERROR: & str = "internal-error";
const DISCONNET_REASON_NODE_CLOSING: & str = "node-closing";

// ## Messages:
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

enum Status {
    Ok,
    AuthenticationError { trial: u8 },
    FailedToSendToNode,
    FailedToreceiveFromNode,
    FailedToSendToClient,
    FailedToreceiveFromClient,
    ClientNotAuthenticated,
    FailedToWriteToSendBuffer,
    ShutdownOrderedByNode,
    InternalError,
    ProtocolProcessingError,
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
            Status::FailedToWriteToSendBuffer =>
                write!(f, "FailedToWriteToSendBuffer"),
            Status::ShutdownOrderedByNode =>
                write!(f, "ShutdownOrderedByNode"),
            Status::InternalError =>
                write!(f, "InternalError"),
            Status::ProtocolProcessingError =>
                write!(f, "ProtocolProcessingError"),
            Status::FailedToreceiveFromClient =>
                write!(f, "FailedToreceiveFromClient"),
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

    fn set(& mut self, new_status: Status) {
        if self.is_in_error_state() {
            debug!("Status already in error \"{}\" when trying to set it to \"{}\"", *self, new_status);
        } else {
            debug!("Status set to \"{}\"", new_status);
            *self = new_status;
        }
    }
}

struct OpenFile {
    node_id: NodeId,
    open_mode: OpenMode,
    file_type: FileType,
    page_size: u64,
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
                        $class.status.set(status);
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
            $class.status.set($error_code);
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
                $class.status.set(status);
                return Err(())
            }
        }
    }}
}

macro_rules! try_in_receive_loop {
    ($class:expr, $operation:expr, $error_code:expr) => {{
        let result = $operation;
        if result.is_err() {
            $class.status.set($error_code);
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
                $class.status.set(status);
                return (None, Some(Err(())));
            }
        }
    }}
}

impl Client {
    pub fn new(
        connection: Connection,
        node_receive: Receiver<ClientProtocol>,
        node_send: Sender<NodeProtocol>,
        socket_buffer_size: usize,
    ) -> Client {

        info!("Creating new client, socket_buffer_size={}", socket_buffer_size);

        Client {
            connection: connection,
            buffer: ReceiveBuffer::with_capacity(socket_buffer_size),
            node_receive: node_receive,
            node_send: node_send,
            open_files: Vec::with_capacity(5),
            user: None,
            status: Status::Ok,
            node_message_buffer: Vec::with_capacity(5),
        }
    }

    pub fn process(& mut self) {

        let message_handlers: Vec<(& str, fn(& mut Client) -> Result<(), ()>, u64)> = vec![
            ("CREATE-FILE:", handle_create_file_req, 1),
            ("CREATE-FOLDER:", handle_create_folder_req, 1),
            ("O:", handle_open_req, 1),
            ("CLOSE:", handle_close_req, 1),
            ("RA-W:", handle_write_random_access_req, 1),
            ("RA-I:", handle_random_access_insert_req, 1),
            ("RA-D:", handle_random_access_delete_req, 1),
            ("BLOB-W:", handle_blob_write_req, 1),
            ("R:", handle_read_req, 1),
            ("DELETE:", handle_delete_fs_element_req, 1),
            ("Q-COUNTERS:", handle_query_counters_req, 1),
            ("Q-LIST:", handle_query_list_req, 1),
            ("Q-FILESYSTEM:", handle_query_fs_req, 1),
            ("ADD-USER-GROUP:", handle_add_user_group, 1),
            ("MOD-USER-GROUP:", handle_mod_user_group, 1),
        ];

        loop {

            let mut is_processing: bool = false;

            if self.handle_messages_from_node()
                .is_err() {
                    error!("Error while processing notification from node");
                    break ;
                }

            if self.status.is_in_error_state() {
                break ;
            }

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

            let message_namespace = match self.buffer.parse_message_namespace() {
                Ok(value) => value,
                Err(()) => {
                    error!("Failed to parse message namespace");
                    match Client::send_response_without_fields(& mut self.connection, 0, CommonErrorCodes::ErrorMalformedMessage as u64) {
                        Ok(()) => (),
                        Err(status) => {
                            self.status.set(status);
                        },
                    }
                    break;
                },
            };

            if ! self.is_authenticated() {
                if self.buffer.expect("A:").is_ok() {
                    let _ = handle_authentication_req(self);
                } else {
                    self.status.set(Status::ClientNotAuthenticated);
                    break ;
                }

            } else {

                if message_handlers
                    .iter()
                    .find( | && (ref handler_name, _, handler_namespace) | {
                        self.buffer.expect(handler_name).is_ok()
                            && message_namespace == handler_namespace
                    })
                    .and_then( | & (_, ref handler, _) | {
                        let _ = handler(self);
                        Some(())
                    })
                    .is_none() {
                        error!("Failed to find handler for message");
                        self.status.set(Status::ProtocolProcessingError);
                        break ;
                    }
            }

            self.buffer.drop_consumed_buffer();

            if self.status.is_in_error_state() {
                break ;
            }
        }

        for mut open_file in self.open_files.drain(..) {
            let _ = open_file.access.close();
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

                    try_with_set_error_state!(self, self.connection.write_with_sleep(buffer.as_bytes()), Status::FailedToSendToClient);
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

    fn send_close_notification(& mut self, reason: & str) -> Result<(), ()> {
        let mut buffer = try_write_buffer!(self, Client::create_notification_buffer());
        try_write_buffer!(self, buffer.write_notification_disconnected(& reason));
        try_with_set_error_state!(self, self.connection.write_with_sleep(buffer.as_bytes()), Status::FailedToSendToClient);
        Ok(())
    }

    fn handle_messages_from_node(& mut self) -> Result<(), ()> {

        match self.receive_from_node() {
            Err(()) => {
                self.status.set(Status::FailedToreceiveFromNode);
                self.send_close_notification(DISCONNET_REASON_INTERNAL_ERROR) ? ;
                return Err(());
            }
            Ok(None) => (),
            Ok(Some(message)) => {

                match message {
                    ClientProtocol::Shutdown { reason } => {
                        self.status.set(Status::ShutdownOrderedByNode);
                        let reason_string = match reason {
                            ShutdownReason::NodeClosing => {
                                DISCONNET_REASON_NODE_CLOSING
                            },
                        };
                        self.send_close_notification(& reason_string) ? ;
                    },
                    _ => {
                        error!("Unhandled notification from node");
                        self.status.set(Status::InternalError);
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
        let mut buffer = SendBuffer::with_capacity(1024 * 4);
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
                warn!("Unexpected message from node");
                self.status.set(Status::InternalError);
            },
        };
    }

    fn send_to_node(& mut self, transaction_id: u64, msg: NodeProtocol) -> Result<(), ()> {
        if let Err(desc) = self.node_send.send(msg) {
            warn!("Failed to send message to node, id={}, desc={}", self, desc);
            self.status.set(Status::FailedToSendToNode);
            try_send_response_without_fields!(self, transaction_id, CommonErrorCodes::InternalCommunicationError as u64);
            return Err(());
        }
        Ok(())
    }

    fn fill_buffer(receive_buffer: & mut ReceiveBuffer, connection: & mut Connection, mut buffer: & mut Buffer) -> Result<(), ()> {

        receive_buffer.take(& mut buffer);
        if buffer.len() == buffer.capacity() {
            return Ok(());
        }

        let mut trial = 1;
        const MAX_NUMBER_OF_TRIALS: usize = 10;

        while buffer.len() != buffer.capacity() && trial < MAX_NUMBER_OF_TRIALS {
            match connection.read(buffer) {
                Ok(true) => (),
                Ok(false) => {
                    sleep(Duration::from_millis(100));
                    trial += 1;
                },
                Err(()) => return Err(()),
            };
            if buffer.len() == buffer.capacity() {
                break;
            }
        }

        if buffer.len() == buffer.capacity() {
            Ok(())
        } else {
            Err(())
        }
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

/*
Authenticate request:
<- [Version]A:[Transaction Id][String: username][String: password];[End]
-> [Version]RSP:[Transaction Id][Uint: error code];[End]
*/
fn handle_authentication_req(client: & mut Client) -> Result<(), ()>
{
    let transaction_id = try_parse!(client.buffer.parse_transaction_id(), client, 0);
    let username = try_parse!(client.buffer.parse_string(), client, transaction_id);
    let password = try_parse!(client.buffer.parse_string(), client, transaction_id);
    try_parse!(client.buffer.expect(";"), client, transaction_id);
    try_parse!(client.buffer.parse_end_of_message(), client, transaction_id);

    trace!("Authenticate, username=\"{}\"", username);

    client.send_to_node(transaction_id, NodeProtocol::AuthenticateRequest {
        username: username.clone(),
        password: password
    }) ? ;

    let id = node_receive::<Id>(
        client,
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
                        client.status.set(Status::AuthenticationError { trial: 1 });
                    }
                    debug!("Invalid password for username={}, status={}", username, client.status);
                    (None, Some(Err(())))
                },

                other => {
                    (Some(other), None)
                }
            }
        })
        ? ;

    debug!("Authentication ok, username=\"{}\", id={}", username, id);

    client.user = Some(id);
    Ok(())
}

/*
Create file request
<- [Version]CREATE-FILE:[Transaction Id][FileDescriptor: parent][String: name][Uint: type];[End]
 * Type: 0: random access,
 * Type: 1: blob,
-> [Version]RSP:[Transaction Id][Uint: error code];(Node-Id)[End]
*/
fn handle_create_file_req(client: & mut Client) -> Result<(), ()>
{
    let transaction_id = try_parse!(client.buffer.parse_transaction_id(), client, 0);
    let parent = try_parse!(client.buffer.parse_file_descriptor(), client, transaction_id);
    let name = try_parse!(client.buffer.parse_string(), client, transaction_id);
    let type_uint = try_parse!(client.buffer.parse_unsigned(), client, transaction_id);
    try_parse!(client.buffer.expect(";"), client, transaction_id);
    try_parse!(client.buffer.parse_end_of_message(), client, transaction_id);

    let type_enum = match type_uint {
        FILE_TYPE_RANDOM_ACCESS => FileType::RandomAccess,
        FILE_TYPE_BLOB => FileType::Blob,
        _ => {
            try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::ErrorMalformedMessage as u64);
            return Err(());
        },
    };

    trace!("Create file: user={}, parent=\"{}\", name=\"{}\", type={}",
           client, parent, name, type_enum);

    let user = client.user.as_ref().unwrap().clone();
    client.send_to_node(transaction_id, NodeProtocol::CreateFileRequest {
        parent: parent,
        type_of_file: type_enum,
        name: name,
        user: user,
    }) ? ;

    node_receive::<()>(
        client,
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

/*
Create folder request
<- [Version]CREATE-FOLDER:[Transaction Id][FileDescriptor: parent][String: name];[End]
-> [Version]RSP:[Transaction Id][Uint: error code];(Node-Id)[End]
*/
fn handle_create_folder_req(client: & mut Client) -> Result<(), ()>
{
    let transaction_id = try_parse!(client.buffer.parse_transaction_id(), client, 0);
    let parent = try_parse!(client.buffer.parse_file_descriptor(), client, transaction_id);
    let name = try_parse!(client.buffer.parse_string(), client, transaction_id);
    try_parse!(client.buffer.expect(";"), client, transaction_id);
    try_parse!(client.buffer.parse_end_of_message(), client, transaction_id);

    trace!("Create folder: user={}, parent=\"{}\",  name=\"{}\"",
           client, parent, name);

    let user = client.user.as_ref().unwrap().clone();
    client.send_to_node(transaction_id, NodeProtocol::CreateFolderRequest {
        parent: parent,
        name: name,
        user: user,
    }) ? ;

    node_receive::<()>(
        client,
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

/*
Open file file:
<- [Version]C:[Transaction Id][FileDescriptor][Uint: type];[End]
 * Type: 0: read
 * Type, 1: read-write
-> [Version]RSP:[Transaction Id][Uint: error code](Node-Id)(Uint: revision)(Uint: size)(Uint: type);[End]
 * Type: 0: random access
 * Type: 1: blob
*/
fn handle_open_req(client: & mut Client) -> Result<(), ()>
{
    let transaction_id = try_parse!(client.buffer.parse_transaction_id(), client, 0);
    let fd = try_parse!(client.buffer.parse_file_descriptor(), client, transaction_id);
    let mode_uint = try_parse!(client.buffer.parse_unsigned(), client, transaction_id);
    try_parse!(client.buffer.expect(";"), client, transaction_id);
    try_parse!(client.buffer.parse_end_of_message(), client, transaction_id);

    let mode = match mode_uint {
        READ => OpenMode::Read,
        READ_WRITE => OpenMode::ReadWrite,
        _ => {
            try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::ErrorMalformedMessage as u64);
            return Err(());
        },
    };

    trace!("Open file, user={}, fd=\"{}\"", client, fd);

    let user = client.user.as_ref().unwrap().clone();
    client.send_to_node(transaction_id, NodeProtocol::OpenFileRequest {
        mode: mode.clone(),
        file_descriptor: fd,
        user: user,
    }) ? ;

    let (access, node_id, properties) = node_receive::<(FileAccess, NodeId, FileProperties)>(
        client,
        & | msg, client | {
            match msg {
                ClientProtocol::OpenFileResponse {
                    result: Ok((access, node_id, properties)),
                } => {
                    let mut buffer = try_in_receive_loop_to_create_buffer!(client, transaction_id, CommonErrorCodes::NoError);
                    try_in_receive_loop!(client, buffer.write_node_id(node_id), Status::FailedToWriteToSendBuffer);
                    try_in_receive_loop!(client, buffer.write_unsigned(properties.revision), Status::FailedToWriteToSendBuffer);
                    try_in_receive_loop!(client, buffer.write_unsigned(properties.size), Status::FailedToWriteToSendBuffer);

                    try_in_receive_loop!(
                        client,
                        buffer.write_unsigned(
                            match properties.file_type {
                                FileType::RandomAccess => FILE_TYPE_RANDOM_ACCESS,
                                FileType::Blob => FILE_TYPE_BLOB,
                            }),
                        Status::FailedToWriteToSendBuffer
                    );

                    try_in_receive_loop!(client, buffer.write_end_of_message(), Status::FailedToWriteToSendBuffer);
                    try_in_receive_loop!(client, client.connection.write_with_sleep(buffer.as_bytes()), Status::FailedToSendToClient);
                    (None, Some(Ok((access, node_id, properties))))
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

    client.open_files.push(OpenFile {
        node_id: node_id,
        open_mode: mode,
        file_type: properties.file_type,
        page_size: properties.page_size,
        access: access
    });
    Ok(())
}

/*
Close file request
<- [Version]C:[Transaction Id][NodeId];[End]
-> [Version]RSP:[Transaction Id][Uint: error code];[End]
*/
fn handle_close_req(client: & mut Client) -> Result<(), ()>
{
    let transaction_id = try_parse!(client.buffer.parse_transaction_id(), client, 0);
    let node_id = try_parse!(client.buffer.parse_node_id(), client, transaction_id);
    try_parse!(client.buffer.expect(";"), client, transaction_id);
    try_parse!(client.buffer.parse_end_of_message(), client, transaction_id);

    trace!("Close: user={}, node_id={}", client, node_id);

    let position = client.open_files.iter().position(| ref open_file | {
        open_file.node_id == node_id
    }) ;

    if let Some(index) = position {
        let mut open_file = client.open_files.remove(index);

        match open_file.access.close() {
            Ok(()) => {
                try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::NoError as u64);
                Ok(())
            },
            Err(error) => {
                try_send_response_without_fields!(client, transaction_id, map_file_error_to_uint(error));
                Err(())
            }
        }
    } else {
        try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::ErrorFileIsNotOpen as u64);
        Err(())
    }
}

fn is_block_in_page(page_size: u64, offset: u64, size: u64) -> bool
{
    let page_start = (offset / page_size) * page_size;
    let page_end = page_start + page_size;
    if (offset + size) > page_end {
        false
    } else {
        true
    }
}

fn is_random_access_edit_allowed(page_size: u64, offset: u64, size: u64) -> bool
{
    if offset > page_size {
        return false;
    }
    is_block_in_page(page_size, offset, size)
}


/*
Write to random access file request
<- [Version]RA-W:[Transaction Id][Node-Id: file][Uint: revision][Block];[End]
 * file needs to be open
 * file needs to be of type random access
-> [Version]RSP:[Transaction Id][Uint: error code];[End]
<- [data]
-> [Version]RSP:[Transaction Id][Uint: error code](Uint: revision);[End]
*/
fn handle_write_random_access_req(client: & mut Client) -> Result<(), ()>
{
    let transaction_id = try_parse!(client.buffer.parse_transaction_id(), client, 0);
    let node_id = try_parse!(client.buffer.parse_node_id(), client, transaction_id);
    let revision = try_parse!(client.buffer.parse_unsigned(), client, transaction_id);
    let (offset, size) = try_parse!(client.buffer.parse_block(), client, transaction_id);
    try_parse!(client.buffer.expect(";"), client, transaction_id);
    try_parse!(client.buffer.parse_end_of_message(), client, transaction_id);

    trace!("Write: user={}, node_id={}, revision={}, offset={}, size={}",
           client, node_id, revision, offset, size);

    let ref mut open_file = match find_open_file(& mut client.open_files, & node_id) {
        Err(()) => {
            try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::ErrorFileIsNotOpen as u64);
            return Err(());
        },
        Ok(v) => v,
    };

    match open_file.open_mode {
        OpenMode::Read => {
            try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::ErrorFileOpenedInReadMode as u64);
            return Err(());
        },
        OpenMode::ReadWrite => (),
    }

    if ! is_random_access_edit_allowed(open_file.page_size, offset, size) {
        warn!("Invalid edit, edited block not in fist page, page_size={}, offset={}, size={}",
              open_file.page_size, offset, size);
        try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::InvalidEdit as u64);
        return Err(());
    }

    try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::NoError as u64);

    let mut data = Buffer::with_capacity(size as usize);
    if Client::fill_buffer(& mut client.buffer, & mut client.connection, & mut data).is_err() {
        client.status.set(Status::FailedToreceiveFromClient);
        try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::FailedToReceiveData as u64);
        return Err(());
    }

    match open_file.access.write(revision, offset, data) {
        Ok(revision) => {
            let mut buffer = try_write_buffer!(client, Client::create_response_buffer(transaction_id, CommonErrorCodes::NoError as u64));
            try_write_buffer!(client, buffer.write_unsigned(revision));
            try_write_buffer!(client, buffer.write_end_of_message());
            try_with_set_error_state!(client, client.connection.write_with_sleep(buffer.as_bytes()), Status::FailedToSendToClient);
            Ok(())
        },
        Err(error) => {
            try_send_response_without_fields!(client, transaction_id, map_file_error_to_uint(error));
            Err(())
        }
    }
}

/*
Insert to random access file
<- [Version]RA-I:[Transaction Id][Node-Id: opened file][Uint: revision][Block];[End]
 * file needs to be open
 * file needs to be of type random access
-> [Version]RSP:[Transaction Id][Uint: error code];[End]
<- [data]
-> [Version]RSP:[Transaction Id][Uint: error code](Uint: revision);[End]
*/
fn handle_random_access_insert_req(client: & mut Client) -> Result<(), ()>
{
    let transaction_id = try_parse!(client.buffer.parse_transaction_id(), client, 0);
    let node_id = try_parse!(client.buffer.parse_node_id(), client, transaction_id);
    let revision = try_parse!(client.buffer.parse_unsigned(), client, transaction_id);
    let (offset, size) = try_parse!(client.buffer.parse_block(), client, transaction_id);
    try_parse!(client.buffer.expect(";"), client, transaction_id);
    try_parse!(client.buffer.parse_end_of_message(), client, transaction_id);

    trace!("Insert: user={}, node_id={}, revision={}, offset={}, size={}",
           client, node_id, revision, offset, size);

    let ref mut open_file = match find_open_file(& mut client.open_files, & node_id) {
        Err(()) => {
            try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::ErrorFileIsNotOpen as u64);
            return Err(());
        },
        Ok(v) => v,
    };

    match open_file.open_mode {
        OpenMode::Read => {
            try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::ErrorFileOpenedInReadMode as u64);
            return Err(());
        },
        OpenMode::ReadWrite => (),
    }

    if ! is_random_access_edit_allowed(open_file.page_size, offset, size) {
        warn!("Invalid edit, edited block not in fist page, page_size={}, offset={}, size={}",
              open_file.page_size, offset, size);
        try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::InvalidEdit as u64);
        return Err(());
    }

    try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::NoError as u64);

    let mut data = Buffer::with_capacity(size as usize);
    if Client::fill_buffer(& mut client.buffer, & mut client.connection, & mut data).is_err() {
        client.status.set(Status::FailedToreceiveFromClient);
        try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::FailedToReceiveData as u64);
        return Err(());
    }

    match open_file.access.insert(revision, offset, data) {
        Ok(revision) => {
            let mut buffer = try_write_buffer!(client, Client::create_response_buffer(transaction_id, CommonErrorCodes::NoError as u64));
            try_write_buffer!(client, buffer.write_unsigned(revision));
            try_write_buffer!(client, buffer.write_end_of_message());
            try_with_set_error_state!(client, client.connection.write_with_sleep(buffer.as_bytes()), Status::FailedToSendToClient);
            Ok(())
        },
        Err(error) => {
            try_send_response_without_fields!(client, transaction_id, map_file_error_to_uint(error));
            Err(())
        }
    }
}

/*
Delete from random access file
<- [Version]RA-D:[Transaction Id][Node-Id: opened file][Uint: revision][Block];[End]
 * file needs to be open
 * file needs to be of type random access
-> [Version]RSP:[Transaction Id][Uint: error code](Uint: revision);[End]
*/
fn handle_random_access_delete_req(client: & mut Client) -> Result<(), ()>
{
    let transaction_id = try_parse!(client.buffer.parse_transaction_id(), client, 0);
    let node_id = try_parse!(client.buffer.parse_node_id(), client, transaction_id);
    let revision = try_parse!(client.buffer.parse_unsigned(), client, transaction_id);
    let (offset, size) = try_parse!(client.buffer.parse_block(), client, transaction_id);
    try_parse!(client.buffer.expect(";"), client, transaction_id);
    try_parse!(client.buffer.parse_end_of_message(), client, transaction_id);

    trace!("Delete: user={}, node_id={}, revision={}, offset={}, size={}",
           client, node_id, revision, offset, size);

    let ref mut open_file = match find_open_file(& mut client.open_files, & node_id) {
        Err(()) => {
            try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::ErrorFileIsNotOpen as u64);
            return Err(());
        },
        Ok(v) => v,
    };

    match open_file.open_mode {
        OpenMode::Read => {
            try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::ErrorFileOpenedInReadMode as u64);
            return Err(());
        },
        OpenMode::ReadWrite => (),
    }

    if ! is_random_access_edit_allowed(open_file.page_size, offset, size) {
        warn!("Invalid edit, edited block not in fist page, page_size={}, offset={}, size={}",
              open_file.page_size, offset, size);
        try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::InvalidEdit as u64);
        return Err(());
    }

    match open_file.access.delete(revision, offset, size) {
        Ok(revision) => {
            let mut buffer = try_write_buffer!(client, Client::create_response_buffer(transaction_id, CommonErrorCodes::NoError as u64));
            try_write_buffer!(client, buffer.write_unsigned(revision));
            try_write_buffer!(client, buffer.write_end_of_message());
            try_with_set_error_state!(client, client.connection.write_with_sleep(buffer.as_bytes()), Status::FailedToSendToClient);
            Ok(())
        },
        Err(error) => {
            try_send_response_without_fields!(client, transaction_id, map_file_error_to_uint(error));
            Err(())
        }
    }
}

/*
Write to blob file
<- [Version]RA-D:[Transaction Id][Node-Id: opened file][Uint: revision][Uint: file size][Uint: block size];[End]
 * file needs to be open
 * file needs to be of type blob
-> [Version]RSP:[Transaction Id][Uint: error code][End]
<- [data]
 * Possible in multiple blocks
-> [Version]RSP:[Transaction Id][Uint: error code](Uint: revision);[End]
*/
fn handle_blob_write_req(client: & mut Client) -> Result<(), ()>
{
    let transaction_id = try_parse!(client.buffer.parse_transaction_id(), client, 0);
    let node_id = try_parse!(client.buffer.parse_node_id(), client, transaction_id);
    let mut revision = try_parse!(client.buffer.parse_unsigned(), client, transaction_id);
    let size = try_parse!(client.buffer.parse_unsigned(), client, transaction_id);
    let block_size = try_parse!(client.buffer.parse_unsigned(), client, transaction_id);

    try_parse!(client.buffer.expect(";"), client, transaction_id);
    try_parse!(client.buffer.parse_end_of_message(), client, transaction_id);

    trace!("Write blob: user={}, node_id={}, revision={}, size={}, block_size={}",
           client, node_id, revision, size, block_size);

    let ref mut open_file = match find_open_file(& mut client.open_files, & node_id) {
        Err(()) => {
            try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::ErrorFileIsNotOpen as u64);
            return Err(());
        },
        Ok(v) => v,
    };

    match open_file.file_type {
        FileType::Blob => (),
        _ => {
            try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::OperationNotPermitedFotFileType as u64);
            return Err(());
        },
    };

    match open_file.open_mode {
        OpenMode::Read => {
            try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::ErrorFileOpenedInReadMode as u64);
            return Err(());
        },
        OpenMode::ReadWrite => (),
    }

    trace!("Write blob: page_size={}", open_file.page_size);

    if size > open_file.page_size {
        // If size of file is greater than page size, file must be written in blocks that do not cross pages
        if block_size > open_file.page_size || open_file.page_size % block_size != 0 {
            try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::BlockSizeIsTooLarge as u64);
            return Err(());
        }
    } else {
        // If size of file is less than page size, block size must match
        if size != block_size {
            try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::BlockSizeIsTooLarge as u64);
            return Err(());
        }
    }

    let user = client.user.as_ref().unwrap();
    let lock = FileLock::LockedBySystemForBlobWrite {
        user: user.clone(),
    };

    if let Err(error) = open_file.access.lock(revision, & lock) {
        try_send_response_without_fields!(client, transaction_id, map_file_error_to_uint(error));
        return Err(());
    }

    try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::NoError as u64);

    let mut bytes_read: u64 = 0;
    while bytes_read < size {

        let bytes_left = size - bytes_read;
        let buffer_size = {
            if bytes_left < block_size {
                bytes_left
            } else {
                block_size
            }
        };

        let mut buffer = Buffer::with_capacity(buffer_size as usize);
        if Client::fill_buffer(& mut client.buffer, & mut client.connection, & mut buffer).is_err() {
            client.status.set(Status::FailedToreceiveFromClient);
            try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::FailedToReceiveData as u64);
            return Err(());
        }

        revision = match open_file.access.write(revision, bytes_read, buffer) {
            Ok(r) => r,
            Err(error) => {
                try_send_response_without_fields!(client, transaction_id, map_file_error_to_uint(error));
                return Err(());
            }
        };

        try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::NoError as u64);
        bytes_read += buffer_size;
    }

    if let Err(error) = open_file.access.unlock(& lock) {
        error!("Failed to unlock file, user={}, error={}", user, map_file_error_to_uint(error));
        return Err(());
    }

    let mut buffer = try_write_buffer!(client, Client::create_response_buffer(transaction_id, CommonErrorCodes::NoError as u64));
    try_write_buffer!(client, buffer.write_unsigned(revision));
    try_write_buffer!(client, buffer.write_end_of_message());
    try_with_set_error_state!(client, client.connection.write_with_sleep(buffer.as_bytes()), Status::FailedToSendToClient);
    Ok(())
}

/*
Read:
<- [Version]R:[Transaction Id][Node-Id: file][Block];[End]
 * file needs to be open
-> [Version]RSP:[Transaction Id][Uint: error code](Uint: revision)(Block: of what will be sent);[End]
-> [data]
 * Data is only sent if file is not empty
*/
fn handle_read_req(client: & mut Client) -> Result<(), ()>
{
    let transaction_id = try_parse!(client.buffer.parse_transaction_id(), client, 0);
    let node_id = try_parse!(client.buffer.parse_node_id(), client, transaction_id);
    let (offset, size) = try_parse!(client.buffer.parse_block(), client, transaction_id);
    try_parse!(client.buffer.expect(";"), client, transaction_id);
    try_parse!(client.buffer.parse_end_of_message(), client, transaction_id);

    trace!("Read: user={}, node_id={}, offset={}, size={}", client, node_id, offset, size);

    let ref mut open_file = match find_open_file(& mut client.open_files, & node_id) {
        Err(()) => {
            try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::ErrorFileIsNotOpen as u64);
            return Err(());
        },
        Ok(v) => v,
    };

    match open_file.access.read(offset, size) {
        Ok((data, revision)) => {
            let mut buffer = try_write_buffer!(client, Client::create_response_buffer(transaction_id, CommonErrorCodes::NoError as u64));
            try_write_buffer!(client, buffer.write_unsigned(revision));
            try_write_buffer!(client, buffer.write_block(offset, data.len()));
            try_write_buffer!(client, buffer.write_end_of_message());
            try_with_set_error_state!(client, client.connection.write_with_sleep(buffer.as_bytes()), Status::FailedToSendToClient);

            if data.len() > 0 {
                try_with_set_error_state!(client, client.connection.write_with_sleep(& data), Status::FailedToSendToClient);
            } else {
                warn!("Read: No data to send");
            }
            Ok(())
        },
        Err(error) => {
            try_send_response_without_fields!(client, transaction_id, map_file_error_to_uint(error));
            Err(())
        }
    }
}

/*
Delete file system element
<- [Version]DELETE:[Transaction Id][FileDescriptor];[End]
-> [Version]RSP:[Transaction Id][Uint: error code];[End]
 */
fn handle_delete_fs_element_req(client: & mut Client) -> Result<(), ()>
{
    let transaction_id = try_parse!(client.buffer.parse_transaction_id(), client, 0);
    let fd = try_parse!(client.buffer.parse_file_descriptor(), client, transaction_id);
    try_parse!(client.buffer.expect(";"), client, transaction_id);
    try_parse!(client.buffer.parse_end_of_message(), client, transaction_id);

    trace!("Delete: user={}, fd={}", client, fd);

    let user = client.user.as_ref().unwrap().clone();
    client.send_to_node(transaction_id, NodeProtocol::DeleteRequest {
        user: user,
        fd: fd,
    }) ? ;

    node_receive::<()>(
        client,
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

fn write_le_kv_su(buffer: & mut SendBuffer, key: & str, value: u64) -> Result<(), ()> {
    buffer.write_list_element_start() ? ;
    buffer.write_key_value_pair_start() ? ;
    buffer.write_string(String::from(key)) ? ;
    buffer.write_unsigned(value) ? ;
    buffer.write_key_value_pair_end() ? ;
    buffer.write_list_element_end()
}

fn write_le_kv_ss(buffer: & mut SendBuffer, key: & str, value: String) -> Result<(), ()> {
    buffer.write_list_element_start() ? ;
    buffer.write_key_value_pair_start() ? ;
    buffer.write_string(String::from(key)) ? ;
    buffer.write_string(value) ? ;
    buffer.write_key_value_pair_end() ? ;
    buffer.write_list_element_end()
}

/*
Query system counters
<- [Version]Q-COUNTERS:[Transaction Id];[End]
-> [Version]RSP:[Transaction Id][List-of-Key-value-pair-of-u32];;[End]
*/
fn handle_query_counters_req(client: & mut Client) -> Result<(), ()>
{
    let transaction_id = try_parse!(client.buffer.parse_transaction_id(), client, 0);
    try_parse!(client.buffer.expect(";"), client, transaction_id);
    try_parse!(client.buffer.parse_end_of_message(), client, transaction_id);

    trace!("Query counters: user={}", client);

    let user = client.user.as_ref().unwrap().clone();
    client.send_to_node(transaction_id, NodeProtocol::CountersRequest {
        user: user,
    }) ? ;

    node_receive::<()>(
        client,
        & | msg, client | {
            match msg {
                ClientProtocol::CountersResponse {
                    result: Ok(counters),
                } => {
                    let mut buffer = try_in_receive_loop_to_create_buffer!(client, transaction_id, CommonErrorCodes::NoError);
                    try_in_receive_loop!(client, buffer.write_list_start(1), Status::FailedToWriteToSendBuffer);
                    try_in_receive_loop!(client, write_le_kv_su(& mut buffer, "active-connections", counters.active_connections as u64), Status::FailedToWriteToSendBuffer);
                    try_in_receive_loop!(client, buffer.write_list_end(), Status::FailedToWriteToSendBuffer);
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

/*
Query list file system contents
<- [Version]Q-LIST:[Transaction Id][FileDescriptor];[End]
-> [Version]RSP:[Transaction Id][List-of-Element-Info];[End]
 * Element-Info: List-of-(String: name, node_id, uint: type-of-element)
 * type-of-element: 0: file, 1: directory
*/
fn handle_query_list_req(client: & mut Client) -> Result<(), ()>
{
    let transaction_id = try_parse!(client.buffer.parse_transaction_id(), client, 0);
    let fd = try_parse!(client.buffer.parse_file_descriptor(), client, 0);
    try_parse!(client.buffer.expect(";"), client, transaction_id);
    try_parse!(client.buffer.parse_end_of_message(), client, transaction_id);

    trace!("Query list: user={}, fd={}", client, fd);

    let user = client.user.as_ref().unwrap().clone();
    client.send_to_node(transaction_id, NodeProtocol::QueryListRequest {
        user: user,
        fd: fd,
    }) ? ;

    node_receive::<()>(
        client,
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
                                FilesystemElementType::Folder => FOLDER,
                                FilesystemElementType::File => FILE,
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

/*
Query fileystem element
<- [Version]Q-FILESYSTEM:[Transaction Id][FileDescriptor];[End]
-> [Version]RSP:[Transaction Id][Uint: type][List-of-key-value-pairs];[End]
 * Uint type: 0: file, 1: directory
*/
fn handle_query_fs_req(client: & mut Client) -> Result<(), ()>
{
    let transaction_id = try_parse!(client.buffer.parse_transaction_id(), client, 0);
    let fd = try_parse!(client.buffer.parse_file_descriptor(), client, 0); //  todo: user transaction id, check other usages as well
    try_parse!(client.buffer.expect(";"), client, 0);
    try_parse!(client.buffer.parse_end_of_message(), client, transaction_id);

    trace!("Query fs: user={}, fd={}", client, fd);

    let user = client.user.as_ref().unwrap().clone();
    client.send_to_node(transaction_id, NodeProtocol::QueryFilesystemRequest {
        user: user,
        fd: fd,
    }) ? ;

    node_receive::<()>(
        client,
        & | msg, client | {
            match msg {
                ClientProtocol::QueryFilesystemResponse {
                    result: Ok(desc),
                } => {

                    let mut buffer = try_in_receive_loop_to_create_buffer!(client, transaction_id, CommonErrorCodes::NoError);
                    match desc {
                        FilesystemElement::File { properties, authority } => {
                            try_in_receive_loop!(client, buffer.write_list_start(5), Status::FailedToWriteToSendBuffer);
                            try_in_receive_loop!(client, write_le_kv_su(& mut buffer, "type", FILE as u64), Status::FailedToWriteToSendBuffer);
                            try_in_receive_loop!(client, write_le_kv_su(& mut buffer, "created",  properties.created_at as u64), Status::FailedToWriteToSendBuffer);
                            try_in_receive_loop!(client, write_le_kv_su(& mut buffer, "modified",  properties.modified_at as u64), Status::FailedToWriteToSendBuffer);
                            try_in_receive_loop!(client, write_le_kv_ss(& mut buffer, "read-access",  authority.read), Status::FailedToWriteToSendBuffer);
                            try_in_receive_loop!(client, write_le_kv_ss(& mut buffer, "write-access",  authority.write), Status::FailedToWriteToSendBuffer);
                            try_in_receive_loop!(client, buffer.write_list_end(), Status::FailedToWriteToSendBuffer);

                        },
                        FilesystemElement::Folder { created_at, modified_at, authority } => {
                            try_in_receive_loop!(client, buffer.write_list_start(5), Status::FailedToWriteToSendBuffer);
                            try_in_receive_loop!(client, write_le_kv_su(& mut buffer, "type", FOLDER as u64), Status::FailedToWriteToSendBuffer);
                            try_in_receive_loop!(client, write_le_kv_su(& mut buffer, "created",  created_at as u64), Status::FailedToWriteToSendBuffer);
                            try_in_receive_loop!(client, write_le_kv_su(& mut buffer, "modified",  modified_at as u64), Status::FailedToWriteToSendBuffer);
                            try_in_receive_loop!(client, write_le_kv_ss(& mut buffer, "read-access",  authority.read), Status::FailedToWriteToSendBuffer);
                            try_in_receive_loop!(client, write_le_kv_ss(& mut buffer, "write-access",  authority.write), Status::FailedToWriteToSendBuffer);
                            try_in_receive_loop!(client, buffer.write_list_end(), Status::FailedToWriteToSendBuffer);
                        },
                    }

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

/*
Add user/group
<- [Version]ADD-USER-GROUP:[Transaction Id][Uint: type][String: name];[End]
 * type 0: user
 * type 1: group
-> [Version]RSP:[Transaction Id][Uint: error code];[End]
*/
fn handle_add_user_group(client: & mut Client) -> Result<(), ()>
{
    let transaction_id = try_parse!(client.buffer.parse_transaction_id(), client, 0);
    let type_of = try_parse!(client.buffer.parse_unsigned(), client, transaction_id);
    let name = try_parse!(client.buffer.parse_string(), client, transaction_id);
    try_parse!(client.buffer.expect(";"), client, transaction_id);
    try_parse!(client.buffer.parse_end_of_message(), client, transaction_id);

    trace!("Create user/group, user={}, name={}, type_of={}", client, name, type_of);

    let user = client.user.as_ref().unwrap().clone();
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
            try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::ErrorMalformedMessage as u64);
            return Err(());
        },
    };

    client.send_to_node(transaction_id, msg) ? ;

    node_receive::<()>(
        client,
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

/*
Modify user/group
<- [Version]MOD-USER-GROUP:[Transaction Id][uint: type][String: name][Key-value-list];[End]
 * type: 0: user, 1: group
-> [Version]RSP:[Transaction Id][Uint: error code];[End]
*/
fn handle_mod_user_group(client: & mut Client) -> Result<(), ()>
{
    let mut password: Option<String> = None;
    let mut expiration: Option<Option<Timestamp>> = None;

    let transaction_id = try_parse!(client.buffer.parse_transaction_id(), client, 0);
    let type_of = try_parse!(client.buffer.parse_unsigned(), client, transaction_id);
    let name = try_parse!(client.buffer.parse_string(), client, transaction_id);

    let number_of_elements = try_parse!(client.buffer.parse_list_start(), client, transaction_id);
    for _ in 0..number_of_elements {
        if client.buffer.parse_list_element_start().is_err() {
            try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::ErrorMalformedMessage as u64);
            return Err(());
        }

        try_parse!(client.buffer.parse_key_value_pair_start(), client, transaction_id);
        let key = try_parse!(client.buffer.parse_string(), client, transaction_id);

        if key == "password" {
            password = Some(try_parse!(client.buffer.parse_string(), client, transaction_id));
        } else if key == "expiration" {
            let value = try_parse!(client.buffer.parse_unsigned(), client, transaction_id);
            if value == 0 {
                expiration = Some(None);
            } else {
                expiration = Some(Some(value as i64));
            }
        } else {
            warn!("Failed to parse key value");
            try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::ErrorMalformedMessage as u64);
            return Err(());
        }

        try_parse!(client.buffer.parse_key_value_pair_end(), client, transaction_id);
        try_parse!(client.buffer.parse_list_element_end(), client, transaction_id);
    }
    try_parse!(client.buffer.parse_list_end(), client, transaction_id);

    try_parse!(client.buffer.expect(";"), client, transaction_id);
    try_parse!(client.buffer.parse_end_of_message(), client, transaction_id);

    let user = client.user.as_ref().unwrap().clone();
    let msg = match type_of {
        TYPE_USER => {
            NodeProtocol::ModifyUser {
                user: user,
                name: name,
                password: password,
                expiration: expiration,
            }
        },
        TYPE_GROUP => {
            NodeProtocol::ModifyGroup {
                user: user,
                name: name,
                expiration: expiration,
            }
        },
        _ => {
            try_send_response_without_fields!(client, transaction_id, CommonErrorCodes::ErrorMalformedMessage as u64);
            return Err(());
        },
    };

    client.send_to_node(transaction_id, msg) ? ;

    node_receive::<()>(
        client,
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
                client.status.set(Status::FailedToreceiveFromNode);
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
    client.status.set(Status::FailedToreceiveFromNode);
    Err(())
}

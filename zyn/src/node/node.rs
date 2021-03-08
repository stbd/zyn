use std::fs::{ create_dir, read_dir };
use std::mem::{ MaybeUninit };
use std::path::{ Path, PathBuf };
use std::ptr::{ null_mut };
use std::sync::mpsc::{ channel, Sender, Receiver, TryRecvError };
use std::thread::{ sleep, spawn, JoinHandle };
use std::time::{ Duration };
use std::vec::{ Vec };

use libc::{ sigwait, sigemptyset, sigaddset, SIGTERM, SIGINT, c_int, size_t, sigprocmask, SIG_SETMASK };
use rand::{ random };

use crate::node::client::{ Client };
use crate::node::common::{ NodeId, FileDescriptor, OpenMode, ADMIN_GROUP, Timestamp, FileType, FileRevision, log_crypto_context_error, utc_timestamp };
use crate::node::tls_connection::{ TlsServer };
use crate::node::crypto::{ Crypto };
use crate::node::file_handle::{ FileAccess, FileProperties };
use crate::node::filesystem::{ Filesystem, FilesystemError, Node as FsNode };
use crate::node::directory::{ Child };
use crate::node::user_authority::{ UserAuthority, Id };
use crate::node::serialize::{ SerializedNode };

pub enum NodeError {
    InvalidUsernamePassword,
    ParentIsNotDirectory,
    UnknownAuthority,
    AuthorityError,
    UnauthorizedOperation,
    InternalCommunicationError,
    InternalError,
    UnknownFile,
    InvalidPageSize,
    FailedToResolveAuthority,
    FailedToAllocateAuthenticationToken,
    FailedToConsumeAuthenticationToken,
}

pub enum ErrorResponse {
    NodeError { error: NodeError },
    FilesystemError { error: FilesystemError },
}

fn fs_error_to_rsp(error: FilesystemError) -> ErrorResponse {
    ErrorResponse::FilesystemError {
        error: error,
    }
}

fn node_error_to_rsp(error: NodeError) -> ErrorResponse {
    ErrorResponse::NodeError {
        error: error,
    }
}

pub enum FilesystemElementType {
    File,
    Directory,
}

pub enum Authority {
    User(String),
    Group(String),
}

pub enum FilesystemElement {
    File {
        properties: FileProperties,
        created_by: Authority,
        modified_by: Authority,
        read: Authority,
        write: Authority,
        node_id: NodeId,

    },
    Directory {
        created_at: Timestamp,
        modified_at: Timestamp,
        read: Authority,
        write: Authority,
        node_id: NodeId,
    },
}

pub enum FilesystemElementProperties {
    File {
        name: String,
        node_id: NodeId,
        revision: FileRevision,
        file_type: FileType,
        size: u64,
    },
    Directory {
        name: String,
        node_id: NodeId,
    },
}

pub enum FileSystemListElement {
    File {
        name: String,
        node_id: NodeId,
        revision: FileRevision,
        file_type: FileType,
        size: u64,
        is_open: bool,
    },
    Directory {
        name: String,
        node_id: NodeId,
        read: Authority,
        write: Authority,
    },
}

pub struct Counters {
    pub active_connections: u32,
    pub number_of_open_files: u32,
    pub number_of_files: u32,
}

pub struct AdminSystemInformation {
    pub certificate_expiration: Timestamp,
}

pub struct SystemInformation {
    pub started_at: Timestamp,
    pub server_id: u64,
    pub admin_system_information: Option<AdminSystemInformation>,
}

pub enum ShutdownReason {
    NodeClosing,
}

pub enum ClientProtocol {
    AuthenticateResponse { result: Result<Id, ErrorResponse> },
    AllocateAuthenticationTokenResponse { result: Result<String, ErrorResponse> },
    CreateFileResponse { result: Result<(NodeId, FileProperties), ErrorResponse> },
    CreateDirectoryResponse { result: Result<NodeId, ErrorResponse> },
    OpenFileResponse { result: Result<(FileAccess, NodeId, FileProperties), ErrorResponse> },
    Shutdown { reason: ShutdownReason },
    CountersResponse { result: Result<Counters, ErrorResponse> },
    QuerySystemResponse { result: Result<SystemInformation, ErrorResponse> },
    QueryFsChildrenResponse { result: Result<Vec<FileSystemListElement>, ErrorResponse> },
    QueryFsElementResponse { result: Result<FilesystemElement, ErrorResponse> },
    QueryFsElementPropertiesResponse { result: Result<FilesystemElementProperties, ErrorResponse> },
    DeleteResponse { result: Result<(), ErrorResponse> },
    AddUserGroupResponse { result: Result<(), ErrorResponse> },
    ModifyUserGroupResponse { result: Result<(), ErrorResponse> },
}

pub enum NodeProtocol {
    AuthenticateWithPasswordRequest { username: String, password: String },
    AuthenticateWithTokenRequest { token: String },
    AllocateAuthenticationTokenRequest { user: Id },
    CreateFileRequest { parent: FileDescriptor, type_of_file: FileType, name: String, user: Id, page_size: Option<u64> },
    CreateDirecotryRequest { parent: FileDescriptor, name: String, user: Id },
    OpenFileRequest { mode: OpenMode, file_descriptor: FileDescriptor, user: Id },
    CountersRequest { user: Id, },
    QuerySystemRequest { user: Id, },
    QueryFsChildrenRequest { user: Id, fd: FileDescriptor, },
    QueryFsElementRequest { user: Id, fd: FileDescriptor, },
    QueryFsElementPropertiesRequest { user: Id, fd: FileDescriptor, fd_parent: FileDescriptor, },
    DeleteRequest { user: Id, fd: FileDescriptor },
    AddUserRequest { user: Id, name: String },
    ModifyUser { user: Id, name: String, password: Option<String>, expiration: Option<Option<Timestamp>> },
    AddGroupRequest { user: Id, name: String },
    ModifyGroup { user: Id, name: String, expiration: Option<Option<Timestamp>> },
    Quit,
}

fn start_signal_listener() -> Result<Receiver<()>, ()> {

    let (sender, receiver) = channel::<()>();
    let mut signal_set: [size_t; 32] = unsafe { MaybeUninit::uninit().assume_init() };
    if unsafe { sigemptyset(signal_set.as_mut_ptr() as _) } != 0 {
        return Err(())
    }
    if unsafe { sigaddset(signal_set.as_mut_ptr() as _, SIGTERM) } != 0 {
        return Err(())
    }
    if unsafe { sigaddset(signal_set.as_mut_ptr() as _, SIGINT) } != 0 {
        return Err(())
    }
    if unsafe { sigprocmask(SIG_SETMASK, signal_set.as_ptr() as _, null_mut()) } != 0 {
        return Err(());
    }

    spawn(move || {
        let mut sig: c_int = 0;
        unsafe { sigwait(signal_set.as_ptr() as _, & mut sig) };
        sender.send(()).unwrap();
    });

    Ok(receiver)
}

struct FilesystemElementAuthorityId {
    read: Id,
    write: Id,
}

struct ClientInfo {
    transmit: Sender<ClientProtocol>,
    receive: Receiver<NodeProtocol>,
    thread_handle: JoinHandle<()>,
}

pub struct NodeSettings {
    pub max_page_size_random_access_file: usize,
    pub max_page_size_blob_file: usize,
    pub max_number_of_files_per_directory: usize,
    pub filesystem_capacity: u64,
    pub socket_buffer_size: u64,
}

pub struct Node {
    server: TlsServer,
    clients: Vec<ClientInfo>,
    filesystem: Filesystem,
    auth: UserAuthority,
    path_workdir: PathBuf,
    crypto: Crypto,
    started_at: Timestamp,
    server_id: u64,
    max_page_size_random_access_file: usize,
    max_page_size_blob_file: usize,
    client_socket_buffer_size: usize,
    max_inactivity_duration_secs: i64,
    authentication_token_duration_secs: i64,
}

impl Node {

    fn path_user_authority(path_workdir: & Path) -> PathBuf {
        path_workdir.join("users")
    }

    fn path_data(path_workdir: & Path) -> PathBuf {
        path_workdir.join("data")
    }

    fn path_filesystem(path_workdir: & Path) -> PathBuf {
        path_workdir.join("fs")
    }

    fn path_node(path_workdir: & Path) -> PathBuf {
        path_workdir.join("node")
    }

    pub fn create(
        crypto: Crypto,
        auth: UserAuthority,
        path_workdir: & Path,
        settings: NodeSettings,
    ) -> Result<(), ()> {

        info!("Creating node, path_workdir={}", path_workdir.display());

        let workdir_it = read_dir(& path_workdir)
            .map_err(| error | error!("Failed to read workdir content, error=\"{}\"", error))
            ? ;

        if workdir_it.count() != 0 {
            error!("Working directory is not empty");
            return Err(());
        }

        let context_auth = crypto.create_context()
            .map_err(| () | log_crypto_context_error())
            ?;

        let context_node_settings = crypto.create_context()
            .map_err(| () | log_crypto_context_error())
            ? ;

        auth.store(context_auth, & Node::path_user_authority(path_workdir))
            .map_err(| () | error!("Failed to store user authority"))
            ? ;

        let path_data_dir = Node::path_data(path_workdir);
        create_dir(& path_data_dir)
            .map_err(| error | error!("Failed to create data dir, error=\"{}\"", error))
            ? ;

        let mut fs = Filesystem::new_with_capacity(
            crypto,
            & path_data_dir,
            settings.filesystem_capacity as usize,
            settings.max_number_of_files_per_directory as usize
        );
        fs.store(& Node::path_filesystem(path_workdir))
            .map_err(| () | error!("Failed to store filesystem"))
            ? ;

        let serialized_node_settings = SerializedNode {
            client_input_buffer_size: settings.socket_buffer_size as u64,
            page_size_for_random_access_files: settings.max_page_size_random_access_file as u64,
            page_size_for_blob_files: settings.max_page_size_blob_file as u64,
        };

        serialized_node_settings.write(context_node_settings, & Node::path_node(path_workdir))
            .map_err(| () | error!("Failed to store node settings"))
            ? ;

        Ok(())
    }

    fn store(& mut self) -> Result<(), ()> {

        info!("Storing node, path_workdir={}", self.path_workdir.display());

        let context_auth = self.crypto.create_context()
            .map_err(| () | log_crypto_context_error())
            ?;

        let context_node_settings = self.crypto.create_context()
            .map_err(| () | log_crypto_context_error())
            ? ;

        self.auth.store(context_auth, & Node::path_user_authority(& self.path_workdir))
            .map_err(| () | error!("Failed to store user authority"))
            ? ;

        self.filesystem.store(& Node::path_filesystem(& self.path_workdir))
            .map_err(| () | error!("Failed to store filesystem"))
            ? ;

        let serialized_node_settings = SerializedNode {
            client_input_buffer_size: self.client_socket_buffer_size as u64,
            page_size_for_random_access_files: self.max_page_size_random_access_file as u64,
            page_size_for_blob_files: self.max_page_size_blob_file as u64,
        };

        serialized_node_settings.write(context_node_settings, & Node::path_node(& self.path_workdir))
            .map_err(| () | error!("Failed to store node settings"))
            ? ;

        Ok(())
    }

    pub fn load(
        crypto: Crypto,
        server: TlsServer,
        path_workdir: & Path,
        max_inactivity_duration_secs: i64,
        authentication_token_duration_secs: i64,
    ) -> Result<Node, ()> {

        info!("Loading node, path_workdir={}", path_workdir.display());

        let context_auth = crypto.create_context()
            .map_err(| () | log_crypto_context_error())
            ?;
        let context_node_settings = crypto.create_context()
            .map_err(| () | log_crypto_context_error())
            ? ;

        let auth = UserAuthority::load(context_auth, & Node::path_user_authority(path_workdir))
            .map_err(| () | error!("Failed to store users"))
            ? ;

        let path_data_dir = Node::path_data(path_workdir);
        let fs = Filesystem::load(crypto.clone(), & path_data_dir, & Node::path_filesystem(path_workdir))
            .map_err(| () | error!("Failed to load filesystem"))
            ? ;

        let settings = SerializedNode::read(context_node_settings, & Node::path_node(path_workdir))
            .map_err(| () | error!("Failed to load node settings"))
            ? ;

        Ok(Node {
            server: server,
            clients: Vec::new(),
            filesystem: fs,
            auth: auth,
            path_workdir: path_workdir.to_path_buf(),
            crypto: crypto,
            max_page_size_random_access_file: settings.page_size_for_random_access_files as usize,
            max_page_size_blob_file: settings.page_size_for_blob_files as usize,
            client_socket_buffer_size: settings.client_input_buffer_size as usize,
            started_at: utc_timestamp(),
            server_id: random::<u64>(),
            max_inactivity_duration_secs: max_inactivity_duration_secs,
            authentication_token_duration_secs: authentication_token_duration_secs,
        })
    }

    pub fn run(& mut self) -> Result<(), ()> {

        let signal_channel = start_signal_listener()
            .map_err(| () | error!("Failed to register interrupt signals"))
            ? ;

        info!("Node ready and waiting for connections");
        let mut node_id_buffer: [NodeId; 20] = [0; 20];

        loop {
            let mut is_processing: bool = false;

            if let Ok(()) = signal_channel.try_recv() {
                info!("Interrupt signal received, shutting down");
                break ;
            }

            let mut communication_to_client_failed: Option<usize> = None;
            for (client_index, client) in self.clients.iter().enumerate() {

                match client.receive.try_recv() {
                    Err(TryRecvError::Disconnected) => {
                        warn!("Failed to receive from client, removing");
                        communication_to_client_failed = Some(client_index);
                        break ;
                    }
                    Err(TryRecvError::Empty) => (),
                    Ok(message) => {
                        is_processing = true;
                        let mut send_failed: bool = false;
                        match message {

                            NodeProtocol::Quit => {

                            },

                            NodeProtocol::AuthenticateWithPasswordRequest {
                                username,
                                password,
                            } => {

                                trace!("Authenticate request, username=\"{}\"", username);

                                let result = self.auth.validate_user(
                                    & username,
                                    & password,
                                    utc_timestamp()
                                ).map_err(| () | node_error_to_rsp(NodeError::InvalidUsernamePassword))
                                    ;

                                send_failed = client.transmit.send(
                                    ClientProtocol::AuthenticateResponse {
                                        result: result,
                                    },
                                ).is_err();
                            },

                            NodeProtocol::AuthenticateWithTokenRequest {
                                token,
                            } => {
                                trace!("Authenticate request, token=\"{}\"", token);
                                let result = self.auth.consume_link_to_id(
                                    & token,
                                    utc_timestamp(),
                                ).map_err(| () | node_error_to_rsp(NodeError::FailedToConsumeAuthenticationToken))
                                    ;

                                send_failed = client.transmit.send(
                                    ClientProtocol::AuthenticateResponse {
                                        result: result
                                    },
                                ).is_err();
                            },

                            NodeProtocol::AllocateAuthenticationTokenRequest {
                                user,
                            } => {
                                trace!("Allocate authnetication token request, user={}", user);
                                let result = self.auth.generate_temporary_link_for_id(
                                    & user,
                                    utc_timestamp() + self.authentication_token_duration_secs,
                                ).map_err(| () | node_error_to_rsp(NodeError::FailedToAllocateAuthenticationToken))
                                    ;

                                send_failed = client.transmit.send(
                                    ClientProtocol::AllocateAuthenticationTokenResponse {
                                        result: result
                                    },
                                ).is_err();
                            },

                            NodeProtocol::CreateFileRequest {
                                parent,
                                type_of_file,
                                name,
                                user,
                                page_size,
                            } => {

                                trace!("Create file request, user={}", user);

                                let result = Node::handle_create_file_req(
                                    & mut node_id_buffer,
                                    & mut self.filesystem,
                                    & mut self.auth,
                                    self.max_page_size_random_access_file,
                                    self.max_page_size_blob_file,
                                    parent,
                                    type_of_file,
                                    name,
                                    user,
                                    page_size
                                );

                                send_failed = client.transmit.send(
                                    ClientProtocol::CreateFileResponse {
                                        result: result
                                    },
                                ).is_err();
                            }

                            NodeProtocol::CreateDirecotryRequest {
                                parent,
                                name,
                                user,
                            } => {

                                trace!("Create directory request, user={}", user);

                                let result = Node::handle_create_directory_req(
                                    & mut node_id_buffer,
                                    & mut self.filesystem,
                                    & mut self.auth,
                                    parent,
                                    name,
                                    user
                                );

                                send_failed = client.transmit.send(
                                    ClientProtocol::CreateDirectoryResponse {
                                        result: result
                                    },
                                ).is_err();
                            }

                            NodeProtocol::OpenFileRequest { mode, file_descriptor, user } => {

                                trace!("Open file request, user={}", user);

                                let result = Node::handle_open_file_request(
                                    & mut node_id_buffer,
                                    & mut self.filesystem,
                                    & mut self.auth,
                                    & mut self.crypto,
                                    mode,
                                    file_descriptor,
                                    user,
                                );

                                send_failed = client.transmit.send(
                                    ClientProtocol::OpenFileResponse {
                                        result: result
                                    },
                                ).is_err();
                            },

                            NodeProtocol::CountersRequest { user } => {

                                trace!("Counters request, user={}", user);

                                let result = Node::handle_counters_request(
                                    & self.clients,
                                    & mut self.filesystem,
                                    & mut self.auth,
                                    user,
                                );

                                send_failed = client.transmit.send(
                                    ClientProtocol::CountersResponse {
                                        result: result
                                    },
                                ).is_err();
                            },

                            NodeProtocol::QuerySystemRequest { user } => {

                                trace!("Query system request, user={}", user);

                                let result = Node::handle_query_system_request(
                                    & mut self.auth,
                                    & self.server,
                                    self.started_at,
                                    self.server_id,
                                    user,
                                );

                                send_failed = client.transmit.send(
                                    ClientProtocol::QuerySystemResponse {
                                        result: result
                                    },
                                ).is_err();
                            },

                            NodeProtocol::QueryFsChildrenRequest {
                                user,
                                fd,
                            } => {

                                trace!("Query fs children request, user={}", user);

                                let result = Node::handle_query_fs_children_request(
                                    & mut node_id_buffer,
                                    & mut self.filesystem,
                                    & mut self.auth,
                                    user,
                                    fd,
                                );

                                send_failed = client.transmit.send(
                                    ClientProtocol::QueryFsChildrenResponse {
                                        result: result
                                    },
                                ).is_err();
                            },

                            NodeProtocol::QueryFsElementPropertiesRequest {
                                user,
                                fd,
                                fd_parent,
                            } => {

                                trace!("Query fs element properties, user={}", user);

                                let result = Node::handle_query_fs_element_properties_request(
                                    & mut node_id_buffer,
                                    & mut self.filesystem,
                                    & mut self.auth,
                                    & mut self.crypto,
                                    user,
                                    fd,
                                    fd_parent,
                                );

                                send_failed = client.transmit.send(
                                    ClientProtocol::QueryFsElementPropertiesResponse {
                                        result: result,
                                    },
                                ).is_err();
                            },

                            NodeProtocol::QueryFsElementRequest {
                                user,
                                fd,
                            } => {

                                trace!("Query fs element, user={}", user);

                                let result = Node::handle_query_fs_element_request(
                                    & mut node_id_buffer,
                                    & mut self.filesystem,
                                    & mut self.auth,
                                    & mut self.crypto,
                                    user,
                                    fd,
                                );

                                send_failed = client.transmit.send(
                                    ClientProtocol::QueryFsElementResponse {
                                        result: result,
                                    },
                                ).is_err();
                            },

                            NodeProtocol::DeleteRequest {
                                user,
                                fd,
                            } => {

                                trace!("Delete, user={}, fd={}", user, fd);

                                let result = Node::handle_delete_request(
                                    & mut node_id_buffer,
                                    & mut self.filesystem,
                                    & mut self.auth,
                                    & mut self.crypto,
                                    user,
                                    fd,
                                );

                                send_failed = client.transmit.send(
                                    ClientProtocol::DeleteResponse {
                                        result: result,
                                    },
                                ).is_err();
                            },

                            NodeProtocol::AddUserRequest {
                                user,
                                name,
                            } => {
                                trace!("Create user, user={}, name={}", user, name);

                                let result = Node::handle_create_user(
                                    & mut self.auth,
                                    user,
                                    name,
                                );

                                send_failed = client.transmit.send(
                                    ClientProtocol::AddUserGroupResponse {
                                        result: result,
                                    },
                                ).is_err();
                            },

                            NodeProtocol::ModifyUser {
                                user,
                                name,
                                password,
                                expiration,
                            } => {

                                trace!("Modify user, user={}, name={}", user, name);

                                let result = Node::handle_modify_user(
                                    & mut self.auth,
                                    user,
                                    name,
                                    password,
                                    expiration,
                                );

                                send_failed = client.transmit.send(
                                    ClientProtocol::AddUserGroupResponse {
                                        result: result,
                                    },
                                ).is_err();
                            },

                            NodeProtocol::AddGroupRequest {
                                user,
                                name
                            } => {
                                trace!("Create group, user={}, name={}", user, name);

                                let result = Node::handle_create_group(
                                    & mut self.auth,
                                    user,
                                    name,
                                );

                                send_failed = client.transmit.send(
                                    ClientProtocol::AddUserGroupResponse {
                                        result: result,
                                    },
                                ).is_err();
                            },

                            NodeProtocol::ModifyGroup {
                                user,
                                name,
                                expiration,
                            } => {

                                trace!("Modify group, user={}, name={}", user, name);

                                let result = Node::handle_modify_group(
                                    & mut self.auth,
                                    user,
                                    name,
                                    expiration,
                                );

                                send_failed = client.transmit.send(
                                    ClientProtocol::AddUserGroupResponse {
                                        result: result,
                                    },
                                ).is_err();
                            },
                        }

                        if send_failed {
                            communication_to_client_failed = Some(client_index);
                            break ;
                        }
                    }
                }
            }

            if let Some(index) = communication_to_client_failed {
                info!("Removing index={}", index);
                let client = self.clients.remove(index);
                let _ = client.thread_handle.join();
            }

            match self.server.accept() {
                Ok(None) => (),
                Ok(Some(connection)) => {
                    is_processing = true;

                    let (tx_node, rx_node) = channel::<ClientProtocol>();
                    let (tx_client, rx_client) = channel::<NodeProtocol>();
                    let buffer_size = self.client_socket_buffer_size;
                    let max_inactivity_duration_secs = self.max_inactivity_duration_secs;

                    let handle = spawn( move || {
                        match Client::new(
                            connection,
                            rx_node,
                            tx_client,
                            buffer_size,
                            max_inactivity_duration_secs,
                        ) {
                            Ok(mut client) => {
                                client.process();
                            },
                            Err(()) => {
                                error!("Failed to create client");
                            }
                        }
                    });

                    self.clients.push(ClientInfo {
                        transmit: tx_node,
                        receive: rx_client,
                        thread_handle: handle,
                    });
                },
                Err(()) => {
                    error!("Failed to accept new connection, closing");
                    break
                },
            };

            if ! is_processing {
                sleep(Duration::from_millis(100));
            }
        }

        for client in self.clients.iter() {
            let _ = client.transmit.send(ClientProtocol::Shutdown {
                reason: ShutdownReason::NodeClosing,
            });
        }

        for client in self.clients.drain(..) {
            if let Err(_) = client.thread_handle.join() {
                error!("Failed to join client");
            }
        }

        self.store()
            .map_err(| () | error!("Failed to store node"))
            ? ;

        Ok(())
    }

    fn handle_create_file_req(
        node_id_buffer: & mut [NodeId],
        filesystem: & mut Filesystem,
        auth: & mut UserAuthority,
        max_page_size_random_access_file: usize,
        max_page_size_blob_file: usize,
        parent_fd: FileDescriptor,
        type_of_file: FileType,
        name: String,
        user: Id,
        requested_page_size: Option<u64>,
    ) -> Result<(NodeId, FileProperties), ErrorResponse> {

        let parent_id = Node::resolve_file_descriptor(
            node_id_buffer,
            filesystem,
            parent_fd
        ) ? ;

        {
            let ref parent = filesystem.node(& parent_id).unwrap();
            let parent = parent.to_directory()
                .map_err(| _ | node_error_to_rsp(NodeError::ParentIsNotDirectory))
                ? ;

            auth.is_authorized(parent.write(), & user, utc_timestamp())
                .map_err(| () | node_error_to_rsp(NodeError::UnauthorizedOperation))
                ? ;
        }

        let page_size = match requested_page_size {
            Some(value) => {
                let value = value as usize;
                let max_page_size = match type_of_file {
                    FileType::RandomAccess => max_page_size_random_access_file,
                    FileType::Blob => max_page_size_blob_file,
                };
                if value > max_page_size {
                    return Err(node_error_to_rsp(NodeError::InvalidPageSize));
                }
                value
            },
            None => {
                match type_of_file {
                    FileType::RandomAccess => max_page_size_random_access_file,
                    FileType::Blob => max_page_size_blob_file,
                }
            }
        };

        filesystem.create_file(
            & parent_id,
            & name,
            user,
            type_of_file,
            page_size,
        ).map_err(fs_error_to_rsp)
    }

    fn handle_create_directory_req(
        node_id_buffer: & mut [NodeId],
        filesystem: & mut Filesystem,
        auth: & mut UserAuthority,
        parent_fd: FileDescriptor,
        name: String,
        user: Id
    ) -> Result<NodeId, ErrorResponse> {

        let parent_id = Node::resolve_file_descriptor(
            node_id_buffer,
            filesystem,
            parent_fd
        ) ? ;

        {
            let ref parent = filesystem.node(& parent_id).unwrap();
            let parent = parent.to_directory()
                .map_err(| _ | node_error_to_rsp(NodeError::ParentIsNotDirectory))
                ? ;

            auth.is_authorized(parent.write(), & user, utc_timestamp())
                .map_err(| () | node_error_to_rsp(NodeError::UnauthorizedOperation))
                ? ;
        }

        filesystem.to_directory(
            & parent_id,
            & name,
            user
        ).map_err(fs_error_to_rsp)
    }


    fn handle_open_file_request(
        node_id_buffer: & mut [NodeId],
        fs: & mut Filesystem,
        auth: & mut UserAuthority,
        crypto: & mut Crypto,
        mode: OpenMode,
        file_descriptor: FileDescriptor,
        user: Id,
    ) -> Result<(FileAccess, NodeId, FileProperties), ErrorResponse> {

        let node_id = Node::resolve_file_descriptor(
            node_id_buffer,
            fs,
            file_descriptor
        ) ? ;

        let (properties, file_auth) = Node::resolve_file_properties(& node_id, fs, crypto) ? ;

        let file = fs.mut_file(& node_id)
            .map_err(fs_error_to_rsp)
            ? ;

        let access = match mode {
            OpenMode::Read => file_auth.read,
            OpenMode::ReadWrite => file_auth.write,
        };

        auth.is_authorized(& access, & user, utc_timestamp())
            .map_err(| () | node_error_to_rsp(NodeError::UnauthorizedOperation))
                  ? ;

        let access = file.open(& crypto, user)
            .map_err(| () | node_error_to_rsp(NodeError::InternalError))
            ? ;

        Ok((access, node_id, properties))
    }

    fn handle_counters_request(
        clients: & Vec<ClientInfo>,
        fs: & mut Filesystem,
        auth: & mut UserAuthority,
        user: Id,
    ) -> Result<Counters, ErrorResponse> {

        auth.is_authorized(& ADMIN_GROUP, & user, utc_timestamp())
            .map_err(| () | node_error_to_rsp(NodeError::UnauthorizedOperation))
            ? ;

        Ok(Counters {
            active_connections: clients.len() as u32,
            number_of_files: fs.number_of_files() as u32,
            number_of_open_files: fs.number_of_open_files() as u32,
        })
    }

    fn handle_query_system_request(
        auth: & mut UserAuthority,
        server: & TlsServer,
        started_at: Timestamp,
        server_id: u64,
        user: Id,
    ) -> Result<SystemInformation, ErrorResponse> {

        let admin_system_information = {
            if auth.is_authorized(& ADMIN_GROUP, & user, utc_timestamp()).is_ok() {
                Some(AdminSystemInformation {
                    certificate_expiration: server.certificate_expiration(),
                })
            } else {
                None
            }
        };

        Ok(SystemInformation {
            started_at: started_at,
            server_id: server_id,
            admin_system_information: admin_system_information,
        })
    }

    fn handle_query_fs_children_request(
        node_id_buffer: & mut [NodeId],
        fs: & mut Filesystem,
        auth: & mut UserAuthority,
        user: Id,
        file_descriptor: FileDescriptor,
    ) -> Result<Vec<FileSystemListElement>, ErrorResponse> {

        let node_id = Node::resolve_file_descriptor(
            node_id_buffer,
            fs,
            file_descriptor
        ) ? ;

        let children: Vec<Child> = {
            let node = fs.node(& node_id)
                .map_err(fs_error_to_rsp)
                ? ;

            let directory = node.to_directory()
                .map_err(fs_error_to_rsp)
                ? ;

            auth.is_authorized(& directory.read(), & user, utc_timestamp())
                .map_err(| () | node_error_to_rsp(NodeError::UnauthorizedOperation))
                ? ;

            directory.clone_children()
        };

        let mut result = Vec::with_capacity(children.len());
        for ref child in children {
            if fs.node(& child.node_id).unwrap().is_not_set() {
                panic!();
            }

            let is_file = fs.node(& child.node_id).unwrap().is_file();
            if is_file {
                let file = fs.mut_file(& child.node_id).unwrap();
                let is_open = file.is_open();
                let properties = file.cached_properties().unwrap();
                result.push(
                    FileSystemListElement::File {
                        name: child.name.clone(),
                        node_id: child.node_id.clone(),
                        revision: properties.revision,
                        file_type: properties.file_type,
                        size: properties.size,
                        is_open: is_open,
                    })
            } else {
                let dir = fs.node(& child.node_id)
                    .unwrap()
                    .to_directory()
                    .unwrap()
                    ;

                let read = Node::resolve_id(auth, dir.read())
                    .map_err(| () | node_error_to_rsp(NodeError::FailedToResolveAuthority))
                    ? ;
                let write = Node::resolve_id(auth, dir.write())
                    .map_err(| () | node_error_to_rsp(NodeError::FailedToResolveAuthority))
                    ? ;
                result.push(
                    FileSystemListElement::Directory {
                        name: child.name.clone(),
                        node_id: child.node_id.clone(),
                        read: read,
                        write: write,
                    })
            }
        }
        Ok(result)
    }

    fn handle_query_fs_element_properties_request(
        node_id_buffer: & mut [NodeId],
        fs: & mut Filesystem,
        auth: & mut UserAuthority,
        crypto: & mut Crypto,
        user: Id,
        file_descriptor: FileDescriptor,
        parent_file_descriptor: FileDescriptor,
    ) -> Result<FilesystemElementProperties, ErrorResponse> {

        let node_id = Node::resolve_file_descriptor(
            node_id_buffer,
            fs,
            file_descriptor
        ) ? ;

        let parent_node_id = Node::resolve_file_descriptor(
            node_id_buffer,
            fs,
            parent_file_descriptor
        ) ? ;

        let (name, auth_read) = {
            let parent_node = fs.node(& parent_node_id)
                .map_err(fs_error_to_rsp)
                ? ;

            let parent = parent_node.to_directory()
                .map_err(| _ | node_error_to_rsp(NodeError::ParentIsNotDirectory))
                ? ;

            let node_index = parent.child_with_node_id(& node_id)
                .map_err(| () | node_error_to_rsp(NodeError::UnknownFile))
                ? ;

            let name = & parent.children().nth(node_index).unwrap().name;

            (name.clone(), parent.read().clone())
        };

        auth.is_authorized(& auth_read, & user, utc_timestamp())
            .map_err(| () | node_error_to_rsp(NodeError::UnauthorizedOperation))
            ? ;

        let is_file = fs.node(& node_id).unwrap().is_file();
        if is_file {

            let file = fs.mut_file(& node_id).unwrap();
            let properties = file.properties(crypto).unwrap();
            Ok(FilesystemElementProperties::File {
                name: name,
                node_id: node_id,
                file_type: properties.file_type,
                revision: properties.revision,
                size: properties.size,
            })
        } else {
            Ok(FilesystemElementProperties::Directory {
                name: name,
                node_id: node_id,
            })
        }
    }

    fn handle_query_fs_element_request(
        node_id_buffer: & mut [NodeId],
        fs: & mut Filesystem,
        auth: & mut UserAuthority,
        crypto: & mut Crypto,
        user: Id,
        file_descriptor: FileDescriptor,
    ) -> Result<FilesystemElement, ErrorResponse> {

        let node_id = Node::resolve_file_descriptor(
            node_id_buffer,
            fs,
            file_descriptor
        ) ? ;

        let is_file = fs.node(& node_id).unwrap().is_file();
        if is_file {

            let (properties, file_auth) = Node::resolve_file_properties(& node_id, fs, crypto) ? ;
            let read = Node::resolve_id(auth, & file_auth.read)
                .map_err(| () | node_error_to_rsp(NodeError::FailedToResolveAuthority))
                ? ;

            let write = Node::resolve_id(auth, & file_auth.write)
                .map_err(| () | node_error_to_rsp(NodeError::FailedToResolveAuthority))
                ? ;

            let created_by = Node::resolve_id(auth, & properties.created_by)
                .map_err(| () | node_error_to_rsp(NodeError::FailedToResolveAuthority))
                ? ;

            let modified_by = Node::resolve_id(auth, & properties.modified_by)
                .map_err(| () | node_error_to_rsp(NodeError::FailedToResolveAuthority))
                ? ;

            let desc = FilesystemElement::File {
                properties: properties,
                created_by: created_by,
                modified_by: modified_by,
                read: read,
                write: write,
                node_id: node_id,
            };

            auth.is_authorized(& file_auth.read, & user, utc_timestamp())
                .map_err(| () | node_error_to_rsp(NodeError::UnauthorizedOperation))
                ? ;

            Ok(desc)

        } else {

            let node = fs.node(& node_id)
                .map_err(fs_error_to_rsp)
                ? ;

            match *node {
                FsNode::Directory { ref directory } => {
                    let read = Node::resolve_id(auth, directory.read())
                        .map_err(| () | node_error_to_rsp(NodeError::FailedToResolveAuthority))
                        ? ;

                    let write = Node::resolve_id(auth, directory.write())
                        .map_err(| () | node_error_to_rsp(NodeError::FailedToResolveAuthority))
                        ? ;

                    let desc = FilesystemElement::Directory {
                        created_at: directory.created(),
                        modified_at: directory.modified(),
                        read: read,
                        write: write,
                        node_id: node_id,
                    };

                    auth.is_authorized(& directory.read(), & user, utc_timestamp())
                        .map_err(| () | node_error_to_rsp(NodeError::UnauthorizedOperation))
                        ? ;

                    return Ok(desc);
                },
                FsNode::File { .. } => panic!(),
                FsNode::NotSet { } => panic!(),
            }
        }
    }

    fn handle_delete_request(
        node_id_buffer: & mut [NodeId],
        fs: & mut Filesystem,
        auth: & mut UserAuthority,
        crypto: & mut Crypto,
        user: Id,
        fd: FileDescriptor,
    ) -> Result<(), ErrorResponse> {

        let node_id = Node::resolve_file_descriptor(
            node_id_buffer,
            fs,
            fd
        ) ? ;

        let parent_node_id = {
            let is_file = fs.node(& node_id).unwrap().is_file();

            if ! is_file {
                fs.node(& node_id)
                    .unwrap()
                    .to_directory()
                    .unwrap()
                    .parent()
            } else {
                let file = fs.mut_file(& node_id).unwrap();
                let properties = file.properties(& crypto)
                    .map_err(| () | node_error_to_rsp(NodeError::InternalCommunicationError))
                    ? ;
                properties.parent
            }
        };

        let index = {
            let node = fs.node(& parent_node_id)
                .map_err(fs_error_to_rsp)
                ? ;

            let directory = node.to_directory()
                .map_err(| _ | node_error_to_rsp(NodeError::ParentIsNotDirectory))
                ? ;

            auth.is_authorized(& directory.write(), & user, utc_timestamp())
                .map_err(| () | node_error_to_rsp(NodeError::UnauthorizedOperation))
                ? ;

            let index = directory.child_with_node_id(& node_id)
                .map_err(| () | node_error_to_rsp(NodeError::UnknownFile))
                ? ;

            index
        };

        fs.delete(& parent_node_id, index, node_id)
            .map_err(fs_error_to_rsp)
            ? ;

        Ok(())
    }

    fn handle_create_user(
        auth: & mut UserAuthority,
        user: Id,
        name: String,
    ) -> Result<(), ErrorResponse> {

        let current_time = utc_timestamp();
        let default_password = "";

        auth.is_authorized(& ADMIN_GROUP, & user, current_time)
            .map_err(| () | node_error_to_rsp(NodeError::UnauthorizedOperation))
            ? ;

        auth.add_user(
            & name,
            default_password,
            Some(current_time - 1)
        )
            .map_err(| () | node_error_to_rsp(NodeError::AuthorityError))
            ? ;

        Ok(())
    }

    fn handle_modify_user(
        auth: & mut UserAuthority,
        user: Id,
        name: String,
        password: Option<String>,
        expiration: Option<Option<Timestamp>>,
    ) -> Result<(), ErrorResponse> {

        let current_time = utc_timestamp();

        let target_user = auth.resolve_user_id(& name)
            .map_err(| () | node_error_to_rsp(NodeError::UnknownAuthority))
            ? ;

        if user != target_user && auth.is_authorized(& ADMIN_GROUP, & user, current_time).is_err() {
            return Err(node_error_to_rsp(NodeError::UnauthorizedOperation));
        }

        if let Some(pw) = password {
            auth.modify_user_password(& target_user, & pw)
                .map_err(| () | node_error_to_rsp(NodeError::AuthorityError))
                ? ;
        }

        if let Some(ex) = expiration {
            auth.modify_user_expiration(& target_user, ex)
                .map_err(| () | node_error_to_rsp(NodeError::AuthorityError))
                ? ;
        }

        Ok(())
    }

    fn handle_create_group(
        auth: & mut UserAuthority,
        user: Id,
        name: String,
    ) -> Result<(), ErrorResponse> {

        let current_time = utc_timestamp();

        auth.is_authorized(& ADMIN_GROUP, & user, current_time)
            .map_err(| () | node_error_to_rsp(NodeError::UnauthorizedOperation))
            ? ;

        auth.add_group(
            & name,
            Some(current_time - 1)
        )
            .map_err(| () | node_error_to_rsp(NodeError::AuthorityError))
            ? ;

        Ok(())
    }

    fn handle_modify_group(
        auth: & mut UserAuthority,
        user: Id,
        name: String,
        expiration: Option<Option<Timestamp>>,
    ) -> Result<(), ErrorResponse> {

        let current_time = utc_timestamp();

        let target_group = auth.resolve_group_id(& name)
            .map_err(| () | node_error_to_rsp(NodeError::UnknownAuthority))
            ? ;

        auth.is_authorized(& ADMIN_GROUP, & user, current_time)
            .map_err(| () | node_error_to_rsp(NodeError::UnauthorizedOperation))
            ? ;

        if let Some(ex) = expiration {
            auth.modify_group_expiration(& target_group, ex)
                .map_err(| () | node_error_to_rsp(NodeError::AuthorityError))
                ? ;
        }

        Ok(())
    }

    fn resolve_file_properties(
        file_node_id: & NodeId,
        fs: & mut Filesystem,
        crypto: & mut Crypto,
    ) -> Result<(FileProperties, FilesystemElementAuthorityId), ErrorResponse> {

        let properties = fs.mut_file(& file_node_id)
            .map_err(fs_error_to_rsp)
            .map(| file |
                 file.properties(& crypto)
                 .map_err(| () | node_error_to_rsp(NodeError::InternalCommunicationError))
            )
            ? ? ;

        let parent = fs.node(& properties.parent)
            .map_err(fs_error_to_rsp)
            ? ;

        let directory = parent.to_directory()
            .map_err(| _ | node_error_to_rsp(NodeError::ParentIsNotDirectory))
            ? ;

        Ok((
            properties,
            FilesystemElementAuthorityId {
                read: directory.read().clone(),
                write: directory.write().clone(),
            }
        ))
    }

    fn resolve_id(
        auth: & UserAuthority,
        id: & Id,
    ) -> Result<Authority, ()> {
        let name = auth.resolve_id_name(id) ? ;
        match *id {
            Id::User(_) => Ok(Authority::User(name)),
            Id::Group(_) => Ok(Authority::Group(name)),
        }
    }

    fn resolve_file_descriptor(
        node_id_buffer: & mut [NodeId],
        filesystem: & Filesystem,
        file_descriptor: FileDescriptor
    ) -> Result<NodeId, ErrorResponse> {
        match file_descriptor {
            FileDescriptor::NodeId(id) => {
                let _ = filesystem.node(& id)
                    .map_err(fs_error_to_rsp)
                    ? ;

                Ok(id)
            },
            FileDescriptor::Path(path) => {
                let size = filesystem.resolve_path_from_root(
                    & path,
                    node_id_buffer
                ).map_err(fs_error_to_rsp)
                    ? ;

                Ok(node_id_buffer[size - 1])
            },
        }
    }
}

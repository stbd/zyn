use std::fs::{ create_dir };
use std::mem::{ uninitialized };
use std::path::{ Path, PathBuf };
use std::ptr::{ null_mut };
use std::sync::mpsc::{ channel, Sender, Receiver, TryRecvError };
use std::thread::{ sleep, spawn, JoinHandle };
use std::time::{ Duration };
use std::vec::{ Vec };

use libc::{ sigwait, sigemptyset, sigaddset, SIGTERM, SIGINT, c_int, size_t, sigprocmask, SIG_SETMASK };

use node::client::{ Client };
use node::common::{ NodeId, FileDescriptor, OpenMode, FileRevision, ADMIN_GROUP, Timestamp, FileType, log_crypto_context_error, utc_timestamp };
use node::connection::{ Server };
use node::crypto::{ Crypto };
use node::file_handle::{ FileAccess, FileProperties };
use node::filesystem::{ Filesystem, FilesystemError, Node as FsNode };
use node::user_authority::{ UserAuthority, Id };

pub enum NodeError {
    InvalidUsernamePassword,
    ParentIsNotFolder,
    UnknownAuthority,
    AuthorityError,
    UnauthorizedOperation,
    InternalCommunicationError,
    InternalError,
    UnknownFile,
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
    Folder,
}

pub struct FilesystemElementAuthority {
    pub read: String,
    pub write: String,
}

pub enum FilesystemElement {
    File {
        properties: FileProperties,
        authority: FilesystemElementAuthority
    },
    Folder {
        created_at: Timestamp,
        modified_at: Timestamp,
        authority: FilesystemElementAuthority,
    },
}

pub struct Counters {
    pub active_connections: u32,
}

pub enum ClientProtocol {
    AuthenticateResponse { result: Result<Id, ErrorResponse> },
    CreateFilesystemElementResponse { result: Result<NodeId, ErrorResponse> },
    OpenFileResponse { result: Result<(FileAccess, NodeId, FileRevision, FileType, u64), ErrorResponse> },
    Shutdown { reason: String },
    CountersResponse { result: Result<Counters, ErrorResponse> },
    QueryListResponse { result: Result<Vec<(String, NodeId, FilesystemElementType)>, ErrorResponse> },
    QueryFilesystemResponse { result: Result<FilesystemElement, ErrorResponse> },
    DeleteResponse { result: Result<(), ErrorResponse> },
    AddUserGroupResponse { result: Result<(), ErrorResponse> },
    ModifyUserGroupResponse { result: Result<(), ErrorResponse> },
    Quit,
}

pub enum NodeProtocol {
    AuthenticateRequest { username: String, password: String },
    CreateFileRequest { parent: FileDescriptor, type_of_file: FileType, name: String, user: Id },
    CreateFolderRequest { parent: FileDescriptor, name: String, user: Id },
    OpenFileRequest { mode: OpenMode, file_descriptor: FileDescriptor, user: Id },
    CountersRequest { user: Id, },
    QueryListRequest { user: Id, fd: FileDescriptor, },
    QueryFilesystemRequest { user: Id, fd: FileDescriptor, },
    DeleteRequest { user: Id, fd: FileDescriptor },
    AddUserRequest { user: Id, name: String },
    ModifyUser { user: Id, name: String, password: Option<String>, expiration: Option<Option<Timestamp>> },
    AddGroupRequest { user: Id, name: String },
    ModifyGroup { user: Id, name: String, expiration: Option<Option<Timestamp>> },
    Quit,
}

fn start_signal_listener() -> Result<Receiver<()>, ()> {

    let (sender, receiver) = channel::<()>();
    let mut signal_set: [size_t; 32] = unsafe { uninitialized() };
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

pub struct Node {
    server: Server,
    clients: Vec<ClientInfo>,
    filesystem: Filesystem,
    auth: UserAuthority,
    path_workdir: PathBuf,
    crypto: Crypto,
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

    pub fn create(
        crypto: Crypto,
        auth: UserAuthority,
        path_workdir: & Path,
    ) -> Result<(), ()> {

        info!("Creating node, path_workdir={}", path_workdir.display());

        let context = crypto.create_context()
            .map_err(| () | log_crypto_context_error())
            ?;

        auth.store(context, & Node::path_user_authority(path_workdir))
            .map_err(| () | error!("Failed to store user authority"))
            ? ;

        let fs = Filesystem::new(crypto, path_workdir);
        fs.store(& Node::path_filesystem(path_workdir))
            .map_err(| () | error!("Failed to store filesystem"))
            ? ;

        Ok(())
    }

    fn store(& mut self) -> Result<(), ()> {

        info!("Storing node, path_workdir={}", self.path_workdir.display());

        let context = self.crypto.create_context()
            .map_err(| () | log_crypto_context_error())
            ?;

        self.auth.store(context, & Node::path_user_authority(& self.path_workdir))
            .map_err(| () | error!("Failed to store user authority"))
            ? ;

        self.filesystem.store(& Node::path_filesystem(& self.path_workdir))
            .map_err(| () | error!("Failed to store filesystem"))
            ? ;
        Ok(())
    }

    pub fn load(crypto: Crypto, server: Server, path_workdir: & Path) -> Result<Node, ()> {

        info!("Loading node, path_workdir={}", path_workdir.display());

        let context = crypto.create_context()
            .map_err(| () | log_crypto_context_error())
            ?;

        let auth = UserAuthority::load(context, & Node::path_user_authority(path_workdir))
            .map_err(| () | error!("Failed to store users"))
            ? ;

        let path_data_dir = Node::path_data(path_workdir);
        create_dir(& path_data_dir)
            .map_err(| error | error!("failed to create data dir, error=\"{}\"", error))
            ? ;


        let fs = Filesystem::load(crypto.clone(), & path_data_dir, & Node::path_filesystem(path_workdir))
            .map_err(| () | error!("Failed to load filesystem"))
            ? ;

        Ok(Node {
            server: server,
            clients: Vec::new(),
            filesystem: fs,
            auth: auth,
            path_workdir: path_workdir.to_path_buf(),
            crypto: crypto,
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

                            NodeProtocol::AuthenticateRequest {
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

                            NodeProtocol::CreateFileRequest {
                                parent,
                                type_of_file,
                                name,
                                user,
                            } => {

                                trace!("Create file request, user={}", user);

                                let result = Node::handle_create_file_req(
                                    & mut node_id_buffer,
                                    & mut self.filesystem,
                                    & mut self.auth,
                                    parent,
                                    type_of_file,
                                    name,
                                    user
                                );

                                send_failed = client.transmit.send(
                                    ClientProtocol::CreateFilesystemElementResponse {
                                        result: result
                                    },
                                ).is_err();
                            }

                            NodeProtocol::CreateFolderRequest {
                                parent,
                                name,
                                user,
                            } => {

                                trace!("Create folder request, user={}", user);

                                let result = Node::handle_create_folder_req(
                                    & mut node_id_buffer,
                                    & mut self.filesystem,
                                    & mut self.auth,
                                    parent,
                                    name,
                                    user
                                );

                                send_failed = client.transmit.send(
                                    ClientProtocol::CreateFilesystemElementResponse {
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
                                    & mut self.auth,
                                    user,
                                );

                                send_failed = client.transmit.send(
                                    ClientProtocol::CountersResponse {
                                        result: result
                                    },
                                ).is_err();
                            },

                            NodeProtocol::QueryListRequest {
                                user,
                                fd,
                            } => {

                                trace!("Query list request, user={}", user);

                                let result = Node::handle_query_list_request(
                                    & mut node_id_buffer,
                                    & self.filesystem,
                                    & mut self.auth,
                                    user,
                                    fd,
                                );

                                send_failed = client.transmit.send(
                                    ClientProtocol::QueryListResponse {
                                        result: result
                                    },
                                ).is_err();
                            },

                            NodeProtocol::QueryFilesystemRequest {
                                user,
                                fd,
                            } => {

                                trace!("Query filessytem, user={}", user);

                                let result = Node::handle_query_filesystem_request(
                                    & mut node_id_buffer,
                                    & mut self.filesystem,
                                    & mut self.auth,
                                    & mut self.crypto,
                                    user,
                                    fd,
                                );

                                send_failed = client.transmit.send(
                                    ClientProtocol::QueryFilesystemResponse {
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
                    let handle = spawn( move || {
                        let mut client = Client::new(
                            connection,
                            rx_node,
                            tx_client);
                        client.process();
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
                reason: String::from("Node shutting down")
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
        parent_fd: FileDescriptor,
        type_of_file: FileType,
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
            let parent = parent.to_folder()
                .map_err(| _ | node_error_to_rsp(NodeError::ParentIsNotFolder))
                ? ;

            auth.is_authorized(parent.write(), & user, utc_timestamp())
                .map_err(| () | node_error_to_rsp(NodeError::UnauthorizedOperation))
                ? ;
        }

        filesystem.create_file(
            & parent_id,
            & name,
            user,
            type_of_file,
        ).map_err(fs_error_to_rsp)
    }

    fn handle_create_folder_req(
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
            let parent = parent.to_folder()
                .map_err(| _ | node_error_to_rsp(NodeError::ParentIsNotFolder))
                ? ;

            auth.is_authorized(parent.write(), & user, utc_timestamp())
                .map_err(| () | node_error_to_rsp(NodeError::UnauthorizedOperation))
                ? ;
        }

        filesystem.create_folder(
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
    ) -> Result<(FileAccess, NodeId, FileRevision, FileType, u64), ErrorResponse> {

        let node_id = Node::resolve_file_descriptor(
            node_id_buffer,
            fs,
            file_descriptor
        ) ? ;

        let (properties, file_auth) = Node::resolve_file_properties(& node_id, fs, crypto) ? ;

        let mut file = fs.mut_file(& node_id)
            .map_err(fs_error_to_rsp)
            ? ;

        let file_size = 0; // todo: use properties.size();
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

        Ok((access, node_id, properties.revision, properties.file_type, file_size))
    }

    fn handle_counters_request(
        clients: & Vec<ClientInfo>,
        auth: & mut UserAuthority,
        user: Id,
    ) -> Result<Counters, ErrorResponse> {

        auth.is_authorized(& ADMIN_GROUP, & user, utc_timestamp())
            .map_err(| () | node_error_to_rsp(NodeError::UnauthorizedOperation))
            ? ;

        Ok(Counters {
            active_connections: clients.len() as u32,
        })
    }

    fn handle_query_list_request(
        node_id_buffer: & mut [NodeId],
        fs: & Filesystem,
        auth: & mut UserAuthority,
        user: Id,
        file_descriptor: FileDescriptor,
    ) -> Result<Vec<(String, NodeId, FilesystemElementType)>, ErrorResponse> {

        let node_id = Node::resolve_file_descriptor(
            node_id_buffer,
            fs,
            file_descriptor
        ) ? ;

        let node = fs.node(& node_id)
            .map_err(fs_error_to_rsp)
            ? ;

        let folder = node.to_folder()
            .map_err(fs_error_to_rsp)
            ? ;

        auth.is_authorized(& folder.read(), & user, utc_timestamp())
            .map_err(| () | node_error_to_rsp(NodeError::UnauthorizedOperation))
            ? ;

        let mut result = Vec::with_capacity(folder.number_of_children());
        for ref child in folder.children() {
            let type_of = match *fs.node(& child.node_id).unwrap() {
                FsNode::Folder { .. } => FilesystemElementType::Folder,
                FsNode::File { .. } => FilesystemElementType::File,
                FsNode::NotSet { .. } => panic!(),
            };

            result.push((child.name.clone(), child.node_id.clone(), type_of))
        }

        Ok(result)
    }

    fn handle_query_filesystem_request(
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

        // Currently files and folders are handled separately
        // as file needs to load metadata which requires
        // mutable access

        {
            let node = fs.node(& node_id)
                .map_err(fs_error_to_rsp)
                ? ;

            match *node {
                FsNode::Folder { ref folder } => {

                    let read = auth.resolve_name(folder.read())
                        .map_err(| () | node_error_to_rsp(NodeError::InternalError))
                        ? ;
                    let write = auth.resolve_name(folder.write())
                        .map_err(| () | node_error_to_rsp(NodeError::InternalError))
                        ? ;

                    let desc = FilesystemElement::Folder {
                        created_at: folder.created(),
                        modified_at: folder.modified(),
                        authority: FilesystemElementAuthority {
                            read: read,
                            write: write,
                        },
                    };

                    auth.is_authorized(& folder.read(), & user, utc_timestamp())
                        .map_err(| () | node_error_to_rsp(NodeError::UnauthorizedOperation))
                        ? ;

                    return Ok(desc);
                },
                FsNode::File { .. } => (),
                FsNode::NotSet { } => panic!(),
            };
        }

        let (properties, file_auth) = Node::resolve_file_properties(& node_id, fs, crypto) ? ;

        let read = auth.resolve_name(& file_auth.read)
            .map_err(| () | node_error_to_rsp(NodeError::InternalError))
            ? ;

        let write = auth.resolve_name(& file_auth.write)
            .map_err(| () | node_error_to_rsp(NodeError::InternalError))
            ? ;

        let desc = FilesystemElement::File {
            properties: properties,
            authority: FilesystemElementAuthority {
                read: read,
                write: write,
            },
        };

        auth.is_authorized(& file_auth.read, & user, utc_timestamp())
            .map_err(| () | node_error_to_rsp(NodeError::UnauthorizedOperation))
            ? ;

        Ok(desc)
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
                    .to_folder()
                    .unwrap()
                    .parent()
            } else {
                let mut file = fs.mut_file(& node_id).unwrap();
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

            let folder = node.to_folder()
                .map_err(| _ | node_error_to_rsp(NodeError::ParentIsNotFolder))
                ? ;

            auth.is_authorized(& folder.write(), & user, utc_timestamp())
                .map_err(| () | node_error_to_rsp(NodeError::UnauthorizedOperation))
                ? ;

            let index = folder.child_with_node_id(& node_id)
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

        let folder = parent.to_folder()
            .map_err(| _ | node_error_to_rsp(NodeError::ParentIsNotFolder))
            ? ;

        Ok((
            properties,
            FilesystemElementAuthorityId {
                read: folder.read().clone(),
                write: folder.write().clone(),
            }
        ))
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

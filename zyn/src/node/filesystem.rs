use std::collections::hash_map::{ DefaultHasher };
use std::hash::{ Hash, Hasher };
use std::path::{ Path, Component, PathBuf };
use std::result::{ Result };
use std::str;
use std::vec::{ Vec };

use chrono::datetime::{ DateTime };
use chrono::{ UTC, NaiveDateTime };

use node::file::{ FileHandle };
use node::folder::{ Folder };
use node::crypto::{ Crypto };
use node::user_authority::{ Id };
use node::serialize::{ SerializedFilesystem };
use node::common::{ NodeId, NODE_ID_ROOT, ADMIN_GROUP, FileType,
                    log_crypto_context_error };

#[derive(Debug)]
pub enum FilesystemError {
    InvalidNodeId,
    FolderIsNotEmpty, // Delete
    InvalidPathSize, // Resolve
    InvalidPath, // Resolve
    HostFilesystemError,
    AllNodesInUse,
    ParentIsNotFolder,
    NodeIsNotFile,
    NodeIsNotFolder,
}

pub enum Node {
    File { file: FileHandle },
    Folder { folder: Folder },
    NotSet,
}

impl Node {
    pub fn to_mut_file(& mut self) -> Result<& mut FileHandle, FilesystemError> {
        if let Node::File { ref mut file } = *self {
            return Ok(file);
        }
        Err(FilesystemError::NodeIsNotFile)
    }

    pub fn to_file(& self) -> Result<& FileHandle, FilesystemError> {
        if let Node::File { ref file } = *self {
            return Ok(file);
        }
        Err(FilesystemError::NodeIsNotFile)
    }

    pub fn to_mut_folder(& mut self) -> Result<& mut Folder, FilesystemError> {
        if let Node::Folder { ref mut folder } = *self {
            return Ok(folder);
        }
        Err(FilesystemError::NodeIsNotFolder)
    }

    pub fn to_folder(& self) -> Result<& Folder, FilesystemError> {
        if let Node::Folder { ref folder } = *self {
            return Ok(folder);
        }
        Err(FilesystemError::NodeIsNotFolder)
    }

    pub fn is_not_set(& self) -> bool {
        match *self {
            Node::NotSet => true,
            _ => false,
        }
    }

    pub fn is_file(& self) -> bool {
        match *self {
            Node::File { .. } => true,
            _ => false,
        }
    }
}

pub const DEFAULT_MAX_NUMBER_OF_NODES: usize = 5000;

pub struct Filesystem {
    nodes: Vec<Node>,
    crypto: Crypto,
    path_storage_folder: PathBuf,
}

impl Filesystem {

    pub fn empty_with_capacity(crypto: Crypto, path_storage_folder: & Path, capacity: usize)
                             -> Filesystem {

        let mut fs = Filesystem {
            nodes: Vec::with_capacity(capacity),
            crypto: crypto,
            path_storage_folder: path_storage_folder.to_path_buf(),
        };
        for _ in 0 .. capacity {
            fs.nodes.push(Node::NotSet);
        }
        fs
    }

    pub fn new_with_capacity(crypto: Crypto, path_storage_folder: & Path, capacity: usize)
                             -> Filesystem {

        let mut fs = Filesystem::empty_with_capacity(crypto, path_storage_folder, capacity);
        fs.nodes[NODE_ID_ROOT as usize] = Node::Folder{
            folder: Folder::create(ADMIN_GROUP.clone(), NODE_ID_ROOT)
        };
        fs
    }

    pub fn new(crypto: Crypto, path_storage_folder: & Path) -> Filesystem {
        Filesystem::new_with_capacity(crypto, path_storage_folder, DEFAULT_MAX_NUMBER_OF_NODES)
    }

    pub fn store(& self, path_basename: & Path) -> Result<(), ()> {

        let mut fs = SerializedFilesystem::new(self.nodes.capacity());
        for (index, node) in self.nodes.iter().enumerate() {
            match *node {
                Node::File { ref file } => {
                    fs.files.push((index as NodeId, file.path().to_path_buf()));
                },
                Node::Folder { ref folder } => {
                    fs.folders.push((
                        index as NodeId,
                        folder.parent(),
                        folder.created(),
                        folder.modified(),
                        folder.read().clone(),
                        folder.write().clone(),
                        folder.children().map(| element | {
                            (element.node_id, element.name.clone())
                        }).collect(),
                    ));
                },
                Node::NotSet => (),
            }
        }

        let context = self.crypto.create_context()
            .map_err(| () | log_crypto_context_error())
            ? ;

        fs.write(context, path_basename)
    }

    pub fn load(crypto: Crypto, path_storage_folder: & Path, path_basename: & Path)
                -> Result<Filesystem, ()> {


        let deserialized = crypto.create_context()
            .map_err(| () |log_crypto_context_error())
            .and_then(| context | SerializedFilesystem::read(context, & path_basename))
            .map_err(| () | error!("Failed to deserialized filesystem"))
            ? ;

        let mut fs = Filesystem::empty_with_capacity(crypto, & path_storage_folder, deserialized.capacity);

        for & (ref node_id, ref path) in deserialized.files.iter() {

            let file = FileHandle::init(path.to_path_buf())
                .map_err(| () | error!("Failed to init file, path=\"{}\"", path.display()))
                ? ;
            fs.nodes[*node_id as usize] = Node::File{ file: file };
        }

        for & (ref node_id, ref parent, ref created, ref modified, ref read, ref write, ref children)
            in deserialized.folders.iter() {
                let mut folder = Folder::from(parent.clone(), created.clone(), modified.clone(), read.clone(), write.clone());
                for & (id, ref name) in children.iter() {
                    folder.add_child(id, name);
                }
                fs.nodes[*node_id as usize] = Node::Folder{ folder: folder };
            }

        Ok(fs)
    }

    pub fn node(& self, node_id: & NodeId) -> Result<& Node, FilesystemError> {
        let i = *node_id as usize;
        if i < self.nodes.len() {
            return Ok(& self.nodes[i]);
        }
        Err(FilesystemError::InvalidNodeId)
    }

    pub fn mut_file(& mut self, node_id: & NodeId)
                    -> Result<& mut FileHandle, FilesystemError> {

        self.nodes.get_mut(*node_id as usize)
            .ok_or(FilesystemError::InvalidNodeId)
            .and_then(| node | {
                node.to_mut_file()
            })

    }

    pub fn resolve_path_from_root(
        & self,
        path: & Path,
        node_id_list: & mut [NodeId]
    ) -> Result<usize, FilesystemError> {

        if ! path.is_absolute() {
            return Err(FilesystemError::InvalidPath);
        }
        self.resolve_path(& NODE_ID_ROOT, path, node_id_list)
    }

    pub fn resolve_path(
        & self,
        start_node: & NodeId,
        path: & Path,
        node_id_list: & mut [NodeId]
    ) -> Result<usize, FilesystemError> {

        let mut list_index: usize = 0;
        let mut parent_node_id: NodeId = start_node.clone();
        for p in path.components() {
            match p {
                Component::RootDir => continue,
                Component::CurDir => return Err(FilesystemError::InvalidPath),
                Component::ParentDir => return Err(FilesystemError::InvalidPath),
                Component::Prefix(_) => return Err(FilesystemError::InvalidPath),
                Component::Normal(field) => {
                    if let Some(element) = field.to_str() {

                        let folder = self.nodes[parent_node_id as usize].to_folder()
                            ? ;

                        if node_id_list.len() <= list_index {
                            return Err(FilesystemError::InvalidPathSize);
                        }
                        node_id_list[list_index] = parent_node_id.clone();
                        list_index += 1;

                        let (node_id, _) = folder.child_with_name(element)
                            .map_err(| () | FilesystemError::InvalidPath)
                            ? ;
                        parent_node_id = node_id;
                        continue ;
                    }
                    return Err(FilesystemError::InvalidPath);
                },
            };
        }

        if node_id_list.len() <= list_index {
            return Err(FilesystemError::InvalidPathSize);
        }
        node_id_list[list_index] = parent_node_id;
        Ok(list_index + 1)
    }

    pub fn create_file(
        & mut self,
        parent_node_id: & NodeId,
        filename: & str,
        user: Id,
        type_of_file: FileType,
    ) -> Result<NodeId, FilesystemError> {

        let (file, node_id) = {

            self.nodes[*parent_node_id as usize].to_folder()
                .map_err(| _ | FilesystemError::ParentIsNotFolder)
                ? ;

            let path_file = self.path_storage_folder.join(
                Filesystem::hash_filename(filename)
            );

            let file = FileHandle::create(
                path_file,
                & self.crypto.clone(),
                user.clone(),
                *parent_node_id,
                type_of_file,
                1024
            )
                .map_err(| () | {
                    warn!("Failed to create file, user={}, filename={}", user, filename);
                    FilesystemError::HostFilesystemError
                })
                ? ;

            let node_id = self.allocate_node_id()
                .map_err(| () | {
                    warn!("Failed to allocate NodeId, user={}, filename={}", user, filename);
                    FilesystemError::AllNodesInUse
                })
                ? ;

            let mut parent = self.nodes.get_mut(*parent_node_id as usize)
                .unwrap()
                .to_mut_folder()
                .unwrap();

            parent.add_child(node_id, filename);

            (file, node_id)
        };

        self.nodes[node_id as usize] = Node::File { file: file };
        Ok(node_id)
    }

    pub fn create_folder(
        & mut self,
        parent_node_id: & NodeId,
        foldername: & str,
        user: Id,
    ) -> Result<NodeId, FilesystemError> {

        let node_id = {
            self.nodes[*parent_node_id as usize].to_folder()
                .map_err(| _ | FilesystemError::ParentIsNotFolder)
                ? ;

            let node_id = self.allocate_node_id()
                .map_err(| () | {
                    warn!("Failed to allocate NodeId, user={}, foldername=\"{}\"",
                          user, foldername);
                    FilesystemError::AllNodesInUse
                })
                ? ;

            let mut parent = self.nodes.get_mut(*parent_node_id as usize)
                .unwrap()
                .to_mut_folder()
                .unwrap();

            parent.add_child(node_id, foldername);
            node_id
        };

        let folder = Folder::create(user.clone(), *parent_node_id);
        self.nodes[node_id as usize] = Node::Folder { folder: folder };
        Ok(node_id)
    }

    pub fn delete(
        & mut self,
        parent_node_id: & NodeId,
        index_in_parent: usize,
        node_id: NodeId
    ) -> Result<(), FilesystemError> {

        {
            let ref mut element = self.nodes[node_id as usize];
            if element.is_not_set() {
                return Err(FilesystemError::InvalidNodeId);
            }

            if let Node::Folder { ref folder } = *element {
                if ! folder.is_empty() {
                    return Err(FilesystemError::FolderIsNotEmpty)
                }
            }
            if let Node::File { ref mut file } = *element {
                file.close()
            }
        }

        {
            let mut parent = self.nodes.get_mut(*parent_node_id as usize)
                .unwrap()
                .to_mut_folder()
                .unwrap();

            parent.remove_child(index_in_parent, & node_id)
                .map_err(| () | FilesystemError::InvalidNodeId)
                ? ;
        }

        self.nodes[node_id as usize] = Node::NotSet;
        Ok(())
    }

    fn hash_filename(name: & str) -> PathBuf {
        let dt = DateTime::<UTC>::from_utc(NaiveDateTime::from_timestamp(0, 0), UTC);
        let f = dt.format("").to_string() + name;
        let mut hasher = DefaultHasher::new();
        f.hash(& mut hasher);
        let filename = hasher.finish().to_string();
        PathBuf::from(filename)
    }

    fn allocate_node_id(& self) -> Result<NodeId, ()> {
        for (i, n) in self.nodes.iter().enumerate() {
            match n {
                & Node::NotSet => {
                    return Ok(i as NodeId)
                },
                _ => continue
            }
        }
        Err(())
    }
}

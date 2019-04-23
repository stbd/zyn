use std::collections::hash_map::{ DefaultHasher };
use std::fs::{ create_dir, remove_dir_all };
use std::hash::{ Hash, Hasher };
use std::path::{ Path, Component, PathBuf };
use std::result::{ Result };
use std::str;
use std::vec::{ Vec };

use node::file_handle::{ FileHandle, FileProperties };
use node::directory::{ Directory };
use node::crypto::{ Crypto };
use node::user_authority::{ Id };
use node::serialize::{ SerializedFilesystem };
use node::common::{ NodeId, NODE_ID_ROOT, ADMIN_GROUP, FileType,
                    log_crypto_context_error, utc_timestamp };

#[derive(Debug)]
pub enum FilesystemError {
    InvalidNodeId,
    DirectoryIsNotEmpty, // Delete
    InvalidPathSize, // Resolve
    InvalidPath, // Resolve
    HostFilesystemError,
    AllNodesInUse,
    ParentIsNotDirectory,
    NodeIsNotFile,
    NodeIsNotDirectory,
    ElementWithNameAlreadyExists,
}

pub enum Node {
    File { file: FileHandle },
    Directory { directory: Directory },
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

    pub fn to_mut_directory(& mut self) -> Result<& mut Directory, FilesystemError> {
        if let Node::Directory { ref mut directory } = *self {
            return Ok(directory);
        }
        Err(FilesystemError::NodeIsNotDirectory)
    }

    pub fn to_directory(& self) -> Result<& Directory, FilesystemError> {
        if let Node::Directory { ref directory } = *self {
            return Ok(directory);
        }
        Err(FilesystemError::NodeIsNotDirectory)
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
pub const DEFAULT_MAX_NUMBER_OF_FILES_PER_DIRECTORY: usize = 5000;

pub struct Filesystem {
    number_of_files: usize,
    max_number_of_files_per_dir: usize,
    nodes: Vec<Node>,
    crypto: Crypto,
    path_storage_directory: PathBuf,
}

impl Filesystem {

    pub fn number_of_files(& self) -> usize {
        self.number_of_files
    }

    pub fn max_number_of_files_per_directory(& self) -> usize {
        self.max_number_of_files_per_dir
    }

    pub fn empty_with_capacity(crypto: Crypto, path_storage_directory: & Path, capacity: usize, max_number_of_files_per_directory: usize)
                             -> Filesystem {

        info!("Creating fileystem: cpacity={}, max_number_of_files_per_directory={}", capacity, max_number_of_files_per_directory);

        let mut fs = Filesystem {
            number_of_files: 0,
            max_number_of_files_per_dir: max_number_of_files_per_directory,
            nodes: Vec::with_capacity(capacity + 1), // Root dir takes one place, so add one
            crypto: crypto,
            path_storage_directory: path_storage_directory.to_path_buf(),
        };
        for _ in 0 .. capacity {
            fs.nodes.push(Node::NotSet);
        }
        fs
    }

    pub fn new_with_capacity(crypto: Crypto, path_storage_directory: & Path, capacity: usize, max_number_of_files_per_directory: usize)
                             -> Filesystem {

        let mut fs = Filesystem::empty_with_capacity(crypto, path_storage_directory, capacity, max_number_of_files_per_directory);
        fs.nodes[NODE_ID_ROOT as usize] = Node::Directory{
            directory: Directory::create(ADMIN_GROUP.clone(), NODE_ID_ROOT)
        };
        fs
    }

    pub fn new(crypto: Crypto, path_storage_directory: & Path) -> Filesystem {
        Filesystem::new_with_capacity(crypto, path_storage_directory, DEFAULT_MAX_NUMBER_OF_NODES, DEFAULT_MAX_NUMBER_OF_FILES_PER_DIRECTORY)
    }

    pub fn store(& mut self, path_basename: & Path) -> Result<(), ()> {

        let mut fs = SerializedFilesystem::new(self.nodes.capacity(), self.max_number_of_files_per_dir);
        for (index, node) in self.nodes.iter_mut().enumerate() {
            match *node {
                Node::File { ref mut file } => {
                    let p = file.cached_properties()
                        .map_err(| () | error!("Failed to get cached properties when storing") )
                        ? ;
                    fs.files.push((
                        index as NodeId,
                        file.path().to_path_buf(),
                        p.file_type,
                        p.revision,
                        p.size,
                    ));
                },
                Node::Directory { ref directory } => {
                    fs.folders.push((
                        index as NodeId,
                        directory.parent(),
                        directory.created(),
                        directory.modified(),
                        directory.read().clone(),
                        directory.write().clone(),
                        directory.children().map(| element | {
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

    pub fn load(crypto: Crypto, path_storage_directory: & Path, path_basename: & Path)
                -> Result<Filesystem, ()> {


        let deserialized = crypto.create_context()
            .map_err(| () |log_crypto_context_error())
            .and_then(| context | SerializedFilesystem::read(context, & path_basename))
            .map_err(| () | error!("Failed to deserialized filesystem"))
            ? ;

        let mut fs = Filesystem::empty_with_capacity(
            crypto,
            & path_storage_directory,
            deserialized.capacity,
            deserialized.max_number_of_files_per_directory,
        );

        for & (ref node_id, ref path, ref file_type, ref revision, ref size) in deserialized.files.iter() {

            let file = FileHandle::init(
                path.to_path_buf(),
                file_type.clone(),
                revision.clone(),
                size.clone()
            )
                .map_err(| () | error!("Failed to init file, path=\"{}\"", path.display()))
                ? ;
            fs.nodes[*node_id as usize] = Node::File{ file: file };
            fs.number_of_files += 1;
        }

        for & (ref node_id, ref parent, ref created, ref modified, ref read, ref write, ref children)
            in deserialized.folders.iter() {
                let mut directory = Directory::from(parent.clone(), created.clone(), modified.clone(), read.clone(), write.clone());
                for & (id, ref name) in children.iter() {
                    directory.add_child(id, name);
                }
                fs.nodes[*node_id as usize] = Node::Directory{ directory: directory };
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

                        let directory = self.nodes[parent_node_id as usize].to_directory()
                            ? ;

                        if node_id_list.len() <= list_index {
                            return Err(FilesystemError::InvalidPathSize);
                        }
                        node_id_list[list_index] = parent_node_id.clone();
                        list_index += 1;

                        let (node_id, _) = directory.child_with_name(element)
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
        page_size: usize,
    ) -> Result<(NodeId, FileProperties), FilesystemError> {

        let (file, node_id, properties) = {

            {
                let parent = self.nodes[*parent_node_id as usize].to_directory()
                    .map_err(| _ | FilesystemError::ParentIsNotDirectory)
                    ? ;

                if parent.child_with_name(filename).is_ok() {
                    return Err(FilesystemError::ElementWithNameAlreadyExists);
                }
            }

            let path_file_root = self.generate_physical_file_path(parent_node_id, filename)
                .map_err(| _ | FilesystemError::HostFilesystemError)
                ? ;

            create_dir(& path_file_root)
                .map_err(| _ | FilesystemError::HostFilesystemError)
               ? ;

            let node_id = self.allocate_node_id()
                .map_err(| () | {
                    warn!("Failed to allocate NodeId, user={}, filename={}", user, filename);
                    FilesystemError::AllNodesInUse
                })
                ? ;

            let mut file = FileHandle::create(
                path_file_root,
                & self.crypto.clone(),
                user.clone(),
                *parent_node_id,
                type_of_file,
                page_size,
            )
                .map_err(| () | {
                    warn!("Failed to create file, user={}, filename={}", user, filename);
                    FilesystemError::HostFilesystemError
                })
                ? ;

            let mut parent = self.nodes.get_mut(*parent_node_id as usize)
                .unwrap()
                .to_mut_directory()
                .unwrap();

            parent.add_child(node_id, filename);

            let properties = file.properties(& self.crypto.clone())
                .map_err(| () | {
                    warn!("Failed to get file poperties when creating, user={}, filename={}", user, filename);
                    FilesystemError::HostFilesystemError
                })
                ? ;

            (file, node_id, properties)
        };

        self.nodes[node_id as usize] = Node::File { file: file };
        self.number_of_files += 1;
        Ok((node_id, properties))
    }

    pub fn to_directory(
        & mut self,
        parent_node_id: & NodeId,
        directoryname: & str,
        user: Id,
    ) -> Result<NodeId, FilesystemError> {

        let node_id = {
            {
                let parent = self.nodes[*parent_node_id as usize].to_directory()
                    .map_err(| _ | FilesystemError::ParentIsNotDirectory)
                    ? ;

                if parent.child_with_name(directoryname).is_ok() {
                    return Err(FilesystemError::ElementWithNameAlreadyExists);
                }
            }

            let node_id = self.allocate_node_id()
                .map_err(| () | {
                    warn!("Failed to allocate NodeId, user={}, directoryname=\"{}\"",
                          user, directoryname);
                    FilesystemError::AllNodesInUse
                })
                ? ;

            let mut parent = self.nodes.get_mut(*parent_node_id as usize)
                .unwrap()
                .to_mut_directory()
                .unwrap();

            parent.add_child(node_id, directoryname);
            node_id
        };

        let directory = Directory::create(user.clone(), *parent_node_id);
        self.nodes[node_id as usize] = Node::Directory { directory: directory };
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

            if let Node::Directory { ref directory } = *element {
                if ! directory.is_empty() {
                    return Err(FilesystemError::DirectoryIsNotEmpty)
                }
            }
            if let Node::File { ref mut file } = *element {
                self.number_of_files -= 1;
                file.close();
                remove_dir_all(file.path())
                    .map_err(| _error | FilesystemError::HostFilesystemError)
                    ? ;
            }
        }

        {
            let parent = self.nodes.get_mut(*parent_node_id as usize)
                .unwrap()
                .to_mut_directory()
                .unwrap();

            parent.remove_child(index_in_parent, & node_id)
                .map_err(| () | FilesystemError::InvalidNodeId)
                ? ;
        }

        self.nodes[node_id as usize] = Node::NotSet;
        Ok(())
    }

    fn generate_physical_file_path(& self, parent_node_id: & NodeId, name: & str) -> Result<PathBuf, ()> {
        let parent_dir = self.path_storage_directory.join(
            PathBuf::from(
                format!("{}", self.number_of_files / self.max_number_of_files_per_dir)
            )
        );

        if ! parent_dir.exists() {
            create_dir(& parent_dir)
                .map_err(| error | error!("Failed to create dir, parent_dir=\"{}\", error=\"{}\"", parent_dir.display(), error))
                ? ;
        }

        let f = format!("{}-{}-{}", utc_timestamp(), parent_node_id, name);
        let mut hasher = DefaultHasher::new();
        f.hash(& mut hasher);
        let filename = hasher.finish().to_string();
        Ok(parent_dir.join(filename))
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

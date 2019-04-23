use std::path::{ PathBuf };
use std::vec::{ Vec };

use node::test_util::tempdir::{ TempDir };
use node::filesystem::{ Filesystem };
use node::user_authority::{ Id };
use node::common::{ NODE_ID_ROOT, NodeId, FileType };
use node::file_handle::{ FileAccess };
use node::test_util::{ create_crypto, create_temp_folder, init_logging };

struct State {
    dir: TempDir,
    fs: Filesystem,
}

impl State {
    fn number_of_files() -> usize {
        10
    }

    fn page_size() -> usize {
        1024
    }

    fn user_1() -> Id {
        Id::User(0)
    }

    fn path(path: & str) -> PathBuf {
        PathBuf::from(path)
    }

    fn serialization_path(& self) -> PathBuf {
        self.dir.path().to_path_buf().join("fs")
    }

    fn empty() -> State {
        init_logging();
        let dir: TempDir = create_temp_folder();
        State {
            fs: Filesystem::new_with_capacity(create_crypto(), dir.path(), State::number_of_files(), 500),
            dir: dir,
        }
    }

    fn store_and_load_fs(& mut self) {
        let serialization_path = self.serialization_path();
        assert!(self.fs.store(& serialization_path).is_ok());
        self.fs = Filesystem::load(create_crypto(), self.dir.path(), & serialization_path).unwrap();
    }

    fn resolve_from_root(& self, path: & str, expected_resolved_buffer_length: usize) -> Vec<NodeId> {
        const SIZE: usize = 20;
        let mut resolved_ids: [NodeId; SIZE] = [0; SIZE];
        let resolved_size = self.fs.resolve_path_from_root(
            & State::path(path),
            & mut resolved_ids)
            .unwrap();
        assert!(resolved_size == expected_resolved_buffer_length);
        let mut buffer = vec![0; SIZE];
        buffer.clone_from_slice(& resolved_ids);
        buffer.resize(resolved_size, 0);
        buffer
    }

    fn verify_resolve_from_root_fails(& self, path: & str) {
        const SIZE: usize = 20;
        let mut resolved_ids: [NodeId; SIZE] = [0; SIZE];
        assert!(self.fs.resolve_path_from_root(& State::path(path), & mut resolved_ids).is_err());
    }

    fn find_child_index(& self, child_name: & str, path_parent: & str, parent_depth: usize) -> usize {
        let resolved_ids = self.resolve_from_root(path_parent, parent_depth);
        let node = self.fs.node(& resolved_ids[resolved_ids.len() - 1]).unwrap();
        let parent = node.to_directory().unwrap();
        let (_, index) = parent.child_with_name(child_name).unwrap();
        index
    }

    fn get_file_access(& mut self, file_node_id: & NodeId, user: & Id) -> FileAccess {
        let handle = self.fs.mut_file(file_node_id).unwrap();
        handle.open(& create_crypto(), user.clone()).unwrap()
    }
}

#[test]
fn test_resolve_root() {
    let state = State::empty();
    let resolved_ids = state.resolve_from_root("/", 1);
    assert!(resolved_ids[0] == NODE_ID_ROOT);
}

#[test]
fn test_to_directory() {
    let name = "folder-1";
    let mut state = State::empty();
    let node_id = state.fs.to_directory(& NODE_ID_ROOT, name, State::user_1()).unwrap();
    let resolved_ids = state.resolve_from_root(& format!("/{}", name), 2);
    assert!(resolved_ids[1] == node_id);
}

#[test]
fn test_create_subfolder() {
    let name_1 = "folder-1";
    let name_2 = "folder-2";
    let mut state = State::empty();
    let node_id_1 = state.fs.to_directory(& NODE_ID_ROOT, name_1, State::user_1()).unwrap();
    let node_id_2 = state.fs.to_directory(& node_id_1, name_2, State::user_1()).unwrap();
    let resolved_ids = state.resolve_from_root(& format!("/{}/{}", name_1, name_2), 3);
    assert!(resolved_ids[1] == node_id_1);
    assert!(resolved_ids[2] == node_id_2);
    assert!(state.fs.node(& node_id_1).unwrap().to_directory().is_ok());
    assert!(state.fs.node(& node_id_2).unwrap().to_directory().is_ok());
}

#[test]
fn test_create_multiple_elements_with_same_path() {
    let mut state = State::empty();
    assert!(state.fs.to_directory(& NODE_ID_ROOT, "folder", State::user_1()).is_ok());
    assert!(state.fs.to_directory(& NODE_ID_ROOT, "folder", State::user_1()).is_err());
    assert!(state.fs.create_file(& NODE_ID_ROOT, "file", State::user_1(), FileType::RandomAccess, State::page_size()).is_ok());
    assert!(state.fs.create_file(& NODE_ID_ROOT, "file", State::user_1(), FileType::RandomAccess, State::page_size()).is_err());
}

#[test]
fn test_create_file() {
    let name = "file-1";
    let mut state = State::empty();
    let (node_id, _) = state.fs.create_file(& NODE_ID_ROOT, name, State::user_1(), FileType::RandomAccess, State::page_size()).unwrap();
    let resolved_ids = state.resolve_from_root(& format!("/{}", name), 2);
    assert!(resolved_ids[1] == node_id);
    assert!(state.fs.node(& node_id).unwrap().to_file().is_ok());
}

#[test]
fn test_create_delete_file() {
    let name = "file-1";
    let mut state = State::empty();
    let (node_id, _) = state.fs.create_file(& NODE_ID_ROOT, name, State::user_1(), FileType::RandomAccess, State::page_size()).unwrap();
    let index = state.find_child_index(name, "/", 1);
    assert!(state.fs.delete(& NODE_ID_ROOT, index, node_id).is_ok());
    assert!(state.fs.node(& node_id).unwrap().is_not_set());
    state.verify_resolve_from_root_fails(& format!("/{}", name));
}

#[test]
fn test_serialization() {
    let folder_name = "folder";
    let filename_1 = "file-1";
    let filename_2 = "file-2";
    let written = vec![1, 2 ,3, 4, 5];
    let mut state = State::empty();

    let node_id_folder = state.fs.to_directory(& NODE_ID_ROOT, folder_name, State::user_1()).unwrap();
    let (node_id_file_1, _) = state.fs.create_file(& node_id_folder, filename_1, State::user_1(), FileType::RandomAccess, State::page_size()).unwrap();
    let (node_id_file_2, _) = state.fs.create_file(& node_id_folder, filename_2, State::user_1(), FileType::RandomAccess, State::page_size()).unwrap();
    let number_of_files = state.fs.number_of_files();
    let max_number_of_files_per_dir = state.fs.max_number_of_files_per_directory();

    let revision_1;
    {
        let mut access_1 = state.get_file_access(& node_id_file_1, & State::user_1());
        revision_1 = access_1.write(0, 0, written.clone()).unwrap();
    }

    state.store_and_load_fs();

    {
        let mut access_2 = state.get_file_access(& node_id_file_1, & State::user_1());
        let (read, revision_2) = access_2.read(0, 10).unwrap();
        assert!(revision_1 == revision_2);
        assert!(written == read);
    }

    let resolved_ids = state.resolve_from_root(& format!("/{}/{}", folder_name, filename_2), 3);
    assert!(resolved_ids[1] == node_id_folder);
    assert!(resolved_ids[2] == node_id_file_2);
    assert!(number_of_files == state.fs.number_of_files());
    assert!(max_number_of_files_per_dir == state.fs.max_number_of_files_per_directory());
}

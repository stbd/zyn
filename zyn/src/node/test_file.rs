use std::path::{ PathBuf };

extern crate tempdir;
use self::tempdir::{ TempDir };

use node::file_handle::{ FileHandle, FileAccess, FileLock, FileProperties };
use node::common::{ Buffer, FileRevision, NodeId, FileType };
use node::user_authority::{ Id };
use node::test_util;

struct State {
    _dir: TempDir,
    user: Id,
    user_2: Id,
    _parent: NodeId,
    path_file: PathBuf,
    file_handle: FileHandle,
}

impl State {
    fn _print_buffer(buffer: & Buffer) {
        print!("Buffer: ");
        for b in buffer {
            print!("{}", b);
        }
        print!("\n");
    }

    fn _path_to_file(& self, filename: & str) -> PathBuf {
        self._dir.path().join(filename)
    }

    fn init() -> State {
        State::init_with_block_size(1024)
    }

    fn init_with_block_size(block_size: usize) -> State {
        test_util::init_logging();
        let dir: TempDir = test_util::create_temp_folder();
        let path_file = dir.path().to_path_buf().join("file");

        let user = Id::User(1);
        let user_2 = Id::User(2);
        let parent: NodeId = 123;
        FileHandle::create(
            path_file.clone(),
            & test_util::create_crypto(),
            user.clone(),
            parent,
            FileType::RandomAccess,
            block_size).unwrap();
        let file_handle = FileHandle::init(path_file.clone()).unwrap();

        State {
            _dir: dir,
            user: user,
            user_2: user_2,
            _parent: parent,
            path_file: path_file,
            file_handle: file_handle,
        }
    }

    fn recreate_file_handle(& mut self) {
        self.file_handle = FileHandle::init(self.path_file.clone()).unwrap();
    }

    fn open(& mut self) -> FileAccess {
        let user = self.user.clone();
        self.open_with_user(user)
    }

    fn open_user_2(& mut self) -> FileAccess {
        let user = self.user_2.clone();
        self.open_with_user(user)
    }

    fn open_with_user(& mut self, user: Id) -> FileAccess {
        let result = self.file_handle.open(& test_util::create_crypto(), user);
        assert!(result.is_ok());
        result.unwrap()
    }

    fn is_open(& mut self) -> bool {
        self.file_handle.is_open()
    }

    fn properties(& mut self) -> FileProperties {
        self.file_handle.properties(& test_util::create_crypto()).unwrap()
    }
}

fn read_and_validate(access: & mut FileAccess, offset: u64, size: u64, expected_buffer: & Buffer, expected_revision: & FileRevision)
    -> (Buffer, FileRevision)
{
    let (read_buffer, read_revision) = access.read(offset, size).unwrap();
    assert!(read_revision == *expected_revision);
    assert!(read_buffer == *expected_buffer);
    (read_buffer, read_revision)
}

impl Drop for State {
    fn drop(& mut self) {
        if self.file_handle.is_open() {
            self.file_handle.close();
        }
    }
}

// Tests

#[test]
fn test_write_delete_insert_read() {
    let buffer_1 = vec![1, 2 ,3, 4, 5, 6];
    let mut state = State::init();
    let mut access = state.open();
    let revision_1 = access.write(0, 0, buffer_1.clone()).unwrap();
    let _ = read_and_validate(& mut access, 0, 10, & buffer_1, & revision_1);
    let revision_2 = access.delete(revision_1, 2, 3).unwrap();
    let buffer_2 = vec![1, 2 ,6];
    let _ = read_and_validate(& mut access, 0, 10, & buffer_2, & revision_2);
    let revision_3 = access.insert(revision_2, 1, vec![10, 11, 12]).unwrap();
    let buffer_3 = vec![1, 10, 11, 12, 2 ,6];
    let _ = read_and_validate(& mut access, 0, 10, & buffer_3, & revision_3);
}

#[test]
fn test_read_empty_file() {
    let mut state = State::init();
    let mut access = state.open();
    let (read_buffer, read_revision) = access.read(0, 10).unwrap();
    assert!(read_buffer.len() == 0);
    assert!(read_revision == 0);
}

#[test]
fn test_revision_increases_for_each_write() {
    let buffer = vec![1, 2 ,3];
    let mut state = State::init();
    let mut access = state.open();
    let revision_1 = access.write(0, 0, buffer.clone()).unwrap();
    let revision_2 = access.write(revision_1, 0, buffer.clone()).unwrap();
    assert!(revision_2 > revision_1);
}

#[test]
fn test_revision_increases_for_each_delete() {
    let buffer = vec![1, 2 ,3, 4, 5, 6];
    let mut state = State::init();
    let mut access = state.open();
    let revision_1 = access.write(0, 0, buffer.clone()).unwrap();
    let revision_2 = access.delete(revision_1, 0, 3).unwrap();
    assert!(revision_2 > revision_1);
}

#[test]
fn test_revision_increases_for_each_insert() {
    let buffer = vec![1, 2 ,3, 4, 5, 6];
    let mut state = State::init();
    let mut access = state.open();
    let revision_1 = access.write(0, 0, buffer.clone()).unwrap();
    let revision_2 = access.write(revision_1, 1, vec![10, 11]).unwrap();
    assert!(revision_2 > revision_1);
}

#[test]
fn test_write_to_file_multiple_times() {
    let mut state = State::init();
    let mut access = state.open();
    let revision_1 = access.write(0, 0, vec![1, 2 ,3]).unwrap();
    let revision_2 = access.write(revision_1, 1, vec![4, 5 ,6]).unwrap();
    let (_, read_revision) = read_and_validate(& mut access, 0, 4, & vec![1, 4, 5, 6], & revision_2);

    // No notifications are sent to self
    assert!(access.pop_notification().is_none());
    // Revisions increase at each modification
    assert!(revision_1 < revision_2);
    // Read revision matches latest write revision
    assert!(revision_2 == read_revision);
}

#[test]
fn test_lock_for_blob_write_prevents_edit_but_allows_read() {
    let mut state = State::init();
    let mut access_1 = state.open();
    let mut access_2 = state.open_user_2();

    let lock = FileLock::LockedBySystemForBlobWrite{ user: state.user.clone() };
    let buffer = vec![1, 2, 3];
    let revision_1 = access_1.write(0, 0, buffer.clone()).unwrap();

    access_1.lock(revision_1, & lock).unwrap();
    let _ = read_and_validate(& mut access_2, 0, 10, & buffer, & revision_1);
    assert!(access_2.write(revision_1, 0, vec![3, 4, 5]).is_err());
    assert!(access_2.insert(revision_1, 0, vec![3, 4, 5]).is_err());
    assert!(access_2.delete(revision_1, 0, 3).is_err());
    let revision_2 = access_1.write(revision_1, 3, vec![3, 4, 5]).unwrap();
    access_1.unlock(& lock).unwrap();
    let _ = access_2.write(revision_2, 0, vec![6, 7, 8]).unwrap();
}

#[test]
fn test_closing_file_access_stops_service() {
    let mut state = State::init();
    assert!(!state.is_open());
    let mut access = state.open();
    assert!(state.is_open());
    assert!(access.close().is_ok());
    test_util::assert_retry(& mut ||{
        ! state.is_open()
    });
}

#[test]
fn test_closing_file_handle_stops_access() {
    let mut state = State::init();
    assert!(!state.is_open());
    let mut access = state.open();
    assert!(state.is_open());
    state.file_handle.close();

    test_util::assert_retry(& mut ||{
        access.pop_notification().is_some()
    });
}

#[test]
fn test_serialization() {
    let written_buffer: Buffer = vec![1, 2 ,3];
    let mut state = State::init();
    let mut access_1 = state.open();
    let revision = access_1.write(0, 0, written_buffer.clone()).unwrap();
    let properties_1 = access_1.properties().unwrap();

    state.recreate_file_handle();
    test_util::assert_retry(& mut ||{
        access_1.pop_notification().is_some()
    });

    let mut access_2 = state.open();
    let _ = read_and_validate(& mut access_2, 0, 10, & written_buffer, & revision);
    let properties_2 = access_2.properties().unwrap();

    assert!(properties_1.parent == properties_2.parent);
    assert!(properties_1.revision == properties_2.revision);
    assert!(properties_1.created_at == properties_2.created_at);
    assert!(properties_1.modified_at == properties_2.modified_at);
    assert!(properties_1.file_type == properties_2.file_type);
    assert!(properties_1.size == properties_2.size);
}

#[test]
fn test_read_overflow_is_truncated() {
    let mut state = State::init();
    let mut access = state.open();
    let written_buffer: Buffer = vec![1, 2 ,3];
    let written_revision = access.write(0, 0, written_buffer.clone()).unwrap();
    let _ = read_and_validate(& mut access, 0, 10, & written_buffer, & written_revision);
}

#[test]
fn test_multiple_accesses() {
    let mut state = State::init();
    let mut access_1 = state.open();
    let mut access_2 = state.open();

    let mut revision;
    {
        // Write with access_1, expect equal read behaviour and notification for access_2
        let written_buffer: Buffer = vec![1, 2 ,3];
        let written_revision = access_1.write(0, 0, written_buffer.clone()).unwrap();
        let _ = read_and_validate(& mut access_1, 0, 10, & written_buffer, & written_revision);
        let _ = read_and_validate(& mut access_2, 0, 10, & written_buffer, & written_revision);
        assert!(access_1.pop_notification().is_none());
        assert!(access_2.pop_notification().is_some());
        revision = written_revision;
    }

    {
        // Write with access_2, expect equal read behaviour and notification for access_1
        let written_buffer: Buffer = vec![4, 5, 6, 7, 8];
        let written_revision = access_2.write(revision, 0, written_buffer.clone()).unwrap();
        let _ = read_and_validate(& mut access_1, 0, 10, & written_buffer, & written_revision);
        let _ = read_and_validate(& mut access_2, 0, 10, & written_buffer, & written_revision);
        assert!(access_1.pop_notification().is_some());
        assert!(access_2.pop_notification().is_none());
        assert!(revision < written_revision);
        revision = written_revision;
    }

    {
        // Delete with access_2, expect equal read behaviour and notification for access_1
        let expected_buffer: Buffer = vec![4, 5];
        let deleted_revision = access_2.delete(revision, 2, 3).unwrap();
        let _ = read_and_validate(& mut access_1, 0, 10, & expected_buffer, & deleted_revision);
        let _ = read_and_validate(& mut access_2, 0, 10, & expected_buffer, & deleted_revision);
        assert!(access_1.pop_notification().is_some());
        assert!(access_2.pop_notification().is_none());
        assert!(revision < deleted_revision);
    }
}

#[test]
fn test_reading_metadata_when_thread_is_not_running() {
    let mut state = State::init();
    assert!(state.is_open() == false);
    let _ = state.properties();
}

#[test]
fn test_reading_metadata_when_thread_is_running() {
    let mut state = State::init();
    let mut _access = state.open();
    assert!(state.is_open() == true);
    let _ = state.properties();
}

#[test]
fn test_metadata_size_is_updated() {
    let mut state = State::init();
    let mut access = state.open();
    let buffer_1: Buffer = vec![4, 5, 6, 7, 8];
    let revision = access.write(0, 0, buffer_1.clone()).unwrap();
    assert!(state.properties().size == buffer_1.len() as u64);
    let revision = access.delete(revision, 0, 2).unwrap();
    assert!(state.properties().size == (buffer_1.len() as u64 - 2));

    let buffer_2: Buffer = vec![11, 12];
    let _ = access.insert(revision, 0, buffer_2.clone()).unwrap();
    assert!(
        state.properties().size == (buffer_1.len() as u64 + buffer_2.len() as u64 - 2)
    );
}

#[test]
fn test_part_of_file_is_allocated() {
    let block_size = 8;
    let mut state = State::init_with_block_size(8);
    let mut access = state.open();
    assert!(state.is_open() == true);
    let part_1: Buffer = (block_size * 0 .. block_size * 1).collect();
    let part_2: Buffer = (block_size * 1 .. block_size * 2).collect();
    let part_3: Buffer = (block_size * 2 .. block_size * 3).collect();

    assert!(access.write(0, 0, part_1.clone()).is_ok());
    assert!(access.write(1, block_size as u64 * 1, part_2.clone()).is_ok());
    assert!(access.write(2, block_size as u64 * 2, part_3.clone()).is_ok());
    let (read_buffer, _) = access.read(block_size as u64 * 0, block_size as u64).unwrap();
    assert!(read_buffer == part_1);
    let (read_buffer, _) = access.read(block_size as u64 * 1, block_size as u64).unwrap();
    assert!(read_buffer == part_2);
    let (read_buffer, _) = access.read(block_size as u64 * 2, block_size as u64).unwrap();
    assert!(read_buffer == part_3);
}

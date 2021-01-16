use std::fs::{ OpenOptions };
use std::io::{ Read };
use std::path::{ Path, PathBuf };

use crate::node::common::{ Buffer };
use crate::node::test_util::tempdir::{ TempDir };
use crate::node::test_util;

struct State {
    temp_dir: TempDir,
}

impl State {
    fn new() -> State {
        test_util::init_logging();
        State {
            temp_dir: test_util::create_temp_folder(),
        }
    }

    fn path_file(& self, filename: & str) -> PathBuf {
        self.temp_dir.path().join(filename)
    }

    fn read_file(path_file: & Path) -> Vec<u8> {
        let mut data = Buffer::new();
        let mut file = OpenOptions::new()
            .read(true)
            .create(false)
            .open(path_file)
            .unwrap()
            ;

        file.read_to_end(& mut data).unwrap();
        data
    }
}

#[test]
fn test_encrypt_decrypt_1mb_file_via_file() {
    let state = State::new();
    let path_test_data = test_util::create_file_of_random_1024_blocks(1024);
    let plaintext = State::read_file(& path_test_data);
    let path_encrypted_data = state.path_file("encrypted");

    let context = test_util::create_crypto_context();
    context.encrypt_to_file(& plaintext, & path_encrypted_data).unwrap();
    let decrypted = context.decrypt_from_file(& path_encrypted_data).unwrap();
    assert!(plaintext == decrypted);
}

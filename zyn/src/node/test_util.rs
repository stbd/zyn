pub extern crate tempdir;
extern crate log;
use self::tempdir::{ TempDir };
use log::{ LogRecord, LogMetadata, LogLevelFilter };
use std::env::{ home_dir };
use std::fs::{ File };
use std::io::Read;
use std::path::{ PathBuf };
use std::str;
use std::sync::{ Once, ONCE_INIT };
use std::thread::{ sleep };
use std::time::{ Duration };

use node::crypto::{ Crypto, Context };


#[allow(dead_code)]
pub fn create_temp_folder() -> TempDir {
    TempDir::new("zyn-unit-tests").unwrap()
}

#[allow(dead_code)]
pub fn sleep_ms(duration_ms: u64) {
    sleep(Duration::from_millis(duration_ms));
}

pub fn assert_retry(function: & mut FnMut() -> bool) {
    for trial in 1..4 {
        if function() {
            return ;
        }
        sleep_ms(trial * 1000);
    }
    assert!(false);
}

pub fn create_crypto() -> Crypto {
    let mut buffer = String::new();
    let mut path = home_dir().unwrap();
    path.push(".zyn-test-user-gpg-fingerprint");
    let mut file = File::open(path).unwrap();;
    file.read_to_string(& mut buffer).unwrap();
    buffer.pop();
    Crypto::new(buffer).unwrap()
}

pub fn create_crypto_context() -> Context {
    let c = create_crypto();
    c.create_context().unwrap()
}

pub fn certificate_paths() -> (PathBuf, PathBuf) {
    let mut path_folder = home_dir().unwrap();
    path_folder.push(".zyn-certificates");
    let mut cert = path_folder.clone();
    cert.push("cert.pem");
    let mut key = path_folder.clone();
    key.push("key.pem");
    (cert, key)
}

struct UnitTestLogger;
impl log::Log for UnitTestLogger {
    fn enabled(&self, _: &LogMetadata) -> bool {
        true
    }

    fn log(&self, record: &LogRecord) {
        if self.enabled(record.metadata()) {
            println!("{} - {}", record.level(), record.args());
        }
    }
}

static LOGGING_GUARD: Once = ONCE_INIT;
pub fn init_logging() {
    LOGGING_GUARD.call_once( || {
        match log::set_logger(|max_log_level| {
            max_log_level.set(LogLevelFilter::Trace);
            Box::new(UnitTestLogger)
        }) {
            Err(e) => { println!("Error initializing unit test logger: {}", e); }
            Ok(()) => ()
        }
    });
}

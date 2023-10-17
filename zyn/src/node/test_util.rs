pub extern crate tempdir;
extern crate log;
use self::tempdir::{ TempDir };
use log::{ Record, Metadata, LevelFilter };
use std::env;
use std::fs::{ File };
use std::io::Read;
use std::path::{ PathBuf };

use std::process::{ Command, Stdio };
use std::sync::{ Once };
use std::thread::{ sleep };
use std::time::{ Duration };

use crate::node::crypto::{ Crypto, Context };


fn path_home() -> PathBuf {
    PathBuf::from(env::var("HOME").unwrap())
}

#[allow(dead_code)]
pub fn create_temp_folder() -> TempDir {
    TempDir::new("zyn-unit-tests").unwrap()
}

#[allow(dead_code)]
pub fn sleep_ms(duration_ms: u64) {
    sleep(Duration::from_millis(duration_ms));
}

pub fn assert_retry(function: & mut dyn FnMut() -> bool) {
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
    let mut path = path_home();
    path.push(".zyn-test-user-gpg-fingerprint");
    let mut file = File::open(path).unwrap();
    file.read_to_string(& mut buffer).unwrap();
    buffer.pop();
    Crypto::new(buffer).unwrap()
}

pub fn create_crypto_context() -> Context {
    let c = create_crypto();
    c.create_context().unwrap()
}

pub fn create_file_of_random_1024_blocks(number_of_blocks: usize) -> PathBuf{

    let size = 1024 * number_of_blocks;
    let mut path = path_home();
    path.push(format!(".zyn-test-data-{}.data", size));

    info!("Creating random data file, size={}, path=\"{}\"",
          size,
          path.display(),
    );

    if path.exists() {
        info!("Random file exists, skipping creation, path={}",
              path.display()
        );
        return path;
    }

    let process = Command::new("dd")
        .arg("bs=1024")
        .arg(format!("count={}", number_of_blocks))
        .arg("if=/dev/urandom")
        .arg(format!("of={}", path.to_str().unwrap()))
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .stdout(Stdio::null())
        .spawn()
        .map_err(| error | {
            panic!("Creating random file spawn process failed, path={}, error=\"{}\"",
                   path.display(),
                   error,
            )
        }).unwrap();

    let output = match process.wait_with_output() {
        Ok(output) => output,
        Err(error) => {
            panic!("Creating random file failed, path={}, error=\"{}\"",
                   path.display(),
                   error,
            );
        }
    };

    if output.status.success() {
        return path;
    }

    panic!("Creating random file process failed with error, path={}",
           path.display()
    );
}

struct UnitTestLogger;
impl log::Log for UnitTestLogger {
    fn enabled(&self, _: &Metadata) -> bool {
        true
    }

    fn log(&self, record: & Record) {
        if self.enabled(record.metadata()) {
            println!("{} - {}", record.level(), record.args());
        }
    }

    fn flush(&self) { }
}

static LOGGER: UnitTestLogger = UnitTestLogger;
static LOGGING_GUARD: Once = Once::new();
pub fn init_logging() {
    LOGGING_GUARD.call_once( || {
        match log::set_logger(&LOGGER)
            .map(|()| log::set_max_level(LevelFilter::Trace)) {
                Err(e) => { println!("Error initializing unit test logger: {}", e); }
                Ok(()) => ()
            }
    });
}

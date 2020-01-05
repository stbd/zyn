use std::ffi;
use std::io::{ ErrorKind };
use std::net::{ TcpListener, TcpStream };
use std::os::unix::io::AsRawFd;
use std::path::{ Path };
use std::ptr;
use std::string::String;
use std::thread::{ sleep };
use std::time::{ Duration };
use std::process::{ Command };

use tls_sys;

use node::common::{ Buffer, Timestamp };

static TLS_WANT_POLLIN: isize = tls_sys::WANT_POLLIN as isize;
static TLS_WANT_POLLOUT: isize = tls_sys::WANT_POLLOUT as isize;
static DEFAULT_SLEEP_DURATION_MS: u64 = 100;

fn _get_tls_error(server: tls_sys::Tls) -> String {
    unsafe {
        return String::from(ffi::CStr::from_ptr(tls_sys::tls_error(server)).to_str().unwrap());
    }
}

pub struct Connection {
    _socket: TcpStream,
    context: tls_sys::Tls,
}

unsafe impl Send for Connection {}
unsafe impl Sync for Connection {}

impl Connection {
    pub fn write_with_sleep(& self, buffer: & [u8]) -> Result<usize, ()> {

        unsafe {
            let mut offset: usize = 0;
            let mut trial: usize = 0;
            while trial < 10 {
                let start_point = buffer.as_ptr().offset(offset as isize);
                let bytes_left = buffer.len() - offset;
                let write_result = tls_sys::tls_write(self.context, start_point as _, bytes_left);

                if write_result > 0 {
                    offset += write_result as usize;
                    if offset == buffer.len() {
                        return Ok(offset);
                    }
                    continue;
                } else if write_result == TLS_WANT_POLLIN {
                    warn!("TLS requested POLLIN, unhandled");
                    // todo: Not sure what to do here
                } else if write_result == TLS_WANT_POLLOUT {
                    // Try again after sleep
                } else {
                    error!("Read error: read_result={}", write_result);
                    return Err(());
                }

                sleep(Duration::from_millis(DEFAULT_SLEEP_DURATION_MS));
                trial += 1;
            }
        }
        Err(())
    }

    pub fn read(& self, buffer: & mut Buffer) -> Result<bool, ()> {
        unsafe {

            let original_size = buffer.len();
            let available_size = buffer.capacity();
            let space_left = available_size - original_size;
            let mut current_size = original_size;
            buffer.resize(available_size, 0);

            let mut read_result;
            loop {
                let start_point = buffer.as_ptr().offset(current_size as isize);
                read_result = tls_sys::tls_read(self.context, start_point as * mut _, space_left);
                if read_result <= 0 {
                    break
                }
                current_size += read_result as usize;
            }

            if read_result == 0 {
                return Err(());
            } else if read_result == TLS_WANT_POLLIN {
                buffer.resize(current_size, 0);
                if current_size > original_size {
                    return Ok(true);
                } else {
                    return Ok(false)
                }
            } else if read_result == TLS_WANT_POLLOUT {
                warn!("TLS request POLLOUT, unhanled");
                // todo: Not sure what the correct behavior is
                // do the same sleep as POLLIN
                return Ok(true)
            } else {
                error!("Read error: read_result={}", read_result);
                return Err(());
            }
        }
    }
}

impl Drop for Connection {
    fn drop(& mut self) {
        debug!("Closing TLS connection");
        unsafe {
            match tls_sys::tls_close(self.context) {
                0 => (),
                _ => {
                    error!("Error closing TLS connection, error={}", _get_tls_error(self.context));
                },
            };
            tls_sys::tls_free(self.context);
        }
    }
}

pub struct Server {
    socket: TcpListener,
    context: tls_sys::Tls,
    certificate_expiration: Timestamp,
}

impl Server {
    pub fn certificate_expiration(& self) -> Timestamp {
        self.certificate_expiration
    }

    pub fn new(local_address: & str, port: u16, path_key: & Path, path_cert: & Path)
               -> Result<Server, ()> {

        let output_not_after_date = Command::new("openssl")
            .arg("x509")
            .arg("-noout")
            .arg("-enddate")
            .arg("-in")
            .arg(path_cert.to_str().unwrap())
            .output()
            .map_err(| error | {
                error!("Failed to query openssl \"not after date\", error=\"{}\"", error);
            })
            ? ;

        if ! output_not_after_date.status.success() {
            error!("openssl \"not after date\" query failed with code: {:?}", output_not_after_date.status);
            return Err(());
        }

        let mut not_after_date = String::from_utf8(output_not_after_date.stdout)
            .map_err(| error | {
                error!("Failed to run openssl command, error=\"{}\"", error);
            })
            ? ;

        not_after_date.drain(.."notAfter=".len()); // Remove field name
        not_after_date.pop();                      // Remove end of line

        let output_timestamp = Command::new("date")
            .arg("-d")
            .arg(not_after_date)
            .arg("+%s")
            .output()
            .map_err(| error | {
                error!("Failed to run openssl date command, error=\"{}\"", error);
            })
            ? ;

        if ! output_timestamp.status.success() {
            error!("openssl date conversion failed code: {:?}", output_timestamp.status);
            return Err(());
        }

        let mut end_timestamp = String::from_utf8(output_timestamp.stdout)
            .map_err(| error | {
                error!("Failed to run openssl command, error=\"{}\"", error);
            })
            ? ;

        end_timestamp.pop(); // Remove end of line
        let expiration: Timestamp = end_timestamp.parse()
            .map_err(| error | {
                error!("Failed to parse certiface expiration, error=\"{}\"", error);
            })
            ? ;

        unsafe {
            match tls_sys::tls_init() {
                0 => (),
                value => {
                    error!("Failed to init TLS: {}", value);
                    return Err(());
                }
            }

            let config = tls_sys::tls_config_new();
            if config.is_null() {
                error!("Failed to create TLS config");
                return Err(());
            }

            match tls_sys::tls_config_set_key_file(config, path_key.to_str().unwrap().as_ptr() as * mut _) {
                0 => (),
                value => {
                    error!("Failed to set key file for TLS, code {}", value);
                    return Err(());
                }
            }

            match tls_sys::tls_config_set_cert_file(config, path_cert.to_str().unwrap().as_ptr() as * mut _) {
                0 => (),
                value => {
                    error!("Failed to set cert file for TLS, code {}", value);
                    return Err(());
                }
            }

            let server = tls_sys::tls_server();
            if server.is_null() {
                error!("Failed to create TLS server");
                return Err(());
            }

            match tls_sys::tls_configure(server, config) {
                0 => (),
                value => {
                    error!("Failed to create configuration for server, error {}: \"{}\"", value, _get_tls_error(server));
                    return Err(());
                }
            }

            info!("Binding listening socket to: {}:{}", local_address, port);

            let socket = TcpListener::bind((local_address, port))
                .map_err(| error | error!("Failed to bind socket: {}", error))
                ? ;

            socket.set_nonblocking(true)
                .map_err(| error | warn!("Failed to set socket to non-bloking mode: {}", error))
                ? ;

            Ok(Server {
                socket: socket,
                context: server,
                certificate_expiration: expiration,
            })
        }
    }

    pub fn accept(& self) -> Result<Option<Connection>, ()> {
        match self.socket.accept() {
            Ok((stream, remote_info)) => {

                stream.set_nonblocking(true)
                    .map_err(| error | warn!("Failed to set socket to non-bloking mode: {}", error))
                    ? ;

                unsafe {
                    let mut context = ptr::null_mut();
                    match tls_sys::tls_accept_socket(self.context, & mut context, stream.as_raw_fd()) {
                        0 => (),
                        error => {
                            warn!("Failed to accept TLS connection from {}, error: {}", remote_info, error);
                            return Err(());
                        }
                    }

                    info!("Accepted connection from: {}", remote_info);

                    return Ok(Some(Connection {
                        _socket: stream,
                        context: context,
                    }))
                }
            },

            Err(error) => {
                if error.kind() == ErrorKind::WouldBlock {
                    return Ok(None);
                }
                warn!("Error accpeting connection: {}", error);
                return Err(())
            }
        }
    }
}

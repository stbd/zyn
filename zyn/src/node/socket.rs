use std::io::{ ErrorKind, Write, Read };
use std::net::{ TcpListener, TcpStream };
use std::thread::{ sleep };
use std::time::{ Duration };

use crate::node::common::{ Buffer };

static DEFAULT_SLEEP_DURATION_MS: u64 = 100;

pub struct Socket {
    stream: TcpStream,
    pub is_ok: bool,
}

impl Socket{

    pub fn write_with_sleep(& mut self, buffer: & Buffer) -> Result<usize, ()> {

        let mut offset: usize = 0;
        let mut trial: usize = 0;


        while trial < 10 {
            let bytes_written = match self.stream.write(buffer.get(offset..).unwrap()) {
                Ok(bytes_written) => {
                    Ok(bytes_written)
                },
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => {
                    Ok(0)
                }
                Err(error) => {
                    error!("write_with_sleep failed with \"{error}\"");
                    Err(())
                }
            } ? ;

            offset += bytes_written;
            if offset == buffer.len() {
                return Ok(offset)
            } else {
                sleep(Duration::from_millis(DEFAULT_SLEEP_DURATION_MS));
                trial += 1;
            }
        }
        Err(())
    }

    pub fn read(& mut self, buffer: & mut Buffer) -> Result<bool, ()> {

        let original_size = buffer.len();
        let mut offset = original_size;
        let mut size = original_size;
        buffer.resize(buffer.capacity(), 0);

        loop {

            let mut has_read = false;
            let amount_read = match self.stream.read(buffer.get_mut(offset..).unwrap()) {
                Ok(s) => {
                    has_read = true;
                    Ok(s)
                },
                Err(ref e) if e.kind() == ErrorKind::WouldBlock => Ok(0),
                Err(error) => {
                    error!("read failed with \"{error}\"");
                    Err(())
                }
            } ? ;

            size += amount_read;
            offset += amount_read;

            if size == buffer.capacity() {
                return Ok(true);
            }

            // If socket reads something, but the amount is zero
            // is most likely means socket was closed
            // https://doc.rust-lang.org/std/io/trait.Read.html#tymethod.read
            if has_read && amount_read == 0 {
                self.is_ok = false;
                return Ok(false)
            }

            if amount_read <= 0 {
                break;
            }
        }

        buffer.resize(size, 0);
        if original_size == buffer.len() {
            return Ok(false)
        } else {
            return Ok(true)
        }
    }
}

pub struct SocketServer {
    socket: TcpListener,
}

impl SocketServer {
    pub fn new(local_address: & str, port: u16)
               -> Result<SocketServer, ()> {

        info!("Binding server to {}:{}", local_address, port);

        let socket = TcpListener::bind((local_address, port))
            .map_err(| error | error!("Failed to bind socket: {}", error))
            ? ;

        socket.set_nonblocking(true)
            .map_err(| error | warn!("Failed to set socket to non-bloking mode: {}", error))
            ? ;

        Ok(SocketServer{
            socket: socket,
        })
    }

    pub fn accept(& self) -> Result<Option<Socket>, ()> {
        match self.socket.accept() {
            Ok((stream, remote_info)) => {

                stream.set_nonblocking(true)
                    .map_err(| error | warn!("Failed to set stream to non-bloking mode: {}", error))
                    ? ;

                info!("Accepted connection from: {}", remote_info);

                return Ok(Some(Socket {
                    stream: stream,
                    is_ok: true,
                }))
            },
            Err(error) => {
                if error.kind() == ErrorKind::WouldBlock {
                    return Ok(None);
                }
                warn!("Error accpeting connection: {}", error);
                Err(())
            }
        }

    }
}

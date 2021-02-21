use std::cmp::{ min };
use std::option::{ Option };
use std::thread::{ sleep };
use std::time::{ Duration };

use embedded_websocket::{ WebSocketServer, WebSocketState };

use crate::node::tls_connection::{ TlsConnection };
use crate::node::common::{ Buffer };
use crate::node::client_protocol_buffer::{ ReceiveBuffer, SendBuffer };

static HTTP_HEADER_END_MARKER: & [u8] = "\r\n".as_bytes();
static PROTOCOL_FIRST_BYTES_ZYN: & [u8] = "V:".as_bytes();
static PROTOCOL_FIRST_BYTES_HTTP_GET: & [u8] = "GET".as_bytes();
const HTTP_HEADER_MAX_SIZE_BYTES: usize = 14; // According to https://github.com/ninjasource/embedded-websocket/blob/master/src/lib.rs

pub struct Connection {
    connection: TlsConnection,
    receive_buffer: ReceiveBuffer,
    websocket: Option<ZynWebsocket>,
    buffer_size: usize,
}

impl Connection {

    pub fn new(tls_connection: TlsConnection, socket_buffer_size: usize) -> Result<Connection, ()> {

        let min_bytes_to_read = 4;

        let mut receive_buffer = ReceiveBuffer::with_capacity(socket_buffer_size);
        let mut websocket: Option<ZynWebsocket> = Option::None;

        for _ in 0..5 {
            match tls_connection.read(& mut receive_buffer.get_mut_buffer()) {
                Err(()) => return Err(()),
                Ok(false) => (),
                Ok(true) => {
                    if receive_buffer.length() >= min_bytes_to_read {

                        if receive_buffer.starts_with(PROTOCOL_FIRST_BYTES_ZYN) {
                            ();
                        } else if receive_buffer.starts_with(PROTOCOL_FIRST_BYTES_HTTP_GET) {
                            websocket = Some(ZynWebsocket::new(& mut receive_buffer));
                            receive_buffer.drop_consumed_data();
                        } else {
                            error!("Failed to detect protocol");
                            return Err(());
                        }
                        break ;
                    }
                }
            }
            sleep(Duration::from_millis(100));
        }

        Ok(Connection {
            receive_buffer: receive_buffer,
            connection: tls_connection,
            websocket: websocket,
            buffer_size: socket_buffer_size,
        })
    }

    pub fn is_using_websocket(& self) -> bool {
        self.websocket.is_some()
    }

    pub fn get_receive_buffer(& mut self) -> & mut ReceiveBuffer {
        & mut self.receive_buffer
    }

    pub fn create_send_buffer(& self) -> SendBuffer {
        SendBuffer::with_capacity(self.buffer_size)
    }

    pub fn create_response_buffer(& self, transaction_id: u64, error_code: u64) -> Result<SendBuffer, ()> {
        let mut buffer = self.create_send_buffer();
        buffer.write_message_namespace(1) ? ;
        buffer.write_response(transaction_id, error_code) ? ;
        Ok(buffer)
    }

    pub fn create_notification_buffer(& self) -> Result<SendBuffer, ()> {
        let mut buffer = self.create_send_buffer();
        buffer.write_message_namespace(1) ? ;
        Ok(buffer)
    }

    pub fn is_ok(& self) -> Result<(), ()> {
        match self.websocket {
            Some(ref websocket) => {
                match websocket.state {
                    ZynWebsocketState::Ok => Ok(()),
                    ZynWebsocketState::Closed => Err(()),
                    ZynWebsocketState::Error => Err(()),
                }
            },
            None => {
                Ok(())
            }
        }
    }

    fn read_from_client(
        websocket: & mut Option<ZynWebsocket>,
        connection: & mut TlsConnection,
        buffer: & mut Buffer,
    ) -> Result<bool, () > {

        let mut is_processing = false;

        match websocket {
            Some(ref mut websocket) => {
                match connection.read(& mut websocket.buffer) {
                    Ok(true) => {
                        is_processing = true
                    },
                    Ok(false) => (),
                    Err(()) => return Err(()),
                };
                match websocket.process(connection, buffer) {
                    Ok(true) => is_processing = true,
                    Ok(false) => (),
                    Err(()) => (),
                };
            },
            None => {
                match connection.read(buffer) {
                    Ok(true) => {
                        is_processing = true
                    },
                    Ok(false) => (),
                    Err(()) => return Err(()),
                };
            }
        }
        Ok(is_processing)
    }

    pub fn process(& mut self) -> Result<bool, () > {
        Connection::read_from_client(
            & mut self.websocket,
            & mut self.connection,
            self.receive_buffer.get_mut_buffer(),
        )
    }

    pub fn fill_buffer_from_client(& mut self, buffer: & mut Buffer) -> Result<(), ()> {

        const MAX_NUMBER_OF_TRIALS: usize = 100;
        let mut trial = 0;

        let data_available = self.receive_buffer.amount_of_unprocessed_data_available();
        if data_available > 0 {
            let data_required = buffer.capacity() - buffer.len();
            let size = min(data_available, data_required);
            buffer.extend(self.receive_buffer.take_data(size));
        }

        while buffer.len() != buffer.capacity() && trial < MAX_NUMBER_OF_TRIALS {

            // info!("fill_buffer_from_client: {} {}", buffer.len(), buffer.capacity());

            let result = Connection::read_from_client(
                & mut self.websocket,
                & mut self.connection,
                buffer,
            );

            match result {
                Ok(true) => (),
                Ok(false) => {
                    sleep(Duration::from_millis(100));
                    trial += 1;
                },
                Err(()) => return Err(()),
            };
        }

        if buffer.len() == buffer.capacity() {
            Ok(())
        } else {
            Err(())
        }
    }

    pub fn write_to_client(& mut self, buffer: & SendBuffer) -> Result<(), ()> {
        self.write_data_to_client(buffer.get_buffer())
    }

    pub fn write_data_to_client(& mut self, buffer: & Buffer) -> Result<(), ()> {

        let mut websocket = self.websocket.take();
        let result = match websocket {
            Some(ref mut s) => {
                s.write_to_client(buffer, & mut self.connection, None)
            },
            None => {
                let size = buffer.len();
                if self.connection.write_with_sleep(buffer) ? == size {
                    Ok(())
                } else {
                    Err(())
                }
            },
        };
        self.websocket = websocket;
        result
    }
}

const WEBSOCKET_OUTPUT_BUFFER_SIZE_BYTES: usize = 1024 * 4;
enum ZynWebsocketState {
    Ok,
    Closed,
    Error,
}

struct ZynWebsocket {
    server: WebSocketServer,
    buffer: Buffer,
    state: ZynWebsocketState,
}

impl ZynWebsocket {
    fn new(buffer: & mut ReceiveBuffer) -> ZynWebsocket {
        ZynWebsocket {
            server: WebSocketServer::new_server(),
            buffer: Buffer::from(buffer.take_data(buffer.amount_of_unprocessed_data_available())),
            state: ZynWebsocketState::Ok,
        }
    }

    fn is_complete_message(& self) -> bool {
        self.buffer
            .windows(HTTP_HEADER_END_MARKER.len())
            .position(| window | window == HTTP_HEADER_END_MARKER)
            .is_some()
    }

    fn write_to_client(
        & mut self,
        buffer: & Buffer,
        connection: & mut TlsConnection,
        message_type: Option<embedded_websocket::WebSocketSendMessageType>,
    ) -> Result<(), ()> {

        let type_of_message = {
            match message_type {
                Some(t) => t,
                None => embedded_websocket::WebSocketSendMessageType::Binary,
            }
        };

        let mut output_buffer = Buffer::with_capacity(WEBSOCKET_OUTPUT_BUFFER_SIZE_BYTES);
        output_buffer.resize(WEBSOCKET_OUTPUT_BUFFER_SIZE_BYTES, 0);
        let block_size = output_buffer.len() - HTTP_HEADER_MAX_SIZE_BYTES;


        let mut chunks = buffer.chunks(block_size).peekable();
        while let Some(chunk) = chunks.next() {

            let mut is_last = false;
            if chunks.peek().is_none() {
                is_last = true;
            }

            let message_size = self.server.write(
                type_of_message,
                is_last,
                chunk,
                output_buffer.as_mut_slice(),
            )
                .map_err(|_| ())
                ? ;

            output_buffer.resize(message_size, 0);
            if connection.write_with_sleep(& output_buffer) ? != message_size {
                error!("Failed to write websocket message to socket");
                self.state = ZynWebsocketState::Error;
                return Err(());
            }
        }
        Ok(())
    }

    fn write_close_message_to_client(
        & mut self,
        connection: & mut TlsConnection,
        status: embedded_websocket::WebSocketCloseStatusCode,
        description: Option<&str>,
    ) -> Result<(), ()> {

        let mut output_buffer = Buffer::with_capacity(WEBSOCKET_OUTPUT_BUFFER_SIZE_BYTES);
        output_buffer.resize(WEBSOCKET_OUTPUT_BUFFER_SIZE_BYTES, 0);

        let message_size = self.server.close(
            status,
            description,
            output_buffer.as_mut_slice(),
        )
            .map_err(|_| ())
            ? ;

        output_buffer.resize(message_size, 0);
        if connection.write_with_sleep(& output_buffer) ? != message_size {
            error!("Failed to write websocket close emssage to socket");
            self.state = ZynWebsocketState::Error;
            return Err(());
        }
        Ok(())
    }

    fn process(& mut self, connection: & mut TlsConnection, data_buffer: & mut Buffer) -> Result<bool, ()> {
        if self.buffer.is_empty() {
            return Ok(false);
        }

        if self.server.state == WebSocketState::None {

            if ! self.is_complete_message() {
                return Ok(false);
            }

            let header = embedded_websocket::read_http_header(& self.buffer)
                .map_err(|error| {
                    self.state = ZynWebsocketState::Error;
                    error!("Error reading websocket http header: {:?}", error);
                })
                ? ;

            if header.websocket_context.is_none() {
                error!("Trying to accept websocket without context");
                self.state = ZynWebsocketState::Error;
                return Err(());
            }

            let context = header.websocket_context.unwrap();
            let mut buffer = Buffer::with_capacity(WEBSOCKET_OUTPUT_BUFFER_SIZE_BYTES);
            buffer.resize(WEBSOCKET_OUTPUT_BUFFER_SIZE_BYTES, 0);

            let rsp_size = self.server.server_accept(
                & context.sec_websocket_key,
                None, // protocol
                buffer.as_mut_slice(),
            )
                .map_err(|error|{
                    self.state = ZynWebsocketState::Error;
                    error!("Failed to accept weboscket: {:?}", error);
                })
                ? ;

            buffer.resize(rsp_size, 0);
            if connection.write_with_sleep(& buffer) ? != rsp_size {
                error!("Failed to write websocket message to socket");
                self.state = ZynWebsocketState::Error;
                return Err(());
            }
            self.buffer.clear(); // todo: All data in buffer is dropped

            info!("Websocket connection to client established");

        } else if self.server.state == WebSocketState::Open {


            let start_index = data_buffer.len();
            data_buffer.resize(data_buffer.capacity(), 0);

            let result = self.server.read(& self.buffer, & mut data_buffer[start_index .. ])
                .map_err(|error|{
                    self.state = ZynWebsocketState::Error;
                    error!("Failed to read data from weboscket: {:?}", error);
                })
                ? ;

            self.buffer.drain( .. result.len_from);
            data_buffer.resize(start_index + result.len_to, 0);

            match result.message_type {
                embedded_websocket::WebSocketReceiveMessageType::Binary => {
                    // debug!("Buffer {}", String::from_utf8_lossy(data_buffer));
                    ();
                },
                embedded_websocket::WebSocketReceiveMessageType::Text => {

                    error!("Received text websocket message, discarding");

                    self.write_close_message_to_client(
                        connection,
                        embedded_websocket::WebSocketCloseStatusCode::InvalidMessageType,
                        None,
                        //Some("Only binary message are supported"),
                    )
                        .map_err(|()| self.state = ZynWebsocketState::Error)
                        ? ;
                },
                embedded_websocket::WebSocketReceiveMessageType::CloseCompleted => {

                    info!("Websocket close completed");
                    self.state = ZynWebsocketState::Closed;

                },
                embedded_websocket::WebSocketReceiveMessageType::CloseMustReply => {

                    info!("Received CloseMustReply");

                    // If message did not fit into buffer, let's just reject it
                    if ! result.end_of_message {
                        self.write_close_message_to_client(
                            connection,
                            embedded_websocket::WebSocketCloseStatusCode::MessageTooBig,
                            None,
                        )
                            .map_err(|()| self.state = ZynWebsocketState::Error)
                            ? ;
                    } else {
                        self.write_to_client(
                            data_buffer,
                            connection,
                            Some(embedded_websocket::WebSocketSendMessageType::CloseReply ),
                        )
                            .map_err(|()| self.state = ZynWebsocketState::Error)
                            ? ;
                    }

                    self.state = ZynWebsocketState::Closed;
                },
                embedded_websocket::WebSocketReceiveMessageType::Ping => {

                    debug!("Received websocket ping message");

                    // If message did not fit into buffer, let's just reject it
                    if ! result.end_of_message {
                        self.write_close_message_to_client(
                            connection,
                            embedded_websocket::WebSocketCloseStatusCode::MessageTooBig,
                            None,
                        )
                            .map_err(|()| self.state = ZynWebsocketState::Error)
                            ? ;
                    } else {

                        self.write_to_client(
                            data_buffer,
                            connection,
                            Some(embedded_websocket::WebSocketSendMessageType::Pong),
                        )
                            .map_err(|()| self.state = ZynWebsocketState::Error)
                            ? ;
                    }
                },
                embedded_websocket::WebSocketReceiveMessageType::Pong => {

                    // Zyn server should never send ping and thus never receive pong
                    self.write_close_message_to_client(
                        connection,
                        embedded_websocket::WebSocketCloseStatusCode::InvalidMessageType,
                        None,
                    )
                        .map_err(|()| self.state = ZynWebsocketState::Error)
                        ? ;
                },
            }

        } else {
            panic!("Websocket was in unhandled state: state={:?}", self.server.state);
        }
        Ok(true)
    }
}

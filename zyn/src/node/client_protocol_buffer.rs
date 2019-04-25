use std::io::{ Write };
use std::path::{ PathBuf };
use std::vec::{ Vec };

use node::common::{ Buffer, NodeId, FileDescriptor, Timestamp };
use node::node::{ Authority };

// todo: collect all static str to constants
pub static FIELD_END_MARKER: & 'static str = "E:;";

const TYPE_USER: u64 = 0;
const TYPE_GROUP: u64 = 1;

pub struct ReceiveBuffer {
    pub buffer: Buffer,
    pub buffer_index: usize,
}

impl ReceiveBuffer {
    pub fn with_capacity(size: usize) -> ReceiveBuffer {
        ReceiveBuffer {
            buffer: Vec::with_capacity(size),
            buffer_index: 0,
        }
    }

    pub fn is_complete_message(& mut self) -> bool {
        let end_marker_bytes = FIELD_END_MARKER.as_bytes();
        self.buffer
            .windows(end_marker_bytes.len())
            .position(| window | window == end_marker_bytes)
            .is_some()
    }

    pub fn take(& mut self, output: & mut Buffer) {
        let requested_max = self.buffer_index + output.len();
        let used_max = {
            if requested_max > self.buffer.len() {
                self.buffer.len()
            } else {
                requested_max
            }
        };
        output.extend(& self.buffer[self.buffer_index .. used_max]);
        self.buffer_index = used_max;
    }

    pub fn drop_consumed_buffer(& mut self) {
        self.buffer.drain(0..self.buffer_index);
        self.buffer_index = 0;
    }

    pub fn debug_buffer(& self) {
        let size = self.buffer.len();
        debug!("Buffer ({}): {}", size, String::from_utf8_lossy(& self.buffer));
    }

    pub fn get_mut_buffer(& mut self) -> & mut Vec<u8> {
        & mut self.buffer
    }

    pub fn get_buffer(& self) -> & [u8] {
        & self.buffer[self.buffer_index .. ]
    }

    pub fn get_buffer_length(& self) -> usize {
        self.buffer.len() - self.buffer_index + 1
    }

    pub fn get_buffer_with_length(& self, size: usize) -> & [u8] {
        & self.buffer[self.buffer_index .. self.buffer_index + size]
    }

    pub fn expect(& mut self, expected: & str) -> Result<(), ()> {

        let expected_bytes = expected.as_bytes();
        {
            if self.get_buffer_length() < expected_bytes.len() {
                return Err(())
            }

            let elements = self.get_buffer_with_length(expected_bytes.len());
            if expected_bytes != elements {
                return Err(())
            }
        }
        self.buffer_index += expected_bytes.len();
        return Ok(())
    }

    pub fn parse_end_of_message(& mut self) -> Result<(), ()> {
        self.expect(FIELD_END_MARKER)
    }

    pub fn parse_transaction_id(& mut self) -> Result<u64, ()> {
        self.expect("T:") ? ;
        let id = self.parse_unsigned() ? ;
        self.expect(";") ? ;
        Ok(id)
    }

    pub fn parse_node_id(& mut self) -> Result<NodeId, ()> {
        self.expect("N:") ? ;
        let id = self.parse_unsigned() ? ;
        self.expect(";") ? ;
        Ok(id as NodeId)
    }

    pub fn parse_block(& mut self) -> Result<(u64, u64), ()> {
        self.expect("BL:") ? ;
        let offset = self.parse_unsigned() ? ;
        let size = self.parse_unsigned() ? ;
        self.expect(";") ? ;
        Ok((offset, size))
    }

    pub fn parse_file_descriptor(& mut self) -> Result<FileDescriptor, ()> {
        self.expect("F:") ? ;
        let result_path = self.parse_path();
        let desc = {
            if result_path.is_ok() {
                let fd = FileDescriptor::from_path(result_path.unwrap()) ? ;
                fd
            } else {
                let id = self.parse_node_id() ? ;
                let fd = FileDescriptor::from_node_id(id as u64) ? ;
                fd
            }
        };
        self.expect(";") ? ;
        Ok(desc)
    }

    pub fn parse_path(& mut self) -> Result<PathBuf, ()> {
        self.expect("P:") ? ;
        let string = self.parse_string() ? ;
        self.expect(";") ? ;

        let path = PathBuf::from(string);
        if ! path.is_absolute() {
            return Err(())
        }
        Ok(path)
    }

    pub fn parse_string(& mut self) -> Result<String, ()> {
        self.expect("S:") ? ;
        let length = self.parse_unsigned() ? ;
        self.expect("B:") ? ;

        let value: String;
        {
            let buffer = self.get_buffer();
            value = String::from_utf8_lossy(& buffer[0 .. length as usize])
                .into_owned();
        }
        self.buffer_index += length as usize;
        self.expect(";") ? ;
        self.expect(";") ? ;
        Ok(value)
    }

    pub fn parse_message_namespace(& mut self) -> Result<u64, ()> {
        self.expect("V:") ? ;
        self.parse_numeric()
    }

    pub fn parse_unsigned(& mut self) -> Result<u64, ()> {
        self.expect("U:") ? ;
        self.parse_numeric()
    }

    fn parse_numeric(& mut self) -> Result<u64, ()> {
        let mut value: Option<u64> = None;
        let mut size: usize = 0;
        {
            let limiter = ";".as_bytes();
            let limiter_length = limiter.len();
            let buffer = self.get_buffer();
            let buffer_length = self.buffer.len();

            let mut i = 0;
            while i < buffer_length {
                if limiter == & buffer[i .. i + limiter_length] {
                    match String::from_utf8_lossy(& buffer[0 .. i])
                        .parse::<u64>() {
                        Ok(v) => {
                            value = Some(v);
                            size = i + limiter_length;
                        }
                        _ => (),
                    };
                    break;
                }
                i += limiter_length;
            }
        }
        match value {
            Some(value) => {
                self.buffer_index += size;
                Ok(value)
            },
            None => {
                Err(())
            }
        }
    }

    pub fn parse_list_start(& mut self) -> Result<u64, ()> {
        self.expect("L:") ? ;
        self.parse_unsigned()
    }

    pub fn parse_list_end(& mut self) -> Result<(), ()> {
        self.expect(";")
    }

    pub fn parse_list_element_start(& mut self) -> Result<(), ()> {
        self.expect("LE:")
    }

    pub fn parse_list_element_end(& mut self) -> Result<(), ()> {
        self.expect(";")
    }

    pub fn parse_key_value_pair_start(& mut self) -> Result<(), ()> {
        self.expect("KVP:")
    }

    pub fn parse_key_value_pair_end(& mut self) -> Result<(), ()> {
        self.expect(";")
    }
}

pub struct SendBuffer {
    buffer: Buffer,
}

impl SendBuffer {
    pub fn with_capacity(size: usize) -> SendBuffer {
        SendBuffer {
            buffer: Vec::with_capacity(size),
        }
    }

    pub fn write_response(& mut self, transaction_id: u64, error_code: u64) -> Result<(), ()> {

        write!(self.buffer, "RSP:T:").map_err(| _ | ()) ? ;
        self.write_unsigned(transaction_id) ? ;
        write!(self.buffer, ";").map_err(| _ | ()) ? ;
        self.write_unsigned(error_code) ? ;
        write!(self.buffer, ";").map_err(| _ | ())
    }

    pub fn write_message_namespace(& mut self, value: u64) -> Result<(), ()> {
        write!(self.buffer, "V:{};", value).map_err(| _ | ())
    }

    pub fn write_end_of_message(& mut self) -> Result<(), ()> {
        write!(self.buffer, "{}", FIELD_END_MARKER).map_err(| _ | ())
    }

    pub fn write_unsigned(& mut self, value: u64) -> Result<(), ()> {
        write!(self.buffer, "U:{};", value)
            .map_err(| _ | ())
    }

    pub fn write_timestamp(& mut self, value: Timestamp) -> Result<(), ()> {
        write!(self.buffer, "TS:{};", value)
            .map_err(| _ | ())
    }

    pub fn write_node_id(& mut self, node_id: NodeId) -> Result<(), ()> {
        write!(self.buffer, "N:").map_err(| _ | ()) ? ;
        self.write_unsigned(node_id as u64) ? ;
        write!(self.buffer, ";").map_err(| _ | ())
    }

    pub fn write_block(& mut self, offset: u64, size: usize) -> Result<(), ()> {
        write!(self.buffer, "BL:").map_err(| _ | ()) ? ;
        self.write_unsigned(offset as u64) ? ;
        self.write_unsigned(size as u64) ? ;
        write!(self.buffer, ";").map_err(| _ | ())
    }

    pub fn write_string(& mut self, value: String) -> Result<(), ()> {
        write!(self.buffer, "S:").map_err(| _ | ()) ? ;
        self.write_unsigned(value.len() as u64) ? ;
        write!(self.buffer, "B:").map_err(| _ | ()) ? ;
        write!(self.buffer, "{};;", value).map_err(| _ | ())
    }

    pub fn write_authority(& mut self, value: Authority) -> Result<(), ()> {
        write!(self.buffer, "AUTHORITY:").map_err(| _ | ()) ? ;
        match value {
            Authority::User(name) => {
                self.write_unsigned(TYPE_USER) ? ;
                 self.write_string(name) ? ;
            },
            Authority::Group(name) => {
                self.write_unsigned(TYPE_GROUP) ? ;
                self.write_string(name) ? ;
            }
        };
        write!(self.buffer, ";").map_err(| _ | ())
    }

    pub fn write_list_start(& mut self, number_of_elements: usize) -> Result<(), ()> {
        write!(self.buffer, "L:").map_err(| _ | ()) ? ;

        self.write_unsigned(number_of_elements as u64)
    }

    pub fn write_list_end(& mut self) -> Result<(), ()> {
        write!(self.buffer, ";").map_err(| _ | ())
    }

    pub fn write_list_element_start(& mut self) -> Result<(), ()> {
        write!(self.buffer, "LE:").map_err(| _ | ())
    }

    pub fn write_list_element_end(& mut self) -> Result<(), ()> {
        write!(self.buffer, ";").map_err(| _ | ())
    }

    pub fn write_key_value_pair_start(& mut self) -> Result<(), ()> {
        write!(self.buffer, "KVP:").map_err(| _ | ())
    }

    pub fn write_key_value_pair_end(& mut self) -> Result<(), ()> {
        write!(self.buffer, ";").map_err(| _ | ())
    }

    pub fn write_key_value_string_unsigned(& mut self, key: String, value: u64) -> Result<(), ()> {
        self.write_key_value_pair_start() ? ;
        self.write_string(key) ? ;
        self.write_unsigned(value) ? ;
        self.write_key_value_pair_end()
    }

    pub fn write_key_value_string_string(& mut self, key: String, value: String) -> Result<(), ()> {
        self.write_key_value_pair_start() ? ;
        self.write_string(key) ? ;
        self.write_string(value) ? ;
        self.write_key_value_pair_end()
    }

    pub fn write_notification_field(& mut self) -> Result<(), ()> {
        write!(self.buffer, "NOTIFICATION:;").map_err(| _ | ())
    }

    pub fn write_notification_closed(& mut self, node_id: & NodeId) -> Result<(), ()> {
        self.write_notification_field() ? ;
        write!(self.buffer, "F-CLOSED:N:U:{};;;", node_id)
            .map_err(| _ | ()) ? ;
        self.write_end_of_message()
    }

    pub fn write_notification_modified(& mut self, node_id: & NodeId, revision: & u64, offset: & u64, size: & u64)
                                       -> Result<(), ()> {
        self.write_notification_field() ? ;
        write!(self.buffer, "F-MOD:;N:U:{};;U:{};BL:U:{};U:{};;", node_id, revision, offset, size)
            .map_err(| _ | ()) ? ;
        self.write_end_of_message()
    }

    pub fn write_notification_inserted(& mut self, node_id: & NodeId, revision: & u64, offset: & u64, size: & u64)
                                       -> Result<(), ()> {
        self.write_notification_field() ? ;
        write!(self.buffer, "F-INS:;N:U:{};;U:{};BL:U:{};U:{};;", node_id, revision, offset, size)
            .map_err(| _ | ()) ? ;
        self.write_end_of_message()
    }

    pub fn write_notification_deleted(& mut self, node_id: & NodeId, revision: & u64, offset: & u64, size: & u64)
                                      -> Result<(), ()> {
        self.write_notification_field() ? ;
        write!(self.buffer, "F-DEL:;N:U:{};;U:{};BL:U:{};U:{};;", node_id, revision, offset, size)
            .map_err(| _ | ()) ? ;
        self.write_end_of_message()
    }

    pub fn write_notification_disconnected(& mut self, description: & str)
                                           -> Result<(), ()> {
        self.write_notification_field() ? ;
        write!(self.buffer, "DISCONNECTED:;S:U:{};B:{};;", description.len(), description)
            .map_err(| _ | ()) ? ;
        self.write_end_of_message()
    }

    pub fn buffer(& self) -> & Buffer {
        & self.buffer
    }

    pub fn debug_buffer(& self) {
        let size = self.buffer.len();
        debug!("Buffer ({}): {}", size, String::from_utf8_lossy(& self.buffer));
    }

    pub fn as_bytes(& self) -> & [u8] {
        & self.buffer
    }
}

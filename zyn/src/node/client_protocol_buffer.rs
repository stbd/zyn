use std::cmp::{ min };
use std::io::{ Write };
use std::path::{ PathBuf };

use crate::node::common::{ Buffer, NodeId, FileDescriptor, Timestamp };
use crate::node::node::{ Authority };

static ZYN_FIELD_END_MARKER: & [u8] = "E:;".as_bytes();
pub static ZYN_FIELD_END: & [u8] = ";".as_bytes();

const TYPE_USER: u64 = 0;
const TYPE_GROUP: u64 = 1;

pub struct ReceiveBuffer {
    input_buffer: Buffer,
    buffer_index: usize,
}

impl ReceiveBuffer {

    pub fn with_capacity(buffer_size: usize) -> ReceiveBuffer {
        ReceiveBuffer {
            input_buffer: Buffer::with_capacity(buffer_size),
            buffer_index: 0,
        }
    }

    pub fn starts_with(& self, bytes: & [u8]) -> bool {
        self.input_buffer.starts_with(bytes)
    }

    pub fn amount_of_unprocessed_data_available(& self) -> usize {
        self.input_buffer.len() - self.buffer_index
    }

    pub fn is_complete_message(& self) -> bool {
        let marker = ZYN_FIELD_END_MARKER;
        self.input_buffer
            .windows(marker.len())
            .position(| window | window == marker)
            .is_some()
    }

    pub fn debug_buffer(& self) {
        let size = self.input_buffer.len();
        debug!("Buffer ({}, {}): {}", size, self.buffer_index, String::from_utf8_lossy(& self.input_buffer));

        // To print hex output of message
        /*
        let mut s = String::new();
        for x in & self.input_buffer {
            s.push_str(& format!(" {:#x}", x));
        }
        debug!("Buffer {}", s);
         */
    }

    pub fn drop_consumed_data(& mut self) {
        self.input_buffer.drain(0..self.buffer_index);
        self.buffer_index = 0;
    }

    pub fn get_buffer(& mut self) -> & Buffer {
        & self.input_buffer
    }

    pub fn get_mut_buffer(& mut self) -> & mut Buffer {
        & mut self.input_buffer
    }

    pub fn length(& self) -> usize {
        self.input_buffer.len()
    }

    pub fn take_data(& mut self, requested_size: usize) -> & [u8] {
        let index = self.buffer_index;
        let size = min(requested_size, self.amount_of_unprocessed_data_available());
        self.buffer_index += size;
        & self.input_buffer[index .. index + size]
    }

    pub fn expect(& mut self, expected_bytes: & [u8]) -> Result<(), ()> {

        {
            if self.amount_of_unprocessed_data_available() < expected_bytes.len() {
                return Err(())
            }

            let elements = & self.input_buffer[self.buffer_index .. self.buffer_index + expected_bytes.len()];
            if expected_bytes != elements {
                return Err(())
            }
        }
        self.buffer_index += expected_bytes.len();
        return Ok(())
    }

    fn parse_numeric(& mut self) -> Result<u64, ()> {
        let mut value: Option<u64> = None;
        let mut size: usize = 0;
        {
            let limiter = ZYN_FIELD_END;
            let limiter_length = limiter.len();
            let buffer_length = self.input_buffer.len() - self.buffer_index;
            let buffer = & self.input_buffer[self.buffer_index ..];

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

    pub fn parse_unsigned(& mut self) -> Result<u64, ()> {
        self.expect("U:".as_bytes()) ? ;
        self.parse_numeric()
    }

    pub fn parse_end_of_message(& mut self) -> Result<(), ()> {
        self.expect(ZYN_FIELD_END_MARKER)
    }

    pub fn parse_message_namespace(& mut self) -> Result<u64, ()> {
        self.expect("V:".as_bytes()) ? ;
        self.parse_numeric()
    }

    pub fn parse_transaction_id(& mut self) -> Result<u64, ()> {
        self.expect("T:".as_bytes()) ? ;
        let id = self.parse_unsigned() ? ;
        self.expect(ZYN_FIELD_END) ? ;
        Ok(id)
    }

    pub fn parse_list_start(& mut self) -> Result<u64, ()> {
        self.expect("L:".as_bytes()) ? ;
        self.parse_unsigned()
    }

    pub fn parse_list_end(& mut self) -> Result<(), ()> {
        self.expect(ZYN_FIELD_END)
    }

    pub fn parse_list_element_start(& mut self) -> Result<(), ()> {
        self.expect("LE:".as_bytes())
    }

    pub fn parse_list_element_end(& mut self) -> Result<(), ()> {
        self.expect(ZYN_FIELD_END)
    }

    pub fn parse_key_value_pair_start(& mut self) -> Result<(), ()> {
        self.expect("KVP:".as_bytes())
    }

    pub fn parse_key_value_pair_end(& mut self) -> Result<(), ()> {
        self.expect(ZYN_FIELD_END)
    }

    pub fn parse_node_id(& mut self) -> Result<NodeId, ()> {
        self.expect("N:".as_bytes()) ? ;
        let id = self.parse_unsigned() ? ;
        self.expect(ZYN_FIELD_END) ? ;
        Ok(id as NodeId)
    }

    pub fn parse_block(& mut self) -> Result<(u64, u64), ()> {
        self.expect("BL:".as_bytes()) ? ;
        let offset = self.parse_unsigned() ? ;
        let size = self.parse_unsigned() ? ;
        self.expect(ZYN_FIELD_END) ? ;
        Ok((offset, size))
    }

    pub fn parse_file_descriptor(& mut self) -> Result<FileDescriptor, ()> {
        self.expect("F:".as_bytes()) ? ;
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
        self.expect(ZYN_FIELD_END) ? ;
        Ok(desc)
    }

    pub fn parse_path(& mut self) -> Result<PathBuf, ()> {
        self.expect("P:".as_bytes()) ? ;
        let string = self.parse_string() ? ;
        self.expect(ZYN_FIELD_END) ? ;

        let path = PathBuf::from(string);
        if ! path.is_absolute() {
            return Err(())
        }
        Ok(path)
    }

    pub fn parse_string(& mut self) -> Result<String, ()> {
        self.expect("S:".as_bytes()) ? ;
        let length = self.parse_unsigned() ? ;
        self.expect("B:".as_bytes()) ? ;

        let value: String;
        {
            let buffer = & self.input_buffer[self.buffer_index .. ];
            value = String::from_utf8_lossy(& buffer[0 .. length as usize])
                .into_owned();
        }

        self.buffer_index += length as usize;
        self.expect(ZYN_FIELD_END) ? ;
        self.expect(ZYN_FIELD_END) ? ;
        Ok(value)
    }
}

pub struct SendBuffer {
    buffer: Buffer,
}

impl SendBuffer {
    pub fn with_capacity(size: usize) -> SendBuffer {
        SendBuffer {
            buffer: Buffer::with_capacity(size),
        }
    }

    pub fn get_buffer(& self) -> & Buffer {
        & self.buffer
    }

    pub fn get_mut_buffer(& mut self) -> & mut Buffer {
        & mut self.buffer
    }

    pub fn size(& self) -> usize {
        self.buffer.len()
    }

    pub fn resize_to_capacity(& mut self) {
        self.resize(self.buffer.capacity());
    }

    pub fn resize(& mut self, size: usize) {
        self.buffer.resize(size, 0);
    }

    pub fn drop_data_from_start(& mut self, size: usize) {
        self.buffer.drain(0..size);
    }

    pub fn write_message_namespace(& mut self, value: u64) -> Result<(), ()> {
        write!(self.buffer, "V:{};", value).map_err(| _ | ())
    }

    pub fn write_end_of_message(& mut self) -> Result<(), ()> {
        let size = self.buffer.write(ZYN_FIELD_END_MARKER).map_err(| _ | ()) ?;
        if size == ZYN_FIELD_END_MARKER.len() {
            Ok(())
        } else {
            Err(())
        }
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

    pub fn write_response(& mut self, transaction_id: u64, error_code: u64) -> Result<(), ()> {

        write!(self.buffer, "RSP:T:").map_err(| _ | ()) ? ;
        self.write_unsigned(transaction_id) ? ;
        write!(self.buffer, ";").map_err(| _ | ()) ? ;
        self.write_unsigned(error_code) ? ;
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


}

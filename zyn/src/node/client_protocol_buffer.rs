use std::vec::{ Vec };

use node::common::{ Buffer };

pub struct ClientBuffer {
    pub buffer: Buffer,
    pub buffer_index: usize,
}

impl ClientBuffer {
    pub fn with_capacity(size: usize) -> ClientBuffer {
        ClientBuffer {
            buffer: Vec::with_capacity(size),
            buffer_index: 0,
        }
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
        let buffer = self.get_buffer();
        let size = buffer.len();
        debug!("Buffer ({}): {}", size, String::from_utf8_lossy(buffer));
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
}

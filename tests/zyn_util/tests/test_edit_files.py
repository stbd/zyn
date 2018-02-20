import logging
import os
import random
import string

from nose.plugins.attrib import attr

import zyn_util.tests.common
import zyn_util.errors


PATH_FILE = os.path.dirname(os.path.abspath(__file__))


class TestLargeFiles(zyn_util.tests.common.TestCommon):

    def _megabytes_to_bytes(self, number_of_megabytes):
        return 1024 * 1024 * number_of_megabytes

    def _create_binary_blob_for_test_data(self, size):
        path_data_file = '{}/data-nonce-{}.bin'.format(PATH_FILE, size)
        if not os.path.exists(path_data_file):
            logging.info("Creating data file with size %d", size)

            current_size = 0
            block = list((string.ascii_uppercase + string.digits) * 1024)
            block_size = len(block)
            with open(path_data_file, 'wb') as fp:
                while current_size < size:
                    random.shuffle(block)
                    size_to_write = size - current_size
                    size_to_write = min(size_to_write, block_size)
                    data = ''.join(block[0:size_to_write])
                    fp.write(data.encode())
                    current_size += size_to_write

        return path_data_file

    def _read_file(self, connection, node_id, path_data, block_size):
        size = os.stat(path_data).st_size
        fp = open(path_data, 'rb')
        offset_start = 0
        offset_end = block_size

        while offset_end < size:
            expected_data = fp.read(block_size)
            read_rsp, data = connection.read_file(node_id, offset_start, block_size)
            self.assertEqual(data, expected_data)
            offset_start = offset_end
            offset_end += block_size

    def _create_write_read_file(self, connection, file_size, block_size, filename='blob'):
        path_data = self._create_binary_blob_for_test_data(file_size)
        create_rsp = connection.create_file_blob(filename, parent_path='/').as_create_rsp()
        open_rsp = connection.open_file_write(node_id=create_rsp.node_id).as_open_rsp()

        data = open(path_data, 'rb').read()
        connection.blob_write(
            open_rsp.node_id,
            open_rsp.revision,
            data,
            block_size
        ).as_write_rsp()
        self._read_file(connection, create_rsp.node_id, path_data, block_size)
        connection.close_file(open_rsp.node_id)

    def test_edit_blob_20(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        self._create_write_read_file(
            c,
            self._megabytes_to_bytes(20),
            int(self._megabytes_to_bytes(.5))
        )

    @attr(speed='slow')
    def test_edit_blob_100(self):
        c = self._start_and_connect_to_node_and_handle_auth()
        self._create_write_read_file(
            c,
            self._megabytes_to_bytes(100),
            int(self._megabytes_to_bytes(.5))
        )

    def _test_rewrite_large_file_with_smaller(self):
        # todo: currently fails
        filename = 'blob'
        file_size = self._megabytes_to_bytes(20)
        block_size = int(self._megabytes_to_bytes(.5))
        data = 'qwerty'.encode('utf8')

        c = self._start_and_connect_to_node_and_handle_auth()
        self._create_write_read_file(c, file_size, block_size, filename)

        open_rsp = c.open_file_write(path='/' + filename).as_open_rsp()
        c.blob_write(open_rsp.node_id, open_rsp.revision, data, len(data)).as_write_rsp()
        read_rsp, read_data = c.read_file(
            open_rsp.node_id,
            0,
            block_size
        )
        read_rsp = read_rsp.as_read_rsp()
        self.assertEqual(read_data, data)
        # self.assertEqual(read_rsp.size, len(data))

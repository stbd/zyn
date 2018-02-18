import logging
import os
import random
import string

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

    def test_edit_large_blob(self):
        size = self._megabytes_to_bytes(100)
        block_size = int(self._megabytes_to_bytes(.5))
        path_data = self._create_binary_blob_for_test_data(size)
        c = self._start_and_connect_to_node_and_handle_auth()
        create_rsp = c.create_file_blob('blob', parent_path='/').as_create_rsp()
        open_rsp = c.open_file_write(node_id=create_rsp.node_id).as_open_rsp()

        data = open(path_data, 'rb').read()
        write_rsp = c.blob_write(
            open_rsp.node_id,
            open_rsp.revision,
            data,
            block_size
        ).as_write_rsp()
        read_rsp, data = c.read_file(open_rsp.node_id, write_rsp.revision, 0, block_size)

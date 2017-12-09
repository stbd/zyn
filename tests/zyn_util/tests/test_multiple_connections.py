import logging
import time

import zyn_util.tests.common
import zyn_util.errors


class TestMultipleConnections(zyn_util.tests.common.TestZyn):
    def setUp(self):
        super(TestMultipleConnections, self).setUp()
        self._process = None
        self._process = self._start_node_default_params(self._work_dir.name, init=True)

    def tearDown(self):
        if self._process:
            self._stop_node(self._process)

    def _stop_node(self, process, expected_return_code=0):
        ret = process.poll()
        if ret is not None:
            assert ret == expected_return_code
        else:
            logging.info('Process {} was still alive, stopping'.format(process.pid))
            process.kill()

    def _connect_and_authenticate(self):
        connection = self._create_connection()
        connection.enable_debug_messages()
        connection.connect(self._remote_ip, self._remote_port)
        rsp = connection.authenticate(self._username, self._password)
        self.assertFalse(rsp.is_error())
        return connection

    def test_counters(self):
        def _validate_counters(connection, expected_number_of_connections):
            rsp = connection.query_counters()
            counters = rsp.get_field(0).key_value_list_to_dict()
            self.assertEqual(
                counters['active-connections'].as_uint(),
                expected_number_of_connections
            )

        c_1 = self._connect_and_authenticate()
        _validate_counters(c_1, 1)
        c_2 = self._connect_and_authenticate()
        _validate_counters(c_1, 2)
        c_2.close()
        time.sleep(0.1)
        _validate_counters(c_1, 1)

    def _expect_part_of_file_notification(
            self,
            connection,
            type_of_notification,
            expected_node_id,
            expected_revision,
            expected_offset,
            expected_size
    ):
        n = connection.read_message()
        self.assertEqual(n.notification_type(), type_of_notification)
        self.assertEqual(n.type(), zyn_util.connection.Message.NOTIFICATION)
        self.assertEqual(n.get_field(0).as_node_id(), expected_node_id)
        self.assertEqual(n.get_field(1).as_uint(), expected_revision)
        self.assertEqual(n.get_field(2).as_block(), (expected_offset, expected_size))
        return n

    def _expect_part_of_file_modified(
            self,
            connection,
            expected_node_id,
            expected_revision,
            expected_offset,
            expected_size
    ):
        return self._expect_part_of_file_notification(
            connection,
            'PF-MOD',
            expected_node_id,
            expected_revision,
            expected_offset,
            expected_size
        )

    def _expect_part_of_file_deleted(
            self,
            connection,
            expected_node_id,
            expected_revision,
            expected_offset,
            expected_size
    ):
        return self._expect_part_of_file_notification(
            connection,
            'PF-DEL',
            expected_node_id,
            expected_revision,
            expected_offset,
            expected_size
        )

    def _expect_part_of_file_inserted(
            self,
            connection,
            expected_node_id,
            expected_revision,
            expected_offset,
            expected_size
    ):
        return self._expect_part_of_file_notification(
            connection,
            'PF-INS',
            expected_node_id,
            expected_revision,
            expected_offset,
            expected_size
        )

    def test_edit_random_access_file(self):

        # todo: Cleanup to use updated connection interface

        def _open(connection, node_id):
            rsp = connection.file_open_write(node_id)
            self.assertFalse(rsp.is_error())
            self.assertEqual(rsp.get_field(0).as_node_id(), node_id)
            return rsp.get_field(1).as_uint()

        def _write(connection, node_id, revision, offset, data):
            rsp = connection.file_write(node_id, revision, offset, data)
            self.assertFalse(rsp.is_error())
            return rsp.get_field(0).as_uint()

        def _insert(connection, node_id, revision, offset, data):
            rsp = connection.file_insert(node_id, revision, offset, data)
            self.assertFalse(rsp.is_error())
            return rsp.get_field(0).as_uint()

        def _delete(connection, node_id, revision, offset, size):
            rsp = connection.file_delete(node_id, revision, offset, size)
            self.assertFalse(rsp.is_error())
            return rsp.get_field(0).as_uint()

        def _read(connection, node_id, offset, expected_data):
            rsp, data = connection.file_read(node_id, offset, len(expected_data))
            self.assertEqual(data, expected_data)

        c_1 = self._connect_and_authenticate()
        c_2 = self._connect_and_authenticate()

        rsp = c_1.file_create('/', 'file')
        self.assertFalse(rsp.is_error())
        node_id = rsp.get_field(0).as_node_id()

        _open(c_1, node_id)
        revision = _open(c_2, node_id)

        data = 'data-1'.encode('utf-8')
        revision = _write(c_1, node_id, revision, 0, data)

        self._expect_part_of_file_modified(c_2, node_id, revision, 0, len(data))
        _read(c_2, node_id, 0, data)

        revision = _delete(c_1, node_id, revision, 0, 3)
        data = data[3:]

        self._expect_part_of_file_deleted(c_2, node_id, revision, 0, 3)
        _read(c_2, node_id, 0, data)

        data_inserted = 'qwerty'.encode('utf-8')
        revision = _insert(c_1, node_id, revision, 1, data_inserted)
        data = data[:1] + data_inserted + data[1:]

        self._expect_part_of_file_inserted(c_2, node_id, revision, 1, len(data_inserted))
        _read(c_2, node_id, 0, data)

        data_written = 'abcdefghijkl'.encode('utf-8')
        revision = _write(c_2, node_id, revision, len(data), data_written)

        self._expect_part_of_file_modified(c_1, node_id, revision, len(data), len(data_written))
        _read(c_1, node_id, len(data), data_written)

        data += data_written

        c_3 = self._connect_and_authenticate()
        self.assertEqual(_open(c_3, node_id), revision)

        data_written = '123456798123456789'.encode('utf-8')
        start_of_data = len(data)
        revision = _write(c_3, node_id, revision, start_of_data, data_written)

        self._expect_part_of_file_modified(c_2, node_id, revision, start_of_data, len(data_written))
        self._expect_part_of_file_modified(c_1, node_id, revision, start_of_data, len(data_written))

        data += data_written
        _read(c_1, node_id, 0, data)
        _read(c_2, node_id, 0, data)
        _read(c_3, node_id, 0, data)

    def _test_edit_blob_file(self):
        c_1 = self._connect_and_authenticate()
        c_2 = self._connect_and_authenticate()

        node_id = c_1.create_file_blob('file', parent_path='/').as_create_rsp()

        c_1.open_file_write(node_id).as_open_rsp()
        _, revision, _, _ = c_2.open_file_write(node_id=node_id).as_open_rsp()

        data = 'qwerty'.encode('utf-8')
        c_1.blob_write(node_id, revision, data, 2).as_write_rsp()

        notification = c_2.pop_notification()
        notification = c_2.read_message()
        self.assertNotEqual(notification, None)
        notification = c_2.read_message()
        self.assertNotEqual(notification, None)

        # self._expect_part_of_file_modified(c_2, node_id, revision + 1, 0, 2)

import logging
import time

import zyn.errors
import zyn.messages

import common


class TestMultipleConnections(common.ZynNodeCommon):
    def _init_node(self):
        self._start_node(init=True)

    def _connect_and_authenticate(self):
        connection = self._connect()
        self._authenticate(connection)
        return connection

    def test_counters(self):
        def _validate_counters(connection, expected_number_of_connections):
            rsp = connection.query_counters()
            counters = rsp.field(0).key_value_list_to_dict()
            self.assertEqual(
                counters['active-connections'].as_uint(),
                expected_number_of_connections
            )

        self._init_node()
        c_1 = self._connect_and_authenticate()
        _validate_counters(c_1, 1)
        c_2 = self._connect_and_authenticate()
        _validate_counters(c_1, 2)
        c_2.disconnect()
        time.sleep(1.1)
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
        self.assertEqual(n.type(), zyn.connection.Message.NOTIFICATION)
        self.assertEqual(n.node_id, expected_node_id)
        self.assertEqual(n.revision, expected_revision)
        self.assertEqual(n.block_offset, expected_offset)
        self.assertEqual(n.block_size, expected_size)
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
            zyn.messages.Notification.TYPE_MODIFIED,
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
            zyn.messages.Notification.TYPE_DELETED,
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
            zyn.messages.Notification.TYPE_INSERTED,
            expected_node_id,
            expected_revision,
            expected_offset,
            expected_size
        )

    def test_edit_random_access_file(self):

        def _open(connection, node_id):
            rsp = connection.open_file_write(node_id)
            self.assertFalse(rsp.is_error())
            rsp = rsp.as_open_rsp()
            self.assertEqual(rsp.node_id, node_id)
            return rsp.revision

        def _write(connection, node_id, revision, offset, data):
            rsp = connection.ra_write(node_id, revision, offset, data)
            self.assertFalse(rsp.is_error())
            return rsp.as_write_rsp().revision

        def _insert(connection, node_id, revision, offset, data):
            rsp = connection.ra_insert(node_id, revision, offset, data)
            self.assertFalse(rsp.is_error())
            return rsp.as_insert_rsp().revision

        def _delete(connection, node_id, revision, offset, size):
            rsp = connection.ra_delete(node_id, revision, offset, size)
            self.assertFalse(rsp.is_error())
            return rsp.as_delete_rsp().revision

        def _read(connection, node_id, offset, expected_data):
            rsp, data = connection.read_file(node_id, offset, len(expected_data))
            self.assertEqual(data, expected_data)

        self._init_node()
        c_1 = self._connect_and_authenticate()
        c_2 = self._connect_and_authenticate()

        rsp = c_1.create_file_random_access('file', parent_path='/')
        self.assertFalse(rsp.is_error())
        node_id = rsp.as_create_rsp().node_id

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

    def test_edit_blob_file(self):
        self._init_node()
        c_1 = self._connect_and_authenticate()
        c_2 = self._connect_and_authenticate()

        node_id = c_1.create_file_blob('file', parent_path='/').as_create_rsp().node_id

        c_1.open_file_write(node_id).as_open_rsp()
        revision = c_2.open_file_write(node_id=node_id).as_open_rsp().revision

        data = 'qwerty'.encode('utf-8')
        revision = c_1.blob_write(node_id, revision, data).as_write_rsp().revision

        self._expect_part_of_file_modified(c_2, node_id, revision, 0, len(data))

        notification = c_2.read_message()
        self.assertEqual(notification, None)

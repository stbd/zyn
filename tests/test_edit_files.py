import os

import pytest

import common
import zyn.errors


class TestLargeFiles(common.ZynNodeCommon):
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
        self._read_file_and_validate_with_file_on_disk(
            connection,
            create_rsp.node_id,
            path_data,
            block_size,
        )
        connection.close_file(open_rsp.node_id)

    def test_edit_blob_20(self):
        c = self._prepare_node_and_authenticate_connection()
        self._create_write_read_file(
            c,
            self._megabytes_to_bytes(20),
            int(self._megabytes_to_bytes(.5))
        )

    @pytest.mark.slow
    def test_edit_blob_100(self):
        c = self._prepare_node_and_authenticate_connection()
        self._create_write_read_file(
            c,
            self._megabytes_to_bytes(100),
            int(self._megabytes_to_bytes(.5))
        )

    def test_rewrite_large_file_with_smaller(self):
        filename = 'blob'
        file_size = self._megabytes_to_bytes(20)
        block_size = int(self._megabytes_to_bytes(.5))
        data = 'qwerty'.encode('utf8')

        c = self._prepare_node_and_authenticate_connection()
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
        self.assertEqual(read_rsp.size, len(data))


class TestMultipleUsersEditingFile(common.ZynNodeCommon):
    def _open_file_write(self, node_id, connections):
        revision = None
        for c in connections:
            rsp = c.open_file_write(node_id=node_id).as_open_rsp()
            if revision is None:
                revision = rsp.revision
            else:
                self.assertEqual(revision, rsp.revision)
        return revision

    def _validate_notification(
            self,
            notification,
            expected_notification_type,
            expected_node_id,
            expected_revision,
            expected_offset,
            expected_size,
    ):
        self.assertNotEqual(notification, None)
        self.assertEqual(notification.notification_type(), expected_notification_type)
        self.assertEqual(notification.node_id, expected_node_id)
        self.assertEqual(notification.revision, expected_revision)
        self.assertEqual(notification.block_offset, expected_offset)
        self.assertEqual(notification.block_size, expected_size)

    def _ra_write(self, connection, node_id, revision, offset, data, connections):
        rsp = connection.ra_write(node_id, revision, offset, data).as_write_rsp()
        for c in connections:
            n = c.pop_notification(timeout=1)
            self._validate_notification(
                n,
                zyn.connection.Notification.TYPE_MODIFIED,
                node_id,
                rsp.revision,
                offset,
                len(data),
            )
        return rsp.revision

    def _ra_insert(self, connection, node_id, revision, offset, data, connections):
        rsp = connection.ra_insert(node_id, revision, offset, data).as_insert_rsp()
        for c in connections:
            n = c.pop_notification(timeout=1)
            self._validate_notification(
                n,
                zyn.connection.Notification.TYPE_INSERTED,
                node_id,
                rsp.revision,
                offset,
                len(data),
            )
        return rsp.revision

    def _ra_delete(self, connection, node_id, revision, offset, size, connections):
        rsp = connection.ra_delete(node_id, revision, offset, size).as_delete_rsp()
        for c in connections:
            n = c.pop_notification(timeout=1)
            self._validate_notification(
                n,
                zyn.connection.Notification.TYPE_DELETED,
                node_id,
                rsp.revision,
                offset,
                size,
            )
        return rsp.revision

    def test_edit_triggers_notification(self):
        filename = 'file'
        c_1 = self._prepare_node_and_authenticate_connection()
        c_2 = self._connect_to_node_and_handle_auth()
        c_3 = self._connect_to_node_and_handle_auth()
        node_id = c_1.create_file_random_access(filename, parent_path='/').as_create_rsp().node_id
        revision = self._open_file_write(node_id, [c_1, c_2, c_3])
        revision = self._ra_write(c_1, node_id, revision, 0, b'write', [c_2, c_3])
        revision = self._ra_insert(c_2, node_id, revision, 2, b'insert', [c_1, c_3])
        revision = self._ra_delete(c_3, node_id, revision, 2, 1, [c_1, c_2])

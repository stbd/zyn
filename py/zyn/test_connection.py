import unittest

import zyn.connection


class TestConnection(unittest.TestCase):
    def test_parse_1(self):
        conn = zyn.connection.ZynConnection(None)
        parsed = conn.parse_message('V:1;R:U:1;;E:;')
        # parsed = conn.parse_message_2('Q:U:1;W:2;;R:U:1;;E:;')
        # print (parsed)
        self.assertEqual(parsed, [['V', 1], ['R', ['U', 1]], ['E']])

    def test_parse_2(self):
        conn = zyn.connection.ZynConnection(None)
        parsed = conn.parse_message('V:1;Q:U:1;S:U:3;B:qwe;;;E:;')
        # print (parsed)
        self.assertEqual(parsed, [['V', 1], ['Q', ['U', 1], ['S', ['U', 3], ['B', "qwe"]]], ['E']])

    def test_parse_3(self):
        conn = zyn.connection.ZynConnection(None)
        conn.parse_message(
            'V:1;RSP:T:U:4;;U:0;;L:U:2;LE:KVP:S:U:4;B:type;'
            ';U:1;;;LE:KVP:S:U:7;B:created;;U:1500665250;;;;;E:;'
        )
        # print (parsed)
        # print (parsed[0])


class TestResponse(unittest.TestCase):

    def _create_response(self, msg):
        connection = zyn.connection.ZynConnection(None)
        parsed = connection.parse_message(msg)
        return zyn.connection.Response(parsed)

    def _response_ok_with_uint_field(self):
        return self._create_response('V:1;RSP:T:U:0;;U:0;;U:5;E:;')

    def test_response_is_ok(self):
        rsp = self._response_ok_with_uint_field()
        self.assertEqual(rsp.is_error(), False)

    def test_number_of_field(self):
        rsp = self._response_ok_with_uint_field()
        self.assertEqual(rsp.number_of_fields(), 1)

    def test_get_field(self):
        rsp = self._response_ok_with_uint_field()
        self.assertEqual(rsp.field(0).as_uint(), 5)

    def test_get_item(self):
        rsp = self._response_ok_with_uint_field()
        self.assertEqual(rsp[2][1], 5)

    def test_get_item_list(self):
        rsp = self._create_response(
            'V:1;RSP:T:U:4;;U:0;;L:U:2;LE:S:U:6;B:folder;'
            ';N:U:1;;U:1;;LE:S:U:4;B:file;;N:U:2;;U:0;;;E:;'
        )
        values = rsp.field(0).as_list()
        self.assertEqual(len(values), 2)
        self.assertEqual(len(values[0]), 3)
        self.assertEqual(len(values[1]), 3)
        self.assertEqual(values[0][0].as_string(), "folder")
        self.assertEqual(values[0][1].as_node_id(), 1)
        self.assertEqual(values[1][0].as_string(), "file")
        self.assertEqual(values[1][1].as_node_id(), 2)

    def test_get_item_list_with_key_values(self):
        rsp = self._create_response(
            'V:1;RSP:T:U:4;;U:0;;L:U:2;LE:KVP:S:U:4;B:type;'
            ';U:1;;;LE:KVP:S:U:7;B:created;;U:1500665250;;;;;E:;'
        )
        values = rsp.field(0).as_list()
        self.assertEqual(len(values), 2)
        key, value = values[0].as_key_value()
        self.assertEqual(key, 'type')
        self.assertEqual(value.as_uint(), 1)
        key, value = values[1].as_key_value()
        self.assertEqual(key, 'created')
        self.assertEqual(value.as_uint(), 1500665250)


class TestNotification(unittest.TestCase):

    def _create_notification(self, msg):
        connection = zyn.connection.ZynConnection(None)
        parsed = connection.parse_message(msg)
        return zyn.messages.Notification(parsed)

    def _notification_with_uint_field(self):
        return self._create_notification('V:1;NOTIFICATION:;FAKE-TYPE:U:5;;E:;')

    def test_number_of_fields(self):
        n = self._notification_with_uint_field()
        self.assertEqual(n.number_of_fields(), 1)

    def test_get_field(self):
        n = self._notification_with_uint_field()
        self.assertEqual(n.field(0).as_uint(), 5)

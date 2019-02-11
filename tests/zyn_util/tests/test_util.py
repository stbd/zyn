import unittest

import zyn_util.util


class TestUtil(unittest.TestCase):
    def test_normalized_remote_path(self):
        expected = '/aa/bb/cc'
        result = zyn_util.util.normalized_remote_path('//aa///bb/cc')
        self.assertEqual(expected, result)

    def test_normalized_remote_path_on_windows_path(self):
        expected = '/aa/bb'
        result = zyn_util.util.normalized_remote_path('\\aa\\\\bb')
        self.assertEqual(expected, result)

    def test_normalized_remote_path_on_empty_path(self):
        with self.assertRaises(ValueError):
            zyn_util.util.normalized_remote_path('')

    def test_split_path_on_file_path(self):
        expected = ('/path', 'file')
        result = zyn_util.util.split_remote_path('/path/file')
        self.assertEqual(expected, result)

    def test_split_path_on_directory_path(self):
        expected = ('/path', 'dir/')
        result = zyn_util.util.split_remote_path('/path/dir/')
        self.assertEqual(expected, result)

    def test_split_path_on_root_file(self):
        expected = ('/', 'file')
        result = zyn_util.util.split_remote_path('/file')
        self.assertEqual(expected, result)

    def test_split_path_on_unnormalized_path(self):
        with self.assertRaises(ValueError):
            zyn_util.util.split_remote_path('/path/file///')

    def test_split_path_on_root(self):
        with self.assertRaises(ValueError):
            zyn_util.util.split_remote_path('/')

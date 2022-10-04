import os
import unittest
from ....error import DirectoryError, FileError
from .setup import sqlite_conn_factory, setup_db
from .directory_dao import SqliteDirectoryDAO

class TestSqliteDirectoryDAO(unittest.TestCase):
    
    def setUp(self):
        config = {
            'sqlite-db-path': 'test_directory_dao.db'
        }
        try:
            os.remove('test_directory_dao.db')
        except:
            pass
        setup_db(config)
        self.conn = sqlite_conn_factory('test_directory_dao.db')()
        self.dao = SqliteDirectoryDAO(self.conn)

    def tearDown(self):
        try:
            self.conn.close()
        except:
            pass
        try:
            os.remove('test_directory_dao.db')
        except:
            pass
    
    def test_create_directory(self):
        self.dao.create_directory([], 'dir_1')
        self.dao.create_directory(['dir_1'], 'dir_1a')
        self.dao.create_directory(['dir_1'], 'dir_1b')
        try:
            self.dao.create_directory(['dir_2'], 'dir_2a')
            self.fail()
        except DirectoryError as e:
            self.assertTrue(str(e).startswith('Invalid path to directory'), 'Expected invalid path error')
        self.dao.create_directory([], 'dir_2')
        self.dao.create_directory(['dir_2'], 'dir_2a')
        self.dao.create_directory(['dir_2', 'dir_2a'], 'dir_2a_1')
        self.dao.create_directory(['dir_2', 'dir_2a'], 'dir_2a_2')

    def test_create_invalid_directory(self):
        try:
            self.dao.create_directory([], '')
            self.fail()
        except DirectoryError as e:
            self.assertEqual('Directory name can\'t be empty!', str(e), 'Expected invalid directory name error')
        self.dao.create_directory([], 'dir_1')
        try:
            self.dao.create_directory([], 'dir_1')
            self.fail()
        except DirectoryError as e:
            self.assertEqual('Directory [dir_1] exists in path [/]', str(e), 'Expected directory exists error')
        self.dao.create_directory(['dir_1'], 'dir_2')
        try:
            self.dao.create_directory(['dir_1'], 'dir_2')
            self.fail()
        except DirectoryError as e:
            self.assertEqual('Directory [dir_2] exists in path [/dir_1]', str(e), 'Expected directory exists error')

    def test_create_file(self):
        self.dao.create_file([], 'file_1')
        self.dao.create_file([], 'file_2')
        self.dao.create_directory([], 'dir_1')
        self.dao.create_file(['dir_1'], 'file_1')
        self.dao.create_file(['dir_1'], 'file_2')
        self.dao.create_directory([], 'dir_2')
        self.dao.create_file(['dir_2'], 'file_1')
        self.dao.create_file(['dir_2'], 'file_2')

    def test_create_invalid_file(self):
        try:
            self.dao.create_file([], '')
            self.fail()
        except FileError as e:
            self.assertEqual('File name can\'t be empty!', str(e), 'Expected invalid file name error')
        self.dao.create_file([], 'file_1')
        try:
            self.dao.create_file([], 'file_1')
            self.fail()
        except FileError as e:
            self.assertEqual('File [file_1] exists in path [/]', str(e), 'Expected file exists error')
        self.dao.create_directory([], 'dir_1')
        self.dao.create_file(['dir_1'], 'file_2')
        try:
            self.dao.create_file(['dir_1'], 'file_2')
            self.fail()
        except FileError as e:
            self.assertEqual('File [file_2] exists in path [/dir_1]', str(e), 'Expected file exists error')

    def test_list_directory(self):
        self.dao.create_directory([], 'dir_1')
        self.dao.create_directory(['dir_1'], 'dir_1a')
        self.dao.create_directory(['dir_1'], 'dir_1b')
        self.dao.create_file(['dir_1'], 'file_3')
        self.dao.create_file(['dir_1'], 'file_4')
        self.dao.create_directory([], 'dir_2')
        self.dao.create_directory(['dir_2'], 'dir_2a')
        self.dao.create_file(['dir_2'], 'file_5')
        self.dao.create_directory(['dir_2', 'dir_2a'], 'dir_2a_1')
        self.dao.create_directory(['dir_2', 'dir_2a'], 'dir_2a_2')
        self.dao.create_file(['dir_2', 'dir_2a'], 'file_6')
        self.dao.create_directory([], 'dir_3')
        self.dao.create_file([], 'file_1')
        self.dao.create_file([], 'file_2')
        dir_entries = self.dao.list_directory([])
        self.assertEqual(len(dir_entries), 5, 'Expected 5 entries in root directory')
        self.assertEqual(dir_entries[0], ('d', 'dir_1'))
        self.assertEqual(dir_entries[1], ('d', 'dir_2'))
        self.assertEqual(dir_entries[2], ('d', 'dir_3'))
        self.assertEqual(dir_entries[3], ('f', 'file_1'))
        self.assertEqual(dir_entries[4], ('f', 'file_2'))
        dir_entries = self.dao.list_directory(['dir_1'])
        self.assertEqual(len(dir_entries), 4, 'Expected 4 entries in dir_1')
        self.assertEqual(dir_entries[0], ('d', 'dir_1a'))
        self.assertEqual(dir_entries[1], ('d', 'dir_1b'))
        self.assertEqual(dir_entries[2], ('f', 'file_3'))
        self.assertEqual(dir_entries[3], ('f', 'file_4'))
        dir_entries = self.dao.list_directory(['dir_2'])
        self.assertEqual(len(dir_entries), 2, 'Expected 2 entries in dir_2')
        self.assertEqual(dir_entries[0], ('d', 'dir_2a'))
        self.assertEqual(dir_entries[1], ('f', 'file_5'))
        dir_entries = self.dao.list_directory(['dir_3'])
        self.assertEqual(len(dir_entries), 0, 'Expected 0 entries in dir_3')
        dir_entries = self.dao.list_directory(['dir_2', 'dir_2a'])
        self.assertEqual(len(dir_entries), 3, 'Expected 3 entries in dir_2/dir_2a')
        self.assertEqual(dir_entries[0], ('d', 'dir_2a_1'))
        self.assertEqual(dir_entries[1], ('d', 'dir_2a_2'))
        self.assertEqual(dir_entries[2], ('f', 'file_6'))
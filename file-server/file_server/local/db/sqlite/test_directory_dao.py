import os
import unittest
from ....error import DirectoryError
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
    
    def test_list_directory(self):
        self.dao.create_directory([], 'dir_1')
        self.dao.create_directory(['dir_1'], 'dir_1a')
        self.dao.create_directory(['dir_1'], 'dir_1b')
        self.dao.create_directory([], 'dir_2')
        self.dao.create_directory(['dir_2'], 'dir_2a')
        self.dao.create_directory(['dir_2', 'dir_2a'], 'dir_2a_1')
        self.dao.create_directory(['dir_2', 'dir_2a'], 'dir_2a_2')
        self.dao.create_directory([], 'dir_3')
        dir_entries = self.dao.list_directory([])
        self.assertEqual(len(dir_entries), 3, 'Expected 3 directories in root directory')
        self.assertEqual(dir_entries[0], ('d', 'dir_1'))
        self.assertEqual(dir_entries[1], ('d', 'dir_2'))
        self.assertEqual(dir_entries[2], ('d', 'dir_3'))
        dir_entries = self.dao.list_directory(['dir_1'])
        self.assertEqual(len(dir_entries), 2, 'Expected 2 directories in root directory')
        self.assertEqual(dir_entries[0], ('d', 'dir_1a'))
        self.assertEqual(dir_entries[1], ('d', 'dir_1b'))
        dir_entries = self.dao.list_directory(['dir_2'])
        self.assertEqual(len(dir_entries), 1, 'Expected 1 directories in root directory')
        self.assertEqual(dir_entries[0], ('d', 'dir_2a'))
        dir_entries = self.dao.list_directory(['dir_3'])
        self.assertEqual(len(dir_entries), 0, 'Expected 0 directories in root directory')
        dir_entries = self.dao.list_directory(['dir_2', 'dir_2a'])
        self.assertEqual(len(dir_entries), 2, 'Expected 2 directories in root directory')
        self.assertEqual(dir_entries[0], ('d', 'dir_2a_1'))
        self.assertEqual(dir_entries[1], ('d', 'dir_2a_2'))
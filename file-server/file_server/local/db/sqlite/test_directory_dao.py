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

    def test_create_invalid_directory(self):
        try:
            self.dao.create_directory([], '')
            self.fail()
        except DirectoryError as e:
            self.assertEqual('Directory name can\'t be empty!', str(e), 'Expected invalid directory name error')
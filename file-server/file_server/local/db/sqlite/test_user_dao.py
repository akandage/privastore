import os
import unittest
from ....error import AuthenticationError
from ....db.sqlite.conn_factory import sqlite_conn_factory
from .setup import setup_db
from .user_dao import SqliteUserDAO

class TestSqliteUserDAO(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config = {
            'sqlite-db-path': 'test_user_dao.db'
        }
        try:
            os.remove('test_user_dao.db')
        except:
            pass
        setup_db(config)

    @classmethod
    def tearDownClass(cls):
        try:
            os.remove('test_user_dao.db')
        except:
            pass
    
    def setUp(self):
        self.conn = sqlite_conn_factory('test_user_dao.db')()
        self.dao = SqliteUserDAO(self.conn)

    def tearDown(self):
        try:
            self.conn.close()
        except:
            pass

    def test_login_user(self):
        try:
            self.dao.login_user('psadmin', 'psadmin')
        except AuthenticationError as e:
            self.fail('Expected login success!')

    def test_login_user_incorrect_username(self):
        try:
            self.dao.login_user('psa', 'psadmin')
            self.fail()
        except AuthenticationError as e:
            self.assertEqual(str(e), 'User not found!', 'Expected user not found')

    def test_login_user_incorrect_password(self):
        try:
            self.dao.login_user('psadmin', 'psa')
        except AuthenticationError as e:
            return
        self.fail('Expected login failure!')
import os
import unittest
from .setup import conn_factory, setup_db
from .user_dao import SqliteUserDAO

class TestSqliteUserDAO(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        config = {
            'sqlite-db-path': 'test_local_server.db'
        }
        setup_db(config)

    @classmethod
    def tearDownClass(cls):
        try:
            os.remove('test_local_server.db')
        except:
            pass
    
    def setUp(self):
        self.conn = conn_factory('test_local_server.db')()
        self.dao = SqliteUserDAO(self.conn)

    def tearDown(self):
        try:
            self.conn.close()
        except:
            pass

    def test_login_user(self):
        res = self.dao.login_user('psadmin', 'psadmin')
        self.assertTrue(res, 'Expected login success!')

    def test_login_user_incorrect_username(self):
        try:
            self.dao.login_user('psa', 'psadmin')
            self.fail()
        except Exception as e:
            self.assertEqual(str(e), 'User not found!', 'Expected user not found')

    def test_login_user_incorrect_password(self):
        res = self.dao.login_user('psadmin', 'psa')
        self.assertFalse(res, 'Expected login failure!')
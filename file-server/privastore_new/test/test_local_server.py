from http import HTTPStatus
import logging
import shutil
import os
import requests
import unittest

from ..error import AuthenticationError
from ..server import config_logging, get_local_server, setup_local_db
from ..session import Sessions
from .test_util import get_error_code

HOSTNAME = os.environ.get('TEST_HOSTNAME', 'localhost')
LOCALS_PORT = int(os.environ.get('LOCALS_PORT', 8090))
TEST_DIR = os.environ.get('TEST_DIRECTORY', 'test_local_server')
API_VERSION = 1
ADMIN_USERNAME = 'psadmin'
ADMIN_PASSWORD = 'psadmin'

class TestLocalServer(unittest.TestCase):

    def get_config(self):
        if self.config:
            return self.config

        self.config = {
            'logging': {
              'log-level': 'DEBUG'  
            },
            'api': {
                'api-type': 'http',
                'api-hostname': HOSTNAME,
                'api-port': str(LOCALS_PORT)
            },
            'db': {
                'db-type': 'sqlite',
                'sqlite-db-path': os.path.join(TEST_DIR, 'local_server.db'),
                'connection-pool-size': '1'
            },
            'session': {
                'session-expiry-time': '300',
                'session-cleanup-interval': '60'
            }
        }
        return self.config

    def cleanup_test_dir(self):
        if os.path.exists(TEST_DIR):
            try:
                shutil.rmtree(TEST_DIR)
            except:
                logging.warning('Could not remove test directory!')

    def setUp(self):
        self.config = None
        self.local_server = None
        self.cleanup_test_dir()
        os.mkdir(TEST_DIR)
        config = self.get_config()
        config_logging('DEBUG')
        setup_local_db(config['db'])
        self.local_server = get_local_server(config)
        self.local_server.start()
        self.local_server.wait_started()

    def tearDown(self):
        if self.local_server:
            self.local_server.stop()
            self.local_server.join()
        self.cleanup_test_dir()
    
    def test_login_user(self):
        LOGIN_URL = 'http://{}:{}/{}/login'.format(HOSTNAME, LOCALS_PORT, API_VERSION)

        r = requests.post(LOGIN_URL, auth=(ADMIN_USERNAME, ADMIN_PASSWORD))
        self.assertEqual(r.status_code, HTTPStatus.OK)
        Sessions.validate_session(r.headers.get('x-privastore-session-id'))
        r = requests.post(LOGIN_URL, auth=('baduser', ADMIN_PASSWORD))
        self.assertEqual(r.status_code, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(get_error_code(r), AuthenticationError.USER_NOT_FOUND)
        r = requests.post(LOGIN_URL, auth=(ADMIN_USERNAME, 'badpass'))
        self.assertEqual(r.status_code, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(get_error_code(r), AuthenticationError.INCORRECT_PASSWORD)
from http import HTTPStatus
import logging
import shutil
import os
import random
import requests
import unittest
import uuid

from ..directory import Directory
from ..error import AuthenticationError
from ..local.api.flask_http import SESSION_ID_HEADER
from ..local.server import clear_local_server
from ..server import config_logging, get_local_server, setup_local_db
from ..session import Sessions
from .test_util import get_error_code

HOSTNAME = os.environ.get('TEST_HOSTNAME', 'localhost')
LOCALS_PORT = int(os.environ.get('LOCALS_PORT', 8090))
TEST_DIR = os.environ.get('TEST_DIRECTORY', 'test_local_server')
API_VERSION = 1
ADMIN_USERNAME = 'psadmin'
ADMIN_PASSWORD = 'psadmin'
SERVER_URL = 'http://{}:{}'.format(HOSTNAME, LOCALS_PORT)
LOGIN_URL = '{}/{}/login'.format(SERVER_URL, API_VERSION)
LOGOUT_URL = '{}/{}/logout'.format(SERVER_URL, API_VERSION)
HEARTBEAT_URL = '{}/{}/heartbeat'.format(SERVER_URL, API_VERSION)
CREATE_DIR_URL = '{}/{}/directory/{{}}'.format(SERVER_URL, API_VERSION)
CREATE_FILE_URL = '{}/{}/file'.format(SERVER_URL, API_VERSION)

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
            'store': {
                'store-type': 'db'
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
        clear_local_server()
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
        print(LOGIN_URL)
        r = requests.post(LOGIN_URL, auth=(ADMIN_USERNAME, ADMIN_PASSWORD))
        self.assertEqual(r.status_code, HTTPStatus.OK)
        Sessions.validate_session(r.headers.get(SESSION_ID_HEADER))
        r = requests.post(LOGIN_URL, auth=('baduser', ADMIN_PASSWORD))
        self.assertEqual(r.status_code, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(get_error_code(r), AuthenticationError.USER_NOT_FOUND)
        r = requests.post(LOGIN_URL, auth=(ADMIN_USERNAME, 'badpass'))
        self.assertEqual(r.status_code, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(get_error_code(r), AuthenticationError.INCORRECT_PASSWORD)
    
    def do_login(self, username, password):
        r = requests.post(LOGIN_URL, auth=(username, password))
        return r.headers.get(SESSION_ID_HEADER)

    def test_session(self):
        r = requests.put(HEARTBEAT_URL)
        self.assertEqual(r.status_code, HTTPStatus.BAD_REQUEST)
        session_id = self.do_login(ADMIN_USERNAME, ADMIN_PASSWORD)
        headers = {SESSION_ID_HEADER: session_id}
        inv_headers = {SESSION_ID_HEADER: 'S-'+str(uuid.uuid4())}
        r = requests.put(HEARTBEAT_URL, headers=headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = requests.put(HEARTBEAT_URL, headers=inv_headers)
        self.assertEqual(r.status_code, HTTPStatus.UNAUTHORIZED)
        r = requests.post(LOGOUT_URL)
        self.assertEqual(r.status_code, HTTPStatus.BAD_REQUEST)
        r = requests.post(LOGOUT_URL, headers=inv_headers)
        self.assertEqual(r.status_code, HTTPStatus.UNAUTHORIZED)
        r = requests.post(LOGOUT_URL, headers=headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = requests.put(HEARTBEAT_URL, headers=headers)
        self.assertEqual(r.status_code, HTTPStatus.UNAUTHORIZED)
    
    def test_create_directory(self):
        session_id = self.do_login(ADMIN_USERNAME, ADMIN_PASSWORD)
        headers = {SESSION_ID_HEADER: session_id}
        r = requests.put(CREATE_DIR_URL.format('dir_1'), headers=headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        resp = dict(r.json())
        self.assertEqual(resp.get('name'), 'dir_1')
        dir_1_uid = resp.get('uid')
        dir_1_path = resp.get('abs-path')
        dir_1_parent = resp.get('parent-uid')
        Directory.validate_uuid(dir_1_uid)
        Directory.validate_uuid(dir_1_parent)
        self.assertEqual(dir_1_path, '/dir_1')
        r = requests.put(CREATE_DIR_URL.format('dir_1'), headers=headers)
        self.assertEqual(r.status_code, HTTPStatus.CONFLICT)
        r = requests.put(CREATE_DIR_URL.format('dir_2'), headers=headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        resp = dict(r.json())
        dir_2_uid = resp.get('uid')
        dir_2_parent = resp.get('parent-uid')
        Directory.validate_uuid(dir_2_uid)
        Directory.validate_uuid(dir_2_parent)
        self.assertEqual(dir_1_parent, dir_2_parent)
        r = requests.put(CREATE_DIR_URL.format(f'dir_1?parent={Directory.generate_uid()}'), headers=headers)
        self.assertEqual(r.status_code, HTTPStatus.NOT_FOUND)
        r = requests.put(CREATE_DIR_URL.format(f'dir_1?parent={dir_1_uid}'), headers=headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        resp = dict(r.json())
        dir_3_uid = resp.get('uid')
        dir_3_path = resp.get('abs-path')
        dir_3_parent = resp.get('parent-uid')
        Directory.validate_uuid(dir_3_uid)
        self.assertEqual(dir_3_parent, dir_1_uid)
        self.assertEqual(dir_3_path, '/dir_1/dir_1')
        r = requests.put(CREATE_DIR_URL.format(f'dir_1?parent={dir_1_uid}'), headers=headers)
        self.assertEqual(r.status_code, HTTPStatus.CONFLICT)
        r = requests.put(CREATE_DIR_URL.format(f'dir_1a?parent={dir_1_uid}'), headers=headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        resp = dict(r.json())
        dir_4_uid = resp.get('uid')
        dir_4_path = resp.get('abs-path')
        dir_4_parent = resp.get('parent-uid')
        Directory.validate_uuid(dir_4_uid)
        self.assertEqual(dir_4_parent, dir_1_uid)
        self.assertEqual(dir_4_path, '/dir_1/dir_1a')
        r = requests.put(CREATE_DIR_URL.format(f'dir_2a?parent={dir_2_uid}'), headers=headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        resp = dict(r.json())
        dir_5_uid = resp.get('uid')
        dir_5_path = resp.get('abs-path')
        dir_5_parent = resp.get('parent-uid')
        Directory.validate_uuid(dir_5_uid)
        self.assertEqual(dir_5_parent, dir_2_uid)
        self.assertEqual(dir_5_path, '/dir_2/dir_2a')
    
    def test_upload_file(self):
        session_id = self.do_login(ADMIN_USERNAME, ADMIN_PASSWORD)
        headers = {SESSION_ID_HEADER: session_id}
        single_file = {
            'file': ('foo.bin', random.randbytes(100*1024), 'application/octet-stream')
        }
        r = requests.put(CREATE_DIR_URL.format('dir_1'), headers=headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        dir_1_uid = dict(r.json()).get('uid')
        r = requests.post(CREATE_FILE_URL, headers=headers, files=single_file)
        self.assertEqual(r.status_code, HTTPStatus.OK)

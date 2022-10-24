from http import HTTPStatus
import os
import requests
import shutil
import unittest
from .local_server import config_logging, LocalServer
from .session import Sessions

HOSTNAME = 'localhost'
PORT = 8090
URL = 'http://{}:{}{{}}'.format(HOSTNAME, PORT)

class TestLocalServer(unittest.TestCase):
    
    def setUp(self):
        os.mkdir('test_local_server')
        config = {
            'logging': {
              'log-level': 'DEBUG'  
            },
            'api': {
                'api-type': 'http',
                'api-hostname': HOSTNAME,
                'api-port': str(PORT)
            },
            'cache': {
                'cache-path': os.path.join('test_local_server', 'cache'),
                'max-file-size': '500MB',
                'cache-size': '1GB',
                'chunk-size': '1MB'
            },
            'db': {
                'db-type': 'sqlite',
                'sqlite-db-path': os.path.join('test_local_server', 'local_server.db'),
                'connection-pool-size': '0'
            }
        }
        config_logging(config['logging'])
        self.server = LocalServer(config)
        self.server.setup_db()
        self.server.start()
        self.server.wait_started()

    def tearDown(self):
        self.server.stop()
        self.server.join()
        try:
            shutil.rmtree('test_local_server')
        except:
            pass

    def test_session(self):
        r = requests.post(URL.format('/1/login'), auth=('psadmin', 'psadm1n'))
        self.assertEqual(r.status_code, HTTPStatus.UNAUTHORIZED)
        r = requests.post(URL.format('/1/login'), auth=('psadm1n', 'psadmin'))
        self.assertEqual(r.status_code, HTTPStatus.UNAUTHORIZED)
        r = requests.post(URL.format('/1/login'), auth=('psadmin', 'psadmin'))
        self.assertEqual(r.status_code, HTTPStatus.OK)
        session_id = r.headers.get('x-privastore-session-id')
        self.assertTrue(session_id is not None)
        try:
            Sessions.validate_session(session_id)
        except:
            self.fail('Expected valid session!')
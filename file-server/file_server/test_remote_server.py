from http import HTTPStatus
import os
import random
import requests
import shutil
import urllib
import uuid
from .remote_server import RemoteServer
from .session import Sessions
from .test_server import TestServer, HOSTNAME, PORT, URL

class TestRemoteServer(TestServer):
    
    def get_test_dir(self):
        return 'test_remote_server'

    def get_config(self):
        return {
            'logging': {
              'log-level': 'DEBUG'  
            },
            'api': {
                'api-type': 'http',
                'api-hostname': HOSTNAME,
                'api-port': str(PORT)
            },
            'auth': {
                'auth-type': 'config',
                'username': 'psadmin',
                'password-hash': '67755C157F6CF48FA66C5193AEEBC73A32EA92EDFD301E678EFE9C8D727F13DD'
            },
            'store': {
                'store-path': os.path.join(self.get_test_dir(), 'cache'),
                'max-file-size': '500MB',
                'store-size': '1GB',
                'chunk-size': '1MB',
                'enable-file-eviction': '0'
            },
            'db': {
                'db-type': 'sqlite',
                'sqlite-db-path': os.path.join(self.get_test_dir(), 'local_server.db'),
                'connection-pool-size': '1'
            },
            'session': {
                'session-expiry-time': '300',
                'session-cleanup-interval': '60'
            }
        }

    def server_factory(self):
        return RemoteServer(self.get_config())

    def test_session_api(self):
        self.start_server()

        r = requests.post(URL.format('/1/login'), auth=('psadmin', 'psadm1n'))
        self.assertEqual(r.status_code, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(r.json()['error'], 'INCORRECT_PASSWORD')
        r = requests.post(URL.format('/1/login'), auth=('psadm1n', 'psadmin'))
        self.assertEqual(r.status_code, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(r.json()['error'], 'USER_NOT_FOUND')
        r = requests.post(URL.format('/1/login'), auth=('psadmin', 'psadmin'))
        self.assertEqual(r.status_code, HTTPStatus.OK)
        session_id = r.headers.get('x-privastore-session-id')
        self.assertTrue(session_id is not None)
        try:
            Sessions.validate_session(session_id)
        except:
            self.fail('Expected valid session!')
        inv_session_id = 'S-{}'.format(str(uuid.uuid4()))
        r = requests.put(URL.format('/1/heartbeat'), headers={'x-privastore-session-id':inv_session_id})
        self.assertEqual(r.status_code, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(r.json()['error'], 'SESSION_NOT_FOUND')
        r = requests.put(URL.format('/1/heartbeat'), headers={'x-privastore-session-id':session_id})
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = requests.post(URL.format('/1/logout'), headers={'x-privastore-session-id':inv_session_id})
        self.assertEqual(r.status_code, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(r.json()['error'], 'SESSION_NOT_FOUND')
        r = requests.post(URL.format('/1/logout'), headers={'x-privastore-session-id':session_id})
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = requests.put(URL.format('/1/heartbeat'), headers={'x-privastore-session-id':session_id})
        self.assertEqual(r.status_code, HTTPStatus.UNAUTHORIZED)
        self.assertEqual(r.json()['error'], 'SESSION_NOT_FOUND')
from http import HTTPStatus
import os
import random
import requests
import shutil
import urllib
import uuid
from .local_server import LocalServer
from .remote_server import RemoteServer
from .session import Sessions
import time
from .test_server import TestServer, HOSTNAME, PORT, URL

REMOTE_PORT = 9090
REMOTE_URL = "http://{}:{}{{}}".format(HOSTNAME, REMOTE_PORT)

class TestLocalServer(TestServer):
    
    def get_test_dir(self):
        return 'test_local_server'

    def get_remote_dir(self):
        return os.path.join(self.get_test_dir(), 'remote')

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
                'api-port': str(PORT)
            },
            'auth': {
                'auth-type': 'db'
            },
            'store': {
                'store-path': os.path.join(self.get_test_dir(), 'cache'),
                'max-file-size': '500MB',
                'store-size': '1GB',
                'chunk-size': '1MB'
            },
            'db': {
                'db-type': 'sqlite',
                'sqlite-db-path': os.path.join(self.get_test_dir(), 'local_server.db'),
                'connection-pool-size': '1'
            },
            'remote': {
                'enable-remote-server': '0'
            },
            'session': {
                'session-expiry-time': '300',
                'session-cleanup-interval': '60'
            }
        }
        return self.config

    def get_remote_config(self):
        if self.remote_config:
            return self.remote_config

        self.remote_config = {
            'logging': {
              'log-level': 'DEBUG'  
            },
            'api': {
                'api-type': 'http',
                'api-hostname': HOSTNAME,
                'api-port': str(REMOTE_PORT)
            },
            'auth': {
                'auth-type': 'config',
                'username': 'psadmin',
                'password-hash': '67755C157F6CF48FA66C5193AEEBC73A32EA92EDFD301E678EFE9C8D727F13DD'
            },
            'store': {
                'store-path': os.path.join(self.get_remote_dir(), 'cache'),
                'max-file-size': '500MB',
                'store-size': '1GB',
                'chunk-size': '1.5MB',
                'enable-file-eviction': '0'
            },
            'db': {
                'db-type': 'sqlite',
                'sqlite-db-path': os.path.join(self.get_remote_dir(), 'remote_server.db'),
                'connection-pool-size': '1'
            },
            'session': {
                'session-expiry-time': '300',
                'session-cleanup-interval': '60'
            }
        }
        return self.remote_config

    def enable_remote(self):
        self.config['remote'] = {
            'enable-remote-server': '1',
            'worker-io-timeout': '90',
            'worker-queue-size': '100',
            'worker-retry-interval': '30',
            'num-download-workers': '2',
            'num-upload-workers': '2'
        }

    def setUp(self):
        super().setUp()
        os.mkdir(os.path.join(self.get_test_dir(), 'remote'))
        self.remote_config = None
        self.remote_config = self.get_remote_config()
        self.remote_server = None

    def tearDown(self):
        if self.remote_server is not None:
            self.remote_server.stop()
            self.remote_server.join()
        super().tearDown()

    def server_factory(self):
        return LocalServer(self.get_config())

    def start_remote_server(self):
        self.remote_server = RemoteServer(self.get_remote_config())
        self.remote_server.setup_db()
        self.remote_server.start()
        self.remote_server.wait_started()

    def check_file_remote(self, file_id: str, file_size: int, headers: dict, check_removed: bool=False, timeout: float=30):
        r = requests.get(REMOTE_URL.format('/1/file/{}/metadata'.format(file_id)), headers=headers, timeout=timeout)

        if check_removed:
            if r.status_code != HTTPStatus.OK and r.status_code != HTTPStatus.NOT_FOUND:
                self.fail('Unexpected file metadata response code {}'.format(str(r.status_code)))
            return r.status_code == HTTPStatus.NOT_FOUND

        if r.status_code != HTTPStatus.OK:
            self.fail('Unexpected file metadata response code {}'.format(str(r.status_code)))
        r = r.json()
        return r['file-size'] == file_size

    def check_file_synced(self, file_path: str, headers: dict(), timeout: float=30, synced_local: bool=True, synced_remote: bool=True):
        r = requests.get(URL.format('/1/file{}'.format(file_path)), headers=headers, timeout=timeout)
        if r.status_code != HTTPStatus.OK:
            self.fail('Unexpected file metadata response code {}'.format(str(r.status_code)))
        r = r.json()
        local_transfer_status = r['versions'][0]['local-transfer-status']
        remote_transfer_status = r['versions'][0]['remote-transfer-status']
        if synced_remote:
            if remote_transfer_status != 'SYNCED_DATA':
                return False
        if synced_local:
            if local_transfer_status != 'SYNCED_DATA':
                return False
        
        return True
    
    def send_login(self):
        r = requests.post(URL.format('/1/login'), auth=('psadmin', 'psadmin'))
        self.assertEqual(r.status_code, HTTPStatus.OK)
        return r.headers.get('x-privastore-session-id')

    def send_remote_login(self):
        r = requests.post(REMOTE_URL.format('/1/login'), auth=('psadmin', 'psadmin'))
        self.assertEqual(r.status_code, HTTPStatus.OK)
        return r.headers.get('x-privastore-session-id')

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
    
    def test_directory_api(self):
        self.start_server()

        session_id = self.send_login()
        inv_session_id = 'S-{}'.format(str(uuid.uuid4()))
        req_headers = {
            'x-privastore-session-id': session_id
        }

        r = self.send_request(URL.format('/1/directory/'), headers={})
        self.assertEqual(r.status_code, HTTPStatus.BAD_REQUEST)
        r = self.send_request(URL.format('/1/directory/'), headers={'x-privastore-session-id':inv_session_id})
        self.assertEqual(r.status_code, HTTPStatus.UNAUTHORIZED)
        r = self.send_request(URL.format('/1/directory'), req_headers)
        self.assertEqual(r.status_code, HTTPStatus.BAD_REQUEST)
        r = self.send_request(URL.format('/1/directory/'), req_headers)
        self.assertEqual(r, [])

        r = self.send_request(URL.format('/1/directory/dir_1'), headers={}, method=requests.put)
        self.assertEqual(r.status_code, HTTPStatus.BAD_REQUEST)
        r = self.send_request(URL.format('/1/directory/dir_1'), headers={'x-privastore-session-id':inv_session_id}, method=requests.put)
        self.assertEqual(r.status_code, HTTPStatus.UNAUTHORIZED)
        r = self.send_request(URL.format('/1/directory/dir_1'), req_headers, method=requests.put)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/directory/dir_1'), req_headers, method=requests.put)
        self.assertEqual(r.status_code, HTTPStatus.CONFLICT)
        r = self.send_request(URL.format('/1/directory/dir_1/dir_1a'), req_headers, method=requests.put)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/directory/dir_1/dir_1a'), req_headers, method=requests.put)
        self.assertEqual(r.status_code, HTTPStatus.CONFLICT)
        r = self.send_request(URL.format('/1/directory/dir_1/dir_1b'), req_headers, method=requests.put)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/directory/dir_2'), req_headers, method=requests.put)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/directory/'), req_headers)
        self.assertEqual(r, [['d', 'dir_1'], ['d', 'dir_2']])
        r = self.send_request(URL.format('/1/directory/dir_1'), req_headers)
        self.assertEqual(r, [['d', 'dir_1a'], ['d', 'dir_1b']])
        r = self.send_request(URL.format('/1/directory/dir_1/dir_1a'), req_headers)
        self.assertEqual(r, [])
        r = self.send_request(URL.format('/1/directory/dir_2'), req_headers)
        self.assertEqual(r, [])
        quoted_dir = urllib.parse.quote('Foo Bar?')
        r = self.send_request(URL.format('/1/directory/{}'.format(quoted_dir)), req_headers, method=requests.put)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/directory/{}/foo'.format(quoted_dir)), req_headers, method=requests.put)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/directory/{}/bar'.format(quoted_dir)), req_headers, method=requests.put)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/directory/'), req_headers)
        self.assertEqual(r, [['d', 'Foo Bar?'], ['d', 'dir_1'], ['d', 'dir_2']])
        r = self.send_request(URL.format('/1/directory/{}/foo'.format(quoted_dir)), req_headers)
        self.assertEqual(r, [])
        r = self.send_request(URL.format('/1/directory/{}/bar'.format(quoted_dir)), req_headers)
        self.assertEqual(r, [])

    def test_file_api(self):
        self.enable_remote()
        self.start_server()
        self.start_remote_server()

        session_id = self.send_login()
        inv_session_id = 'S-{}'.format(str(uuid.uuid4()))
        req_headers = {
            'x-privastore-session-id': session_id
        }

        remote_session_id = self.send_remote_login()
        remote_req_headers = {
            'x-privastore-session-id': remote_session_id
        }

        r = self.send_request(URL.format('/1/directory/dir_1'), req_headers, method=requests.put)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/directory/dir_1/dir_1a'), req_headers, method=requests.put)
        self.assertEqual(r.status_code, HTTPStatus.OK)

        # TODO: Test with different content types.
        req_headers['Content-Type'] = 'application/octet-stream'

        small_file = random.randbytes(500*1024)
        chunk_file = random.randbytes(1024*1024)
        large_file = random.randbytes(5*1024*1024)

        r = self.send_request(URL.format('/1/upload/file_1'), data=small_file, headers={}, method=requests.post)
        self.assertEqual(r.status_code, HTTPStatus.BAD_REQUEST)
        r = self.send_request(URL.format('/1/upload/file_1'), data=small_file, headers={'x-privastore-session-id':inv_session_id}, method=requests.post)
        self.assertEqual(r.status_code, HTTPStatus.UNAUTHORIZED)
        r = self.send_request(URL.format('/1/upload/dir_1'), data=small_file, headers=req_headers, method=requests.post)
        self.assertEqual(r.status_code, HTTPStatus.CONFLICT)
        r = self.send_request(URL.format('/1/upload/file_1'), data=small_file, headers=req_headers, method=requests.post)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/file/file_1'), headers=req_headers, method=requests.get)
        self.assertEqual(r['versions'][0]['version'], 1)
        self.assertEqual(r['versions'][0]['file-size'], len(small_file))
        file_1_size = r['versions'][0]['size-on-disk']
        self.assertTrue(file_1_size >= len(small_file))
        self.assertEqual(r['versions'][0]['total-chunks'], 1)
        file_1_id = r['versions'][0]['local-file-id']
        file_1_remote_id = r['versions'][0]['remote-file-id']
        self.assertTrue(self.wait_for(self.check_file_synced, args=['/file_1', req_headers]))
        self.assertTrue(self.check_file_remote(file_1_remote_id, file_1_size, headers=remote_req_headers))
        r = self.send_request(URL.format('/1/upload/file_1'), data=small_file, headers=req_headers, method=requests.post)
        self.assertEqual(r.status_code, HTTPStatus.CONFLICT)

        r = self.send_request(URL.format('/1/download/file_1'), headers={}, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.BAD_REQUEST)
        r = self.send_request(URL.format('/1/download/file_1'), headers={'x-privastore-session-id':inv_session_id}, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.UNAUTHORIZED)
        r = self.send_request(URL.format('/1/download/file_2'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.NOT_FOUND)
        r = self.send_request(URL.format('/1/download/file_1'), headers=req_headers, method=requests.get)
        self.assertTrue(r.headers.get('Content-Type') is not None)
        self.assertEqual(r.headers.get('Content-Type'), 'application/octet-stream')
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, small_file)

        r = self.send_request(URL.format('/1/upload/file_2'), data=chunk_file, headers=req_headers, method=requests.post)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/file/file_2'), headers=req_headers, method=requests.get)
        self.assertEqual(r['versions'][0]['version'], 1)
        self.assertEqual(r['versions'][0]['file-size'], len(chunk_file))
        file_2_size = r['versions'][0]['size-on-disk']
        self.assertTrue(file_2_size >= len(chunk_file))
        self.assertEqual(r['versions'][0]['total-chunks'], 1)
        file_2_id = r['versions'][0]['local-file-id']
        file_2_remote_id = r['versions'][0]['remote-file-id']
        self.assertTrue(self.wait_for(self.check_file_synced, args=['/file_2', req_headers]))
        self.assertTrue(self.check_file_remote(file_2_remote_id, file_2_size, headers=remote_req_headers))
        r = self.send_request(URL.format('/1/upload/file_3'), data=large_file, headers=req_headers, method=requests.post)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/file/file_3'), headers=req_headers, method=requests.get)
        self.assertEqual(r['versions'][0]['version'], 1)
        self.assertEqual(r['versions'][0]['file-size'], len(large_file))
        file_3_size = r['versions'][0]['size-on-disk']
        self.assertTrue(file_3_size >= len(large_file))
        self.assertEqual(r['versions'][0]['total-chunks'], 5)
        file_3_id = r['versions'][0]['local-file-id']
        file_3_remote_id = r['versions'][0]['remote-file-id']
        self.assertTrue(self.wait_for(self.check_file_synced, args=['/file_3', req_headers]))
        self.assertTrue(self.check_file_remote(file_3_remote_id, file_3_size, headers=remote_req_headers))
        r = self.send_request(URL.format('/1/upload/dir_1/file_1'), data=small_file, headers=req_headers, method=requests.post)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/upload/dir_1/dir_1a/file_1'), data=small_file, headers=req_headers, method=requests.post)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/download/file_2'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, chunk_file)
        r = self.send_request(URL.format('/1/download/file_3'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, large_file)
        r = self.send_request(URL.format('/1/download/dir_1/file_1'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, small_file)
        r = self.send_request(URL.format('/1/download/dir_1/dir_1a/file_1'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, small_file)

        self.restart_server()

        session_id = self.send_login()
        req_headers = {
            'x-privastore-session-id': session_id
        }

        r = self.send_request(URL.format('/1/download/file_1'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, small_file)
        r = self.send_request(URL.format('/1/download/file_2'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, chunk_file)
        r = self.send_request(URL.format('/1/download/file_3'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, large_file)
        r = self.send_request(URL.format('/1/download/dir_1/file_1'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, small_file)
        r = self.send_request(URL.format('/1/download/dir_1/dir_1a/file_1'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, small_file)

        self.stop_server()
        shutil.rmtree(os.path.join(self.get_test_dir(), 'cache', file_1_id))
        shutil.rmtree(os.path.join(self.get_test_dir(), 'cache', file_3_id))
        self.restart_server()

        session_id = self.send_login()
        req_headers = {
            'x-privastore-session-id': session_id
        }

        r = self.send_request(URL.format('/1/download/file_1'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, small_file)
        r = self.send_request(URL.format('/1/download/file_2'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, chunk_file)
        r = self.send_request(URL.format('/1/download/file_3'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, large_file)

        r = self.send_request(URL.format('/1/file/file_1'), headers=req_headers, method=requests.delete)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/download/file_1'), headers=req_headers, method=requests.head)
        self.assertEqual(r.status_code, HTTPStatus.NOT_FOUND)
        self.assertTrue(self.wait_for(self.check_file_remote, args=[file_1_remote_id, file_1_size, remote_req_headers], kwargs={'check_removed':True}, timeout=1))

        r = self.send_request(URL.format('/1/file/dir_1/file_1'), headers=req_headers, method=requests.get)
        file_1_remote_id = r['versions'][0]['remote-file-id']
        r = self.send_request(URL.format('/1/file/dir_1/file_1'), headers=req_headers, method=requests.delete)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/download/dir_1/file_1'), headers=req_headers, method=requests.head)
        self.assertEqual(r.status_code, HTTPStatus.NOT_FOUND)
        self.assertTrue(self.wait_for(self.check_file_remote, args=[file_1_remote_id, file_1_size, remote_req_headers], kwargs={'check_removed':True}, timeout=1))

        self.stop_server()
        # Clear the cache to force downloads from the remote server.
        shutil.rmtree(os.path.join(self.get_test_dir(), 'cache'))
        self.restart_server()

        session_id = self.send_login()
        req_headers = {
            'x-privastore-session-id': session_id
        }

        r = self.send_request(URL.format('/1/download/file_1'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.NOT_FOUND)
        r = self.send_request(URL.format('/1/download/file_2'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, chunk_file)
        r = self.send_request(URL.format('/1/download/file_3'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, large_file)
        r = self.send_request(URL.format('/1/download/dir_1/file_1'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.NOT_FOUND)
        r = self.send_request(URL.format('/1/download/dir_1/dir_1a/file_1'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, small_file)
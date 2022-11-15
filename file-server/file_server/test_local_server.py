from http import HTTPStatus
import os
import random
import requests
import urllib
import uuid
from .local_server import LocalServer
from .session import Sessions
from .test_server import TestServer, HOSTNAME, PORT, URL

class TestLocalServer(TestServer):
    
    def get_test_dir(self):
        return 'test_local_server'

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
            'encryption': {
                'key-algorithm': 'aes-128-cbc',
                'key-bytes': '770A8A65DA156D24EE2A093277530142'
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

    def server_factory(self):
        return LocalServer(self.get_config())

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

        r = requests.post(URL.format('/1/login'), auth=('psadmin', 'psadmin'))
        self.assertEqual(r.status_code, HTTPStatus.OK)
        session_id = r.headers.get('x-privastore-session-id')
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
        self.start_server()

        r = requests.post(URL.format('/1/login'), auth=('psadmin', 'psadmin'))
        self.assertEqual(r.status_code, HTTPStatus.OK)
        session_id = r.headers.get('x-privastore-session-id')
        inv_session_id = 'S-{}'.format(str(uuid.uuid4()))
        req_headers = {
            'x-privastore-session-id': session_id
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
        r = self.send_request(URL.format('/1/upload/file_3'), data=large_file, headers=req_headers, method=requests.post)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/upload/dir_1/file_1'), data=small_file, headers=req_headers, method=requests.post)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/upload/dir_1/file_2'), data=chunk_file, headers=req_headers, method=requests.post)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/upload/dir_1/file_3'), data=large_file, headers=req_headers, method=requests.post)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/upload/dir_1/dir_1a/file_1'), data=small_file, headers=req_headers, method=requests.post)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/upload/dir_1/dir_1a/file_2'), data=chunk_file, headers=req_headers, method=requests.post)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/upload/dir_1/dir_1a/file_3'), data=large_file, headers=req_headers, method=requests.post)
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
        r = self.send_request(URL.format('/1/download/dir_1/file_2'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, chunk_file)
        r = self.send_request(URL.format('/1/download/dir_1/file_3'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, large_file)
        r = self.send_request(URL.format('/1/download/dir_1/dir_1a/file_1'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, small_file)
        r = self.send_request(URL.format('/1/download/dir_1/dir_1a/file_2'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, chunk_file)
        r = self.send_request(URL.format('/1/download/dir_1/dir_1a/file_3'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, large_file)

        self.restart_server()

        r = requests.post(URL.format('/1/login'), auth=('psadmin', 'psadmin'))
        self.assertEqual(r.status_code, HTTPStatus.OK)
        session_id = r.headers.get('x-privastore-session-id')
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
        r = self.send_request(URL.format('/1/download/dir_1/file_2'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, chunk_file)
        r = self.send_request(URL.format('/1/download/dir_1/file_3'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, large_file)
        r = self.send_request(URL.format('/1/download/dir_1/dir_1a/file_1'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, small_file)
        r = self.send_request(URL.format('/1/download/dir_1/dir_1a/file_2'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, chunk_file)
        r = self.send_request(URL.format('/1/download/dir_1/dir_1a/file_3'), headers=req_headers, method=requests.get)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, large_file)
from http import HTTPStatus
import os
import random
import requests
import uuid
from .file import File
from .remote_server import RemoteServer
from .session import Sessions
from .test_server import TestServer, HOSTNAME, PORT, URL

class TestRemoteServer(TestServer):
    
    def get_test_dir(self):
        return 'test_remote_server'

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
        return self.config

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
    
    def test_create_file(self):
        self.start_server()
        r = requests.post(URL.format('/1/login'), auth=('psadmin', 'psadmin'))
        self.assertEqual(r.status_code, HTTPStatus.OK)
        session_id = r.headers.get('x-privastore-session-id')
        req_headers = {
            'x-privastore-session-id': session_id,
            'x-privastore-epoch-no': '1'
        }
        inv_session_id = 'S-{}'.format(str(uuid.uuid4()))
        r = self.send_request(URL.format('/1/file'), method=requests.post, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        file_1_id = r.headers.get('x-privastore-remote-file-id')
        self.assertTrue(file_1_id is not None)
        self.assertTrue(File.is_valid_file_id(file_1_id))
        r = self.send_request(URL.format('/1/file?size=1000'), method=requests.post, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        file_2_id = r.headers.get('x-privastore-remote-file-id')
        self.assertTrue(file_2_id is not None)
        self.assertTrue(File.is_valid_file_id(file_2_id))
        self.assertNotEqual(file_1_id, file_2_id)
        file_1_metadata = self.send_request(URL.format('/1/file/{}/metadata'.format(file_1_id)), headers=req_headers)
        self.assertTrue(file_1_metadata is not None)
        self.assertEqual(file_1_metadata.get('remote-file-id'), file_1_id)
        self.assertEqual(file_1_metadata.get('file-size'), 0)
        self.assertEqual(file_1_metadata.get('file-chunks'), 0)
        self.assertEqual(file_1_metadata.get('is-committed'), False)
        file_2_metadata = self.send_request(URL.format('/1/file/{}/metadata'.format(file_2_id)), headers=req_headers)
        self.assertTrue(file_2_metadata is not None)
        self.assertEqual(file_2_metadata.get('remote-file-id'), file_2_id)
        self.assertEqual(file_2_metadata.get('file-size'), 1000)
        self.assertEqual(file_1_metadata.get('file-chunks'), 0)
        self.assertEqual(file_2_metadata.get('is-committed'), False)

    def test_commit_file(self):
        self.start_server()
        r = requests.post(URL.format('/1/login'), auth=('psadmin', 'psadmin'))
        self.assertEqual(r.status_code, HTTPStatus.OK)
        session_id = r.headers.get('x-privastore-session-id')
        req_headers = {
            'x-privastore-session-id': session_id,
            'x-privastore-epoch-no': '1'
        }
        r = self.send_request(URL.format('/1/file?size=1000'), method=requests.post, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        file_1_id = r.headers.get('x-privastore-remote-file-id')
        self.assertTrue(file_1_id is not None)
        self.assertTrue(File.is_valid_file_id(file_1_id))
        chunk_1 = random.randbytes(1000)
        chunk_2 = random.randbytes(500)
        chunk_3 = random.randbytes(500)
        r = self.send_request(URL.format('/1/file/{}?chunk=1'.format(file_1_id)), data=chunk_1, method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/file/{}/commit'.format(file_1_id)), method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        file_1_metadata = self.send_request(URL.format('/1/file/{}/metadata'.format(file_1_id)), headers=req_headers)
        self.assertTrue(file_1_metadata is not None)
        self.assertEqual(file_1_metadata.get('remote-file-id'), file_1_id)
        self.assertEqual(file_1_metadata.get('file-size'), 1000)
        self.assertEqual(file_1_metadata.get('file-chunks'), 1)
        self.assertEqual(file_1_metadata.get('is-committed'), True)
        r = self.send_request(URL.format('/1/file/{}/commit'.format(file_1_id)), data=chunk_1, method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/file/{}?chunk=2'.format(file_1_id)), data=chunk_2, method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.CONFLICT)
        self.assertEqual(r.json().get('error'), 'FILE_IS_COMMITTED')
        r = self.send_request(URL.format('/1/file?size=2000'), method=requests.post, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        file_2_id = r.headers.get('x-privastore-remote-file-id')
        r = self.send_request(URL.format('/1/file/{}?chunk=1'.format(file_2_id)), data=chunk_1, method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/file/{}?chunk=2'.format(file_2_id)), data=chunk_2, method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/file/{}/commit'.format(file_2_id)), method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.CONFLICT)
        self.assertEqual(r.json().get('error'), 'FILE_TOO_SMALL')
        r = self.send_request(URL.format('/1/file/{}?chunk=3'.format(file_2_id)), data=chunk_3, method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/file/{}/commit'.format(file_2_id)), method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        file_2_metadata = self.send_request(URL.format('/1/file/{}/metadata'.format(file_2_id)), headers=req_headers)
        self.assertTrue(file_2_metadata is not None)
        self.assertEqual(file_2_metadata.get('remote-file-id'), file_2_id)
        self.assertEqual(file_2_metadata.get('file-size'), 2000)
        self.assertEqual(file_2_metadata.get('file-chunks'), 3)
        self.assertEqual(file_2_metadata.get('is-committed'), True)

    def test_write_remote_file(self):
        self.config['store']['chunk-size'] = '1000B'
        self.start_server()
        r = requests.post(URL.format('/1/login'), auth=('psadmin', 'psadmin'))
        self.assertEqual(r.status_code, HTTPStatus.OK)
        session_id = r.headers.get('x-privastore-session-id')
        req_headers = {
            'x-privastore-session-id': session_id,
            'x-privastore-epoch-no': '1'
        }
        r = self.send_request(URL.format('/1/file?size=1500'), method=requests.post, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        file_1_id = r.headers.get('x-privastore-remote-file-id')
        chunk_1 = random.randbytes(1000)
        chunk_2 = random.randbytes(1500)
        chunk_3 = random.randbytes(500)
        r = self.send_request(URL.format('/1/file/{}?chunk=1'.format(file_1_id)), data=chunk_1, method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/file/{}?chunk=2'.format(file_1_id)), data=chunk_2, method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.BAD_REQUEST)
        self.assertEqual(r.json()['error'], 'FILE_CHUNK_TOO_LARGE')
        r = self.send_request(URL.format('/1/file/{}?chunk=2'.format(file_1_id)), data=chunk_3, method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/file/{}?chunk=3'.format(file_1_id)), data=chunk_3, method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.CONFLICT)
        self.assertEqual(r.json()['error'], 'FILE_TOO_LARGE')
        r = self.send_request(URL.format('/1/file/{}/commit'.format(file_1_id)), method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        file_1_metadata = self.send_request(URL.format('/1/file/{}/metadata'.format(file_1_id)), headers=req_headers)
        self.assertTrue(file_1_metadata is not None)
        self.assertEqual(file_1_metadata.get('remote-file-id'), file_1_id)
        self.assertEqual(file_1_metadata.get('file-size'), 1500)
        self.assertEqual(file_1_metadata.get('file-chunks'), 2)
        self.assertEqual(file_1_metadata.get('is-committed'), True)

    def test_read_remote_file(self):
        self.start_server()
        r = requests.post(URL.format('/1/login'), auth=('psadmin', 'psadmin'))
        self.assertEqual(r.status_code, HTTPStatus.OK)
        session_id = r.headers.get('x-privastore-session-id')
        req_headers = {
            'x-privastore-session-id': session_id,
            'x-privastore-epoch-no': '1'
        }
        r = self.send_request(URL.format('/1/file?size=2000'), method=requests.post, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        file_1_id = r.headers.get('x-privastore-remote-file-id')
        chunk_1 = random.randbytes(1000)
        chunk_2 = random.randbytes(500)
        chunk_3 = random.randbytes(500)
        r = self.send_request(URL.format('/1/file/{}?chunk=1'.format(file_1_id)), data=chunk_1, method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/file/{}?chunk=2'.format(file_1_id)), data=chunk_2, method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/file/{}?chunk=3'.format(file_1_id)), data=chunk_3, method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/file/{}?chunk=1'.format(file_1_id)), method=requests.get, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.CONFLICT)
        self.assertEqual(r.json()['error'], 'FILE_IS_UNCOMMITTED')
        r = self.send_request(URL.format('/1/file/{}/commit'.format(file_1_id)), method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/file/{}?chunk=1'.format(file_1_id)), method=requests.get, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, chunk_1)
        r = self.send_request(URL.format('/1/file/{}?chunk=3'.format(file_1_id)), method=requests.get, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, chunk_3)
        r = self.send_request(URL.format('/1/file/{}?chunk=2'.format(file_1_id)), method=requests.get, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        self.assertEqual(r.content, chunk_2)
        r = self.send_request(URL.format('/1/file/{}?chunk=4'.format(file_1_id)), method=requests.get, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.BAD_REQUEST)
        self.assertEqual(r.json()['error'], 'INVALID_CHUNK_NUM')
        r = self.send_request(URL.format('/1/file/{}?chunk=-1'.format(file_1_id)), method=requests.get, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.BAD_REQUEST)

    def test_end_epoch(self):
        self.start_server()
        r = requests.post(URL.format('/1/login'), auth=('psadmin', 'psadmin'))
        self.assertEqual(r.status_code, HTTPStatus.OK)
        session_id = r.headers.get('x-privastore-session-id')
        req_headers = {
            'x-privastore-session-id': session_id,
            'x-privastore-epoch-no': '1'
        }
        r = self.send_request(URL.format('/1/epoch/1'), method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/epoch/1'), method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.CONFLICT)
        self.assertEqual(r.json()['error'], 'EPOCH_IS_OVER')
        r = self.send_request(URL.format('/1/file?size=2000'), method=requests.post, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.CONFLICT)
        self.assertEqual(r.json()['error'], 'EPOCH_IS_OVER')
        req_headers['x-privastore-epoch-no'] = '2'
        r = self.send_request(URL.format('/1/file?size=1000'), method=requests.post, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        file_1_id = r.headers.get('x-privastore-remote-file-id')
        chunk_1 = random.randbytes(1000)
        chunk_2 = random.randbytes(1000)
        r = self.send_request(URL.format('/1/file/{}?chunk=1'.format(file_1_id)), data=chunk_1, method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/epoch/2?marker-id={}'.format(file_1_id)), method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.CONFLICT)
        self.assertEqual(r.json()['error'], 'FILE_IS_UNCOMMITTED')
        r = self.send_request(URL.format('/1/file/{}/commit'.format(file_1_id)), method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/file?size=2000'), method=requests.post, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        file_2_id = r.headers.get('x-privastore-remote-file-id')
        r = self.send_request(URL.format('/1/file/{}?chunk=1'.format(file_2_id)), data=chunk_1, method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/file/{}?chunk=2'.format(file_2_id)), data=chunk_2, method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/epoch/2?marker-id={}'.format(file_1_id)), method=requests.put, headers=req_headers)
        self.assertEqual(r.status_code, HTTPStatus.OK)
        r = self.send_request(URL.format('/1/file/{}/metadata'.format(file_1_id)), method=requests.get, headers=req_headers)
        self.assertTrue(r.get('error') is None)
        r = self.send_request(URL.format('/1/file/{}/metadata'.format(file_2_id)), method=requests.get, headers=req_headers)
        self.assertEqual(r.json()['error'], 'FILE_NOT_FOUND')
        

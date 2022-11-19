from .api.http.http_request_handler import SESSION_ID_HEADER
from collections import namedtuple
from .error import FileServerErrorCode, RemoteClientError
from http import HTTPStatus
import logging
import random
from .remote.api.http.http_request_handler import EPOCH_NO_HEADER, FILE_ID_HEADER
import requests
import time
from typing import Optional

RemoteFileMetadata = namedtuple('RemoteFileMetadata', ['file_size', 'file_store_usage', 'file_chunks', 'created_epoch', 'removed_epoch'])

class RemoteEndpoint(object):

    def __init__(self, host: str, port: int, ssl: bool = False):
        self._host = host
        self._port = port
        self._ssl = ssl
    
    def http_url(self):
        protocol = 'https' if self._ssl else 'http'
        host = self._host
        port = self._port
        return f'{protocol}://{host}:{port}'
    
    def __str__(self):
        host = self._host
        port = self._port
        return f'{host}:{port}'

class RemoteCredentials(object):

    def __init__(self, username: str, password: str):
        self._username = username
        self._password = password

    def username(self):
        return self._username
    
    def password(self):
        return self._password
    
    def to_tuple(self):
        return (self._username, self._password)

class RemoteClient(object):

    def __init__(self, host: str=None, port: int=None, ssl: bool = False, remote_creds: RemoteCredentials = None, retry_interval: int = 1):
        self._remote_creds = remote_creds
        self._retry_interval = retry_interval
        self._endpoints: list[RemoteEndpoint] = []
        if host is not None and port is not None:
            self.add_remote_endpoint(RemoteEndpoint(host, port, ssl))
        self._session_id: str = None
    
    def set_retry_interval(self, retry_interval: int) -> None:
        self._retry_interval = retry_interval

    def retry_interval(self) -> int:
        return self._retry_interval

    def add_remote_endpoint(self, endpoint: RemoteEndpoint) -> None:
        self._endpoints.append(endpoint)
    
    def get_remote_endpoint(self) -> RemoteEndpoint:
        if len(self._endpoints) == 0:
            raise RemoteClientError('No remote server endpoints!')

        endpoint = random.choice(self._endpoints)
        logging.debug('Using remote endpoint [{}]'.format(str(endpoint)))
        return endpoint

    def set_remote_credentials(self, creds: RemoteCredentials) -> None:
        self._remote_creds = creds

    def get_remote_credentials(self) -> RemoteCredentials:
        return self._remote_creds

    def login_url(self, endpoint: RemoteEndpoint):
        return f'{endpoint.http_url()}/1/login'

    def session_heartbeat_url(self, endpoint: RemoteEndpoint):
        return f'{endpoint.http_url()}/1/heartbeat'

    def create_file_url(self, endpoint: RemoteEndpoint, file_size: Optional[int] = None):
        if file_size is not None:
            return f'{endpoint.http_url()}/1/file?size={file_size}'
        return f'{endpoint.http_url()}/1/file'

    def file_chunk_url(self, endpoint: RemoteEndpoint, file_id: str, chunk_offset: int):
        return f'{endpoint.http_url()}/1/file/{file_id}?chunk={chunk_offset}'

    def commit_url(self, endpoint: RemoteEndpoint, file_id: str):
        return f'{endpoint.http_url()}/1/file/{file_id}/commit'

    def session_expired(self):
        logging.warn('Session [{}] expired'.format(self._session_id))
        self._session_id = None

    def get_error_code(self, response: requests.Response) -> str:
        if response is not None:
            try:
                return response.json()['error']
            except:
                pass
        
        logging.warn('Remote server did not send back error code!')

        return FileServerErrorCode.REMOTE_ERROR

    def heartbeat_session(self, timeout: int = 90) -> None:
        if self._session_id is None:
            raise RemoteClientError('No session id!')

        start_t = time.time()
        end_t = start_t + timeout

        headers = dict()
        headers[SESSION_ID_HEADER] = self._session_id

        while True:
            now = time.time()
            if timeout <= 0 or now >= end_t:
                raise RemoteClientError('Timed out heartbeating session [{}]!'.format(self._session_id), FileServerErrorCode.IO_TIMEOUT)
            
            endpoint = self.get_remote_endpoint()

            try:
                r = requests.put(self.session_heartbeat_url(endpoint), headers=headers, timeout=(end_t-now))
            except Exception as e:
                logging.error('Session [{}] heartbeat request error: {}'.format(self._session_id, str(e)))
                time.sleep(self.retry_interval())
                continue
            
            if r.status_code == HTTPStatus.OK:
                logging.debug('Session [{}] heartbeat'.format(self._session_id))
                return
            elif r.status_code != HTTPStatus.UNAUTHORIZED:
                raise RemoteClientError('Session [{}] heartbeat error [{}]'.format(self._session_id, self.get_error_code(r)), FileServerErrorCode.REMOTE_ERROR)

            self.session_expired()
            raise RemoteClientError('Session [{}] expired'.format(self._session_id))

    def get_session_id(self, timeout: int = 90) -> str:
        if self._session_id is not None:
            return self._session_id

        start_t = time.time()
        end_t = start_t + timeout
      
        remote_creds = self.get_remote_credentials().to_tuple()

        while True:
            now = time.time()
            if timeout <= 0 or now >= end_t:
                raise RemoteClientError('Timed out starting session!', FileServerErrorCode.IO_TIMEOUT)
            
            endpoint = self.get_remote_endpoint()

            try:
                r = requests.post(self.login_url(endpoint), auth=remote_creds, timeout=(end_t-now))
            except Exception as e:
                logging.error('Start session request error: {}'.format(str(e)))
                time.sleep(self.retry_interval())
                continue
            
            if r.status_code == HTTPStatus.OK:
                self._session_id = r.headers[SESSION_ID_HEADER]
                logging.debug('Session [{}] started'.format(self._session_id))
                return self._session_id
            elif r.status_code == HTTPStatus.UNAUTHORIZED:
                raise RemoteClientError('Invalid remote server credentials!', FileServerErrorCode.REMOTE_AUTH_ERROR)
            else:
                raise RemoteClientError('Start session error [{}]'.format(self.get_error_code(r)), FileServerErrorCode.REMOTE_ERROR)
    
    def create_file(self, file_size: Optional[int] = None, timeout: int = 90) -> str:
        start_t = time.time()
        end_t = start_t + timeout

        headers = dict()

        while True:
            headers[SESSION_ID_HEADER] = self.get_session_id(timeout=(end_t-time.time()))

            now = time.time()
            if timeout <= 0 or now >= end_t:
                raise RemoteClientError('Timed out creating file!', FileServerErrorCode.IO_TIMEOUT)

            endpoint = self.get_remote_endpoint()

            try:
                r = requests.post(self.create_file_url(endpoint, file_size), headers=headers, timeout=(end_t-now))
            except Exception as e:
                logging.error('Create file size [{}] request error: {}'.format(file_size, str(e)))
                time.sleep(self.retry_interval())
                continue

            if r.status_code == HTTPStatus.OK:
                remote_file_id = r.headers.get(FILE_ID_HEADER)
                logging.debug('Created file [{}]'.format(remote_file_id))
                return remote_file_id
            elif r.status_code == HTTPStatus.UNAUTHORIZED:
                self.session_expired()
            else:
                error_code = self.get_error_code(r)
                raise RemoteClientError('Create file size [{}] error [{}]'.format(file_size, error_code), error_code)

    def send_file_chunk(self, remote_file_id: str, chunk_data: bytes, chunk_offset: int, timeout: int = 90) -> Optional[str]:
        start_t = time.time()
        end_t = start_t + timeout

        headers = dict()

        while True:
            headers[SESSION_ID_HEADER] = self.get_session_id(timeout=(end_t-time.time()))

            now = time.time()
            if timeout <= 0 or now >= end_t:
                raise RemoteClientError('Timed out sending file [{}] chunk [{}] data!'.format(remote_file_id, chunk_offset), FileServerErrorCode.IO_TIMEOUT)

            endpoint = self.get_remote_endpoint()

            try:
                r = requests.put(self.file_chunk_url(endpoint, remote_file_id, chunk_offset), headers=headers, data=chunk_data, timeout=(end_t-now))
            except Exception as e:
                logging.error('Send file [{}] chunk [{}] request error: {}'.format(remote_file_id, chunk_offset, str(e)))
                time.sleep(self.retry_interval())
                continue

            if r.status_code == HTTPStatus.OK:
                logging.debug('Sent file [{}] chunk [{}]'.format(remote_file_id, chunk_offset))
                return
            elif r.status_code == HTTPStatus.UNAUTHORIZED:
                self.session_expired()
            else:
                error_code = self.get_error_code(r)
                raise RemoteClientError('Send file [{}] chunk [{}] error [{}]'.format(remote_file_id, chunk_offset, error_code), error_code)


    def commit_file(self, remote_file_id: str, epoch_no: int, timeout: int = 90) -> Optional[str]:
        start_t = time.time()
        end_t = start_t + timeout

        headers = dict()
        headers[EPOCH_NO_HEADER] = str(epoch_no)

        while True:
            headers[SESSION_ID_HEADER] = self.get_session_id(timeout=(end_t-time.time()))

            now = time.time()
            if timeout <= 0 or now >= end_t:
                raise RemoteClientError('Timed out committing remote file [{}] data!'.format(remote_file_id), FileServerErrorCode.IO_TIMEOUT)

            endpoint = self.get_remote_endpoint()

            try:
                r = requests.put(self.commit_url(endpoint, remote_file_id), headers=headers, timeout=(end_t-now))
            except Exception as e:
                logging.error('Commit file [{}] epoch [{}] request error: {}'.format(remote_file_id, epoch_no, str(e)))
                time.sleep(self.retry_interval())
                continue

            if r.status_code == HTTPStatus.OK:
                logging.debug('File [{}] epoch [{}] committed'.format(remote_file_id, epoch_no))
                return
            elif r.status_code == HTTPStatus.UNAUTHORIZED:
                self.session_expired()
            else:
                error_code = self.get_error_code(r)
                raise RemoteClientError('Commit file [{}] epoch [{}] error [{}]'.format(remote_file_id, epoch_no, error_code), error_code)
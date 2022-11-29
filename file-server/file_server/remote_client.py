from .api.http.http_request_handler import SESSION_ID_HEADER
from collections import namedtuple
from .error import FileServerErrorCode, RemoteClientError
from http import HTTPStatus
import logging
import random
from .remote.api.http.http_request_handler import EPOCH_NO_HEADER, FILE_ID_HEADER
import requests
import time
from typing import Optional, Union

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

    def login_path(self):
        return '/1/login'

    def session_heartbeat_path(self):
        return '/1/heartbeat'

    def create_file_path(self, file_size: Optional[int] = None):
        if file_size is not None:
            return f'/1/file?size={file_size}'
        return f'/1/file'

    def file_path(self, file_id: str):
        return f'/1/file/{file_id}'

    def file_chunk_path(self, file_id: str, chunk_offset: int):
        return f'/1/file/{file_id}?chunk={chunk_offset}'

    def commit_path(self, file_id: str):
        return f'/1/file/{file_id}/commit'

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

    def send_remote_request(self, path: str, method=requests.get, headers=dict(), auth=None, data=None, renew_session: bool=False, timeout: float=90) -> Union[requests.Response, str]:
        start_t = time.time()
        end_t = start_t + timeout

        while True:
            now = time.time()
            if timeout <= 0 or now >= end_t:
                return FileServerErrorCode.IO_TIMEOUT
            
            if renew_session:
                headers[SESSION_ID_HEADER] = self.get_session_id(timeout=(end_t - now))
            
            endpoint = self.get_remote_endpoint()
            url = endpoint.http_url() + path

            try:
                r = method(url, auth=auth, data=data, headers=headers, timeout=(end_t-now))
            except Exception as e:
                logging.error('Request error: {}'.format(str(e)))
                time.sleep(self.retry_interval())
                logging.debug('Retrying request ...')
                continue

            logging.debug('Request returned status {}'.format(str(r.status_code)))

            if r.status_code == HTTPStatus.OK:
                return r
            elif r.status_code == HTTPStatus.UNAUTHORIZED:
                if renew_session:
                    self.session_expired()
                    continue

            error_code = self.get_error_code(r)
            logging.debug('Request returned error code {}'.format(error_code))
            return error_code
    
    def heartbeat_session(self, timeout: float=90) -> None:
        if self._session_id is None:
            raise RemoteClientError('No session id!')

        headers = dict()
        headers[SESSION_ID_HEADER] = self._session_id

        logging.debug('Sending session [{}] heartbeat'.format(self._session_id))
        path = self.session_heartbeat_path()
        res = self.send_remote_request(path, method=requests.put, headers=headers, timeout=timeout)
        if isinstance(res, requests.Response):
            logging.debug('Session [{}] heartbeat ok'.format(self._session_id))
            return
        else:
            logging.error('Heartbeat session [{}] error {}'.format(res))
            raise RemoteClientError('Heartbeat session [{}] error {}'.format(res), res)

    def get_session_id(self, timeout: float=90) -> str:
        if self._session_id is not None:
            return self._session_id

        path = self.login_path()
        remote_creds = self.get_remote_credentials()

        logging.debug('Login user [{}]'.format(remote_creds.username()))
        res = self.send_remote_request(path, method=requests.post, auth=remote_creds.to_tuple(), timeout=timeout)
        if isinstance(res, requests.Response):
            self._session_id = session_id = res.headers.get(SESSION_ID_HEADER)
            logging.debug('User [{}] session [{}] started'.format(remote_creds.username(), session_id))
            return session_id
        else:
            logging.error('Login user [{}] error {}'.format(remote_creds.username(), res))
            raise RemoteClientError('Login user [{}] error {}'.format(remote_creds.username(), res), res)
    
    def create_file(self, file_size: Optional[int] = None, timeout: int = 90) -> str:
        path = self.create_file_path(file_size)

        logging.debug('Creating file size [{}]'.format(file_size))
        res = self.send_remote_request(path, method=requests.post, renew_session=True, timeout=timeout)
        if isinstance(res, requests.Response):
            file_id = res.headers.get(FILE_ID_HEADER)
            logging.debug('Created file [{}] size [{}]'.format(file_id, file_size))
            return file_id
        else:
            logging.error('Create file error {}'.format(res))
            raise RemoteClientError('Create file error {}'.format(res), res)

    def remove_file(self, file_id: str, timeout: int = 90) -> str:
        path = self.file_path(file_id)

        logging.debug('Removing file [{}]'.format(file_id))
        res = self.send_remote_request(path, method=requests.delete, renew_session=True, timeout=timeout)
        if isinstance(res, requests.Response):
            logging.debug('Removed file [{}]'.format(file_id))
        else:
            logging.error('Remove file [{}] error {}'.format(file_id, res))
            raise RemoteClientError('Remove file [{}] error {}'.format(file_id, res), res)

    def read_file_chunk(self, remote_file_id: str, chunk_offset: int, timeout: int = 90) -> bytes:
        path = self.file_chunk_path(remote_file_id, chunk_offset)

        logging.debug('Reading file [{}] chunk offset [{}]'.format(remote_file_id, chunk_offset))
        res = self.send_remote_request(path, method=requests.get, renew_session=True, timeout=timeout)
        if isinstance(res, requests.Response):
            chunk = res.content
            chunk_len = len(chunk)
            logging.debug('Read file [{}] chunk offset [{}] size [{}B]'.format(remote_file_id, chunk_offset, chunk_len))
            return chunk
        else:
            logging.error('Read file [{}] chunk [{}] error {}'.format(remote_file_id, chunk_offset, res))
            raise RemoteClientError('Send file [{}] chunk [{}] error {}'.format(remote_file_id, chunk_offset, res), res)

    def send_file_chunk(self, remote_file_id: str, chunk_data: bytes, chunk_offset: int, timeout: int = 90) -> None:
        path = self.file_chunk_path(remote_file_id, chunk_offset)
        chunk_len = len(chunk_data)

        logging.debug('Sending file [{}] chunk offset [{}] size [{}B]'.format(remote_file_id, chunk_offset, chunk_len))
        res = self.send_remote_request(path, method=requests.put, data=chunk_data, renew_session=True, timeout=timeout)
        if isinstance(res, requests.Response):
            logging.debug('Sent file [{}] chunk offset [{}] size [{}B]'.format(remote_file_id, chunk_offset, chunk_len))
            return
        else:
            logging.error('Send file [{}] chunk [{}] size [{}B] error {}'.format(remote_file_id, chunk_offset, chunk_len, res))
            raise RemoteClientError('Send file [{}] chunk [{}] size [{}B] error {}'.format(remote_file_id, chunk_offset, chunk_len, res), res)

    def commit_file(self, remote_file_id: str, epoch_no: int, timeout: int = 90) -> None:
        path = self.commit_path(remote_file_id)

        headers = dict()
        headers[EPOCH_NO_HEADER] = str(epoch_no)

        logging.debug('Commit file [{}] epoch-no [{}]'.format(remote_file_id, epoch_no))
        res = self.send_remote_request(path, method=requests.put, headers=headers, renew_session=True, timeout=timeout)
        if isinstance(res, requests.Response):
            logging.debug('Committed file [{}] epoch-no [{}]'.format(remote_file_id, epoch_no))
            return
        else:
            logging.error('Commit file [{}] epoch-no [{}] error {}'.format(remote_file_id, epoch_no, res))
            raise RemoteClientError('Commit file [{}] epoch-no [{}] error {}'.format(remote_file_id, epoch_no, res), res)
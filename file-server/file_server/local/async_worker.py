from .db.dao_factory import DAOFactory
from ..db.db_conn_mgr import DbConnectionManager
from .db.file_dao import FileVersionMetadata
from ..error import FileServerErrorCode, WorkerError
from ..file_cache import FileCache
from .file_transfer_status import FileTransferStatus
import logging
from queue import Queue
from ..remote_client import RemoteClient, RemoteCredentials, RemoteEndpoint
from typing import Optional
from ..worker import Worker
from ..worker_task import WorkerTask

SESSION_ID_HEADER = 'x-privastore-session-id'

class AsyncWorker(Worker):

    def __init__(self, dao_factory: DAOFactory, db_conn_mgr: DbConnectionManager, store: FileCache, worker_name: str='async-worker', worker_index: int=None, queue_size: int=1, completion_queue: Optional[Queue[WorkerTask]] = None, retry_interval: int=1, io_timeout: int=90):
        super().__init__(worker_name=worker_name, worker_index=worker_index, queue_size=queue_size, completion_queue=completion_queue)
        self._dao_factory = dao_factory
        self._db_conn_mgr = db_conn_mgr
        self._store = store
        self._retry_interval = retry_interval
        self._io_timeout = io_timeout
        self._remote_client = RemoteClient(retry_interval=retry_interval)

        conn = self.db_conn_mgr().db_connect()
        try:
            remote_dao = self.dao_factory().remote_dao(conn)
            servers = remote_dao.get_remote_servers('default-cluster')
            creds = remote_dao.get_remote_credentials('default-cluster')
            for server in servers:
                self._remote_client.add_remote_endpoint(server)
            self._remote_client.set_remote_credentials(creds)
        finally:
            self.db_conn_mgr().db_close(conn)
    
    def dao_factory(self) -> DAOFactory:
        return self._dao_factory
    
    def db_conn_mgr(self) -> DbConnectionManager:
        return self._db_conn_mgr
    
    def store(self) -> FileCache:
        return self._store

    def retry_interval(self):
        return self._retry_interval
    
    def io_timeout(self):
        return self._io_timeout
    
    def remote_client(self):
        return self._remote_client
    
    def get_file_metadata(self, local_id: str) -> 'FileVersionMetadata':
        conn = self.db_conn_mgr().db_connect()
        try:
            return self.dao_factory().file_dao(conn).get_file_version_metadata(local_id=local_id)
        finally:
            self.db_conn_mgr().db_close(conn)

    def update_file_remote(self, local_id: str, remote_id: Optional[str]=None, transfer_status: FileTransferStatus=FileTransferStatus.NONE, transferred_chunks: int=0):
        conn = self.db_conn_mgr().db_connect()
        try:
            self.dao_factory().file_dao(conn).update_file_remote(local_id, remote_id, transfer_status, transferred_chunks)
        finally:
            self.db_conn_mgr().db_close(conn)
    
    def update_file_download(self, local_id: str, transferred_chunks: int=0):
        conn = self.db_conn_mgr().db_connect()
        try:
            self.dao_factory().file_dao(conn).update_file_download(local_id, transferred_chunks)
        finally:
            self.db_conn_mgr().db_close(conn)
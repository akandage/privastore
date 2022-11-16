from .db.dao_factory import DAOFactory
from ..db.db_conn_mgr import DbConnectionManager
from ..error import FileServerErrorCode, WorkerError
from ..file_cache import FileCache
import logging
from queue import Queue
from ..remote_client import RemoteClient, RemoteCredentials, RemoteEndpoint
from typing import Optional
from ..worker import Worker
from ..worker_task import WorkerTask

SESSION_ID_HEADER = 'x-privastore-session-id'

class AsyncWorker(Worker):

    def __init__(self, dao_factory: DAOFactory, db_conn_mgr: DbConnectionManager, store: FileCache, worker_index: int=None, queue: Optional[Queue[WorkerTask]] = None, retry_interval: int=1, io_timeout: int=90):
        super().__init__(worker_name='upload-worker', worker_index=worker_index, queue=queue)
        self._dao_factory = dao_factory
        self._db_conn_mgr = db_conn_mgr
        self._store = store
        self._retry_interval = retry_interval
        self._io_timeout = io_timeout
        self._remote_client = RemoteClient(retry_interval=retry_interval)

        #
        # TODO: Configure this from db.
        #
        self._remote_client.add_remote_endpoint(RemoteEndpoint('localhost', '9090'))
        self._remote_client.set_remote_credentials(RemoteCredentials('psadmin', 'psadmin'))
    
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
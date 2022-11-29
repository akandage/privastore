from .db.dao_factory import DAOFactory
from ..db.db_conn_mgr import DbConnectionManager
from .db.file_dao import FileVersionMetadata
from .database import DbWrapper
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
        self._db = DbWrapper(dao_factory, db_conn_mgr)
        self._store = store
        self._retry_interval = retry_interval
        self._io_timeout = io_timeout
        self._remote_client = RemoteClient(retry_interval=retry_interval)

        conn = self.db().db_conn_mgr().db_connect()
        try:
            remote_dao = self.db().dao_factory().remote_dao(conn)
            servers = remote_dao.get_remote_servers('default-cluster')
            creds = remote_dao.get_remote_credentials('default-cluster')
            for server in servers:
                self._remote_client.add_remote_endpoint(server)
            self._remote_client.set_remote_credentials(creds)
        finally:
            self.db().db_conn_mgr().db_close(conn)
    
    def db(self) -> DbWrapper:
        return self._db
    
    def store(self) -> FileCache:
        return self._store

    def retry_interval(self):
        return self._retry_interval
    
    def io_timeout(self):
        return self._io_timeout
    
    def remote_client(self):
        return self._remote_client
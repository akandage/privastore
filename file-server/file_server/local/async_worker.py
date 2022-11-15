from .db.dao_factory import DAOFactory
from ..db.db_conn_mgr import DbConnectionManager
from ..file_cache import FileCache
from queue import Queue
from typing import Optional
from ..worker import Worker
from ..worker_task import WorkerTask

class AsyncWorker(Worker):

    def __init__(self, dao_factory: DAOFactory, db_conn_mgr: DbConnectionManager, store: FileCache, worker_index: int=None, queue: Optional[Queue[WorkerTask]] = None, num_retries: int=1, io_timeout: int=90):
        super().__init__(worker_name='upload-worker', worker_index=worker_index, queue=queue)
        self._dao_factory = dao_factory
        self._db_conn_mgr = db_conn_mgr
        self._store = store
        self._num_retries = num_retries
        self._io_timeout = io_timeout
    
    def dao_factory(self) -> DAOFactory:
        return self._dao_factory
    
    def db_conn_mgr(self) -> DbConnectionManager:
        return self._db_conn_mgr
    
    def store(self) -> FileCache:
        return self._store

    def num_retries(self):
        return self._num_retries
    
    def io_timeout(self):
        return self._io_timeout
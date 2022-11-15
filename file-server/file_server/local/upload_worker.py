from .async_worker import AsyncWorker
from .db.dao_factory import DAOFactory
from ..db.db_conn_mgr import DbConnectionManager
from ..error import FileError, UploadWorkerError
from ..file import File
from ..file_cache import FileCache
from queue import Queue
from .transfer_chunk_task import TransferChunkTask
from .transfer_file_task import TransferFileTask
from typing import Optional
from ..worker import Worker
from ..worker_task import WorkerTask

class UploadWorker(AsyncWorker):

    def __init__(self, dao_factory: DAOFactory, db_conn_mgr: DbConnectionManager, store: FileCache, worker_index: int=None, queue: Optional[Queue[WorkerTask]] = None, num_retries: int=1, io_timeout: int=90):
        super().__init__(dao_factory, db_conn_mgr, store, worker_index, queue, num_retries, io_timeout)

    def process_task(self, task: WorkerTask) -> None:
        if task.task_code() == TransferChunkTask.TASK_CODE:
            self.do_transfer_chunk(task)
        if task.task_code() == TransferFileTask.TASK_CODE:
            self.do_transfer_file(task)
        raise UploadWorkerError('Unrecognized task code [{}]'.format(task.task_code()))
    
    def do_transfer_chunk(self, task: TransferChunkTask) -> None:
        pass

    def do_transfer_file(self, task: TransferFileTask) -> None:
        pass
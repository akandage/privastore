from .async_worker import AsyncWorker
from collections import namedtuple
from .db.dao_factory import DAOFactory
from ..db.db_conn_mgr import DbConnectionManager
from ..error import FileError, FileServerErrorCode, FileUploadError, WorkerError
from ..file import File
from ..file_cache import FileCache
from .file_transfer_status import FileTransferStatus
from http import HTTPStatus
import logging
from queue import Queue
import requests
import time
from .transfer_chunk_task import TransferChunkTask
from .transfer_file_task import TransferFileTask
from typing import Optional
from ..worker import Worker
from ..worker_task import WorkerTask

class UploadWorker(AsyncWorker):

    def __init__(self, dao_factory: DAOFactory, db_conn_mgr: DbConnectionManager, store: FileCache, worker_index: int=None, task_queue: Optional[Queue[WorkerTask]] = None, completion_queue: Optional[Queue[WorkerTask]] = None, retry_interval: int=1, io_timeout: int=90):
        super().__init__(dao_factory, db_conn_mgr, store, worker_index, task_queue, completion_queue, retry_interval, io_timeout)

    def process_task(self, task: WorkerTask) -> None:
        if task.task_code() == TransferChunkTask.TASK_CODE:
            self.do_transfer_chunk(task)
        if task.task_code() == TransferFileTask.TASK_CODE:
            self.do_transfer_file(task)
        raise WorkerError('Unrecognized task code [{}]'.format(task.task_code()))

    def do_transfer_chunk(self, task: TransferChunkTask) -> None:
        pass

    def do_transfer_file(self, task: TransferFileTask) -> None:
        pass

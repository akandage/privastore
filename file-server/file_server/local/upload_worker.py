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
        remote_file_id = self.remote_client().create_file(task.file_size(), timeout=self.io_timeout())
        logging.debug('Created remote file [{}]'.format(remote_file_id))

        conn = self.db_conn_mgr().db_connect()
        try:
            self.dao_factory().file_dao(conn).update_file_remote(task.file_path(), task.file_name(), task.file_version(), remote_file_id, FileTransferStatus.TRANSFERRING_DATA)
            logging.debug('Updated remote transfer status')
        finally:
            self.db_conn_mgr().db_close(conn)
        
        file = self.store().read_file(task.local_file_id())
        logging.debug('Opened file [{}] in cache for reading'.format(task.local_file_id()))

        try:
            chunks_sent = 0
            while True:
                chunk_data = file.read_chunk()
                if len(chunk_data) == 0:
                    break
                self.remote_client().send_file_chunk(remote_file_id, chunk_data, chunks_sent+1, timeout=self.io_timeout())
                chunks_sent += 1
            logging.debug('Sent {} file chunks'.format(chunks_sent))
        finally:
            self.store().close_file(file)
            logging.debug('Closed file in cache')
        
        conn = self.db_conn_mgr().db_connect()
        try:
            self.dao_factory().file_dao(conn).update_file_transfer_status(task.file_path(), task.file_name(), task.file_version(), remote_transfer_status=FileTransferStatus.TRANSFERRED_DATA)
            logging.debug('Updated remote transfer status')
        finally:
            self.db_conn_mgr().db_close(conn)


from .async_worker import AsyncWorker
from .commit_file_task import CommitFileTask
from .db.dao_factory import DAOFactory
from ..db.db_conn_mgr import DbConnectionManager
from ..error import FileServerErrorCode, FileUploadError, WorkerError
from ..file_cache import FileCache
from .file_transfer_status import FileTransferStatus
import logging
from queue import Queue
from .transfer_chunk_task import TransferChunkTask
from .transfer_file_task import TransferFileTask
from typing import Optional
from ..worker import Worker
from ..worker_task import WorkerTask

class UploadWorker(AsyncWorker):

    def __init__(self, dao_factory: DAOFactory, db_conn_mgr: DbConnectionManager, store: FileCache, worker_index: int=None, queue_size: int=1, completion_queue: Optional[Queue[WorkerTask]] = None, retry_interval: int=1, io_timeout: int=90):
        super().__init__(dao_factory, db_conn_mgr, store, worker_index, queue_size, completion_queue, retry_interval, io_timeout)

    def process_task(self, task: WorkerTask) -> None:
        if task.task_code() == CommitFileTask.TASK_CODE:
            # TODO
            return
        if task.task_code() == TransferChunkTask.TASK_CODE:
            self.do_transfer_chunk(task)
        elif task.task_code() == TransferFileTask.TASK_CODE:
            self.do_transfer_file(task)
        else:
            raise WorkerError('Unrecognized task code [{}]'.format(task.task_code()))

    def do_transfer_chunk(self, task: TransferChunkTask) -> None:
        pass

    def do_transfer_file(self, task: TransferFileTask) -> None:
        remote_file_id = self.remote_client().create_file(task.file_size(), timeout=self.io_timeout())
        logging.debug('Created remote file [{}]'.format(remote_file_id))

        if self.is_current_task_cancelled():
            raise FileUploadError('File [{}] upload cancelled'.format(task.local_file_id()), FileServerErrorCode.REMOTE_UPLOAD_CANCELLED)

        conn = self.db_conn_mgr().db_connect()
        try:
            self.dao_factory().file_dao(conn).update_file_remote(task.local_file_id(), remote_file_id, FileTransferStatus.TRANSFERRING_DATA)
            logging.debug('Updated remote transfer status')
        finally:
            self.db_conn_mgr().db_close(conn)
        
        file = self.store().read_file(task.local_file_id())
        logging.debug('Opened file [{}] in cache for reading'.format(task.local_file_id()))

        try:
            chunks_sent = 0
            while True:
                if self.is_current_task_cancelled():
                    raise FileUploadError('File [{}] upload cancelled'.format(task.local_file_id()), FileServerErrorCode.REMOTE_UPLOAD_CANCELLED)
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
            self.dao_factory().file_dao(conn).update_file_remote(task.local_file_id(), transfer_status=FileTransferStatus.TRANSFERRED_DATA)
            logging.debug('Updated remote transfer status')
        finally:
            self.db_conn_mgr().db_close(conn)


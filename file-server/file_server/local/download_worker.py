from .async_worker import AsyncWorker
from .commit_file_task import CommitFileTask
from .db.dao_factory import DAOFactory
from ..db.db_conn_mgr import DbConnectionManager
from ..error import FileDownloadError, FileServerError, FileServerErrorCode, WorkerError
from ..file_cache import FileCache
from .file_transfer_status import FileTransferStatus
import logging
from queue import Queue
from ..remote_client import RemoteClientError
from .transfer_chunk_task import TransferChunkTask
from .transfer_file_task import TransferFileTask
from typing import Optional
from ..worker_task import WorkerTask

class DownloadWorker(AsyncWorker):

    def __init__(self, dao_factory: DAOFactory, db_conn_mgr: DbConnectionManager, store: FileCache, worker_index: int=None, queue_size: int=1, completion_queue: Optional[Queue[WorkerTask]] = None, retry_interval: int=1, io_timeout: int=90):
        super().__init__(dao_factory, db_conn_mgr, store, 'download-worker', worker_index, queue_size, completion_queue, retry_interval, io_timeout)

    def process_task(self, task: WorkerTask) -> None:
        if task.task_code() == TransferFileTask.TASK_CODE:
            self.do_download_file(task)
        else:
            raise WorkerError('Unrecognized task code [{}]'.format(task.task_code()))

    def do_download_file(self, task: TransferFileTask) -> None:
        file_metadata = self.get_file_metadata(task.local_file_id())
        remote_id = file_metadata.remote_id
        transfer_status = file_metadata.remote_transfer_status
        total_chunks = file_metadata.total_chunks

        if transfer_status != FileTransferStatus.SYNCED_DATA:
            raise FileDownloadError('Cannot download file [{}] not fully synced on remote server'.format(task.local_file_id()), FileServerErrorCode.REMOTE_DOWNLOAD_ERROR)

        logging.debug('Downloading file [{}] from remote server'.format(task.local_file_id()))

        file = self.store().append_file(task.local_file_id())
        logging.debug('Opened file for appending')

        try:
            try:
                for chunk_offset in range(total_chunks):
                    if self.is_current_task_cancelled():
                        raise FileDownloadError('File [{}] download cancelled', FileServerErrorCode.REMOTE_DOWNLOAD_CANCELLED)
                    chunk = self.remote_client().read_file_chunk(remote_id, chunk_offset, timeout=self.io_timeout())
                    file.append_chunk(chunk)
                    logging.debug('Received chunk')
                logging.debug('Received {} chunks'.format(total_chunks))
            finally:
                self.store().close_file(file)
                logging.debug('Closed file in cache')
        except FileServerError as e:
            try:
                self.store().remove_file(file)
                logging.debug('Removed file from cache')
            except:
                logging.warn('Error removing file {} from cache: {}'.format(task.local_file_id(), str(e)))
            raise e


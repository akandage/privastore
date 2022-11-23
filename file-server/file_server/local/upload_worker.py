from .async_worker import AsyncWorker
from .commit_file_task import CommitFileTask
from .db.dao_factory import DAOFactory
from ..db.db_conn_mgr import DbConnectionManager
from ..error import FileServerError, FileServerErrorCode, FileUploadError, WorkerError
from ..file_cache import FileCache
from .file_transfer_status import FileTransferStatus
import logging
from queue import Queue
from ..remote_client import RemoteClientError
from .transfer_chunk_task import TransferChunkTask
from .transfer_file_task import TransferFileTask
from typing import Optional
from ..worker_task import WorkerTask

class UploadWorker(AsyncWorker):

    def __init__(self, dao_factory: DAOFactory, db_conn_mgr: DbConnectionManager, store: FileCache, worker_index: int=None, queue_size: int=1, completion_queue: Optional[Queue[WorkerTask]] = None, retry_interval: int=1, io_timeout: int=90):
        super().__init__(dao_factory, db_conn_mgr, store, 'upload-worker', worker_index, queue_size, completion_queue, retry_interval, io_timeout)

    def process_task(self, task: WorkerTask) -> None:
        if task.task_code() == CommitFileTask.TASK_CODE:
            self.do_commit_file(task)
        elif task.task_code() == TransferChunkTask.TASK_CODE:
            self.do_transfer_chunk(task)
        elif task.task_code() == TransferFileTask.TASK_CODE:
            self.do_transfer_file(task)
        else:
            raise WorkerError('Unrecognized task code [{}]'.format(task.task_code()))

    def do_commit_file(self, task: CommitFileTask) -> None:
        file_metadata = self.get_file_metadata(task.local_file_id())
        remote_id = file_metadata.remote_id
        remote_transfer_status = file_metadata.remote_transfer_status

        if remote_transfer_status == FileTransferStatus.TRANSFERRED_DATA or remote_transfer_status == FileTransferStatus.SYNCING_DATA:
            self.update_file_remote(task.local_file_id(), transfer_status=FileTransferStatus.SYNCING_DATA)
            logging.debug('Updated file remote status to syncing data')

            try:
                self.remote_client().commit_file(remote_id, task.epoch_no(), timeout=self.io_timeout())
                logging.debug('Committed file')
            except RemoteClientError as e:
                if e.error_code() != FileServerErrorCode.FILE_IS_COMMITTED:
                    self.update_file_remote(task.local_file_id(), transfer_status=FileTransferStatus.SYNC_DATA_FAILED)
                    raise e
                else:
                    logging.debug('File [{}] already committed'.format(task.local_file_id()))

            self.update_file_remote(task.local_file_id(), transfer_status=FileTransferStatus.SYNCED_DATA)
            logging.debug('Updated file remote status to synced data')
        elif remote_transfer_status == FileTransferStatus.SYNCED_DATA:
            logging.debug('File [{}] already committed'.format(task.local_file_id()))
            return
        else:
            raise FileUploadError('Cannot commit file [{}]. Invalid status {}'.format(task.local_file_id(), remote_transfer_status))

    def do_transfer_chunk(self, task: TransferChunkTask) -> None:
        pass

    def do_transfer_file(self, task: TransferFileTask) -> None:
        file_metadata = self.get_file_metadata(task.local_file_id())
        remote_transfer_status = file_metadata.remote_transfer_status

        if remote_transfer_status == FileTransferStatus.SYNCING_DATA or remote_transfer_status == FileTransferStatus.SYNCED_DATA or remote_transfer_status == FileTransferStatus.SYNC_DATA_FAILED:
            raise FileUploadError('File [{}] has or may already be committed. Cannot transfer file data'.format(task.local_file_id()), FileServerErrorCode.FILE_IS_COMMITTED)

        remote_file_id = self.remote_client().create_file(task.file_size(), timeout=self.io_timeout())
        logging.debug('Created remote file [{}]'.format(remote_file_id))

        if self.is_current_task_cancelled():
            raise FileUploadError('File [{}] upload cancelled'.format(task.local_file_id()), FileServerErrorCode.REMOTE_UPLOAD_CANCELLED)
        
        file = self.store().read_file(task.local_file_id())
        logging.debug('Opened file [{}] in cache for reading'.format(task.local_file_id()))

        self.update_file_remote(task.local_file_id(), remote_file_id, FileTransferStatus.TRANSFERRING_DATA)
        logging.debug('Updated file remote status to transferring data')

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
        except FileServerError as e:
            self.update_file_remote(task.local_file_id(), transfer_status=FileTransferStatus.TRANSFER_DATA_FAILED)
            raise e
        finally:
            self.store().close_file(file)
            logging.debug('Closed file in cache')
        
        self.update_file_remote(task.local_file_id(), transfer_status=FileTransferStatus.TRANSFERRED_DATA)
        logging.debug('Updated file remote status to transferred data')


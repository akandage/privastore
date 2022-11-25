from .async_worker import AsyncWorker
import configparser
from .commit_file_task import CommitFileTask
from ..daemon import Daemon
from .db.dao_factory import DAOFactory
from .db.file_dao import FileVersionMetadata
from ..db.db_conn_mgr import DbConnectionManager
from .download_worker import DownloadWorker
from ..error import FileDownloadError, FileServerErrorCode, FileUploadError
from ..file import File
from ..file_cache import FileCache
from .file_task import FileTask
from .file_transfer_status import FileTransferStatus
import logging
from queue import Queue
from threading import RLock
import time
from .transfer_file_task import TransferFileTask
from typing import Union
from .upload_worker import UploadWorker
from ..util.file import config_bool
from ..worker_task import PingWorkerTask, WorkerTask

class AsyncController(Daemon):

    def __init__(self, remote_config: Union[dict, configparser.ConfigParser], dao_factory: DAOFactory, db_conn_mgr: DbConnectionManager, store: FileCache):
        super().__init__('async-controller')

        self._dao_factory = dao_factory
        self._db_conn_mgr = db_conn_mgr
        self._store = store

        self._remote_enabled = config_bool(remote_config.get('enable-remote-server', '1'))
        self._num_upload_workers = int(remote_config.get('num-upload-workers', '1'))
        self._num_download_workers = int(remote_config.get('num-download-workers', '1'))
        worker_queue_size = int(remote_config.get('worker-queue-size', '100'))
        worker_retry_interval = int(remote_config.get('worker-retry-interval', '1'))
        self._worker_io_timeout = worker_io_timeout = int(remote_config.get('worker-io-timeout', '90'))

        logging.debug('Num upload workers: [{}]'.format(self._num_upload_workers))
        logging.debug('Num download workers: [{}]'.format(self._num_download_workers))
        logging.debug('Worker queue size: [{}]'.format(worker_queue_size))
        logging.debug('Worker I/O timeout: [{}s]'.format(worker_io_timeout))
        logging.debug('Worker retry interval: [{}s]'.format(worker_retry_interval))

        self._completion_queue: Queue[WorkerTask] = Queue(worker_queue_size*2)
        self._upload_workers: list[UploadWorker]= []
        for i in range(self._num_upload_workers):
            self._upload_workers.append(UploadWorker(dao_factory, db_conn_mgr, 
                store, worker_index=i, queue_size=worker_queue_size, 
                completion_queue=self._completion_queue, 
                retry_interval=worker_retry_interval, 
                io_timeout=worker_io_timeout))
        self._download_workers: list[DownloadWorker]= []
        for i in range(self._num_download_workers):
            self._download_workers.append(DownloadWorker(dao_factory, db_conn_mgr, 
                store, worker_index=i, queue_size=worker_queue_size, 
                completion_queue=self._completion_queue, 
                retry_interval=worker_retry_interval, 
                io_timeout=worker_io_timeout))
        
        self._upload_tasks: dict[str, list[FileTask]] = dict()
        self._uploads_lock: RLock = RLock()
        self._download_tasks: dict[str, TransferFileTask] = dict()
        self._downloads_lock: RLock = RLock()
    
    def dao_factory(self):
        return self._dao_factory

    def db_conn_mgr(self):
        return self._db_conn_mgr
    
    def store(self):
        return self._store

    def remote_enabled(self):
        return self._remote_enabled

    def worker_io_timeout(self):
        return self._worker_io_timeout

    def get_file_metadata(self, local_id: str) -> 'FileVersionMetadata':
        conn = self.db_conn_mgr().db_connect()
        try:
            return self.dao_factory().file_dao(conn).get_file_version_metadata(local_id=local_id)
        finally:
            self.db_conn_mgr().db_close(conn)

    def update_file_download(self, local_id: str, transferred_chunks: int=0):
        conn = self.db_conn_mgr().db_connect()
        try:
            self.dao_factory().file_dao(conn).update_file_download(local_id, transferred_chunks)
        finally:
            self.db_conn_mgr().db_close(conn)

    def has_upload(self, local_file_id: str):
        with self._uploads_lock:
            return local_file_id in self._upload_tasks
    
    def has_download(self, local_file_id: str):
        with self._downloads_lock:
            return local_file_id in self._download_tasks

    def add_upload_task(self, local_file_id: str, task: TransferFileTask):
        with self._uploads_lock:
            if self.has_upload(local_file_id):
                raise FileUploadError('File [{}] already being uploaded'.format(local_file_id))
            self._upload_tasks[local_file_id] = [task]

    def add_commit_task(self, local_file_id: str, task: CommitFileTask):
        with self._uploads_lock:
            if not self.has_upload(local_file_id):
                raise FileUploadError('File [{}] not being uploaded'.format(local_file_id))
            self._upload_tasks[local_file_id].append(task)

    def add_download_task(self, local_file_id: str, task: TransferFileTask):
        with self._downloads_lock:
            if self.has_download(local_file_id):
                raise FileUploadError('File [{}] already being downloaded'.format(local_file_id))
            self._download_tasks[local_file_id] = task

    def get_upload_task(self, local_file_id: str) -> list[FileTask]:
        with self._uploads_lock:
            return self._upload_tasks.get(local_file_id)

    def get_download_task(self, local_file_id: str) -> TransferFileTask:
        with self._downloads_lock:
            return self._download_tasks.get(local_file_id)

    def remove_upload_task(self, local_file_id: str):
        with self._uploads_lock:
            if local_file_id in self._upload_tasks:
                self._upload_tasks.pop(local_file_id)

    def remove_download_task(self, local_file_id: str):
        with self._downloads_lock:
            if local_file_id in self._download_tasks:
                self._download_tasks.pop(local_file_id)

    def get_upload_worker(self, local_file_id: str) -> UploadWorker:
        return self._upload_workers[hash(local_file_id) % self._num_upload_workers]

    def get_download_worker(self, local_file_id: str) -> DownloadWorker:
        return self._download_workers[hash(local_file_id) % self._num_download_workers]

    def start_upload(self, local_file_id: str, file_size: int, timeout: float=None):
        task = TransferFileTask(local_file_id, file_size, is_commit=False)
        self.add_upload_task(local_file_id, task)
        self.get_upload_worker(local_file_id).send_task(task, timeout=timeout)
        return task

    def commit_upload(self, local_file_id: str, timeout: float=None):
        # TODO: Epoch handling.
        task = CommitFileTask(local_file_id, epoch_no=1)
        self.add_commit_task(local_file_id, task)
        #
        # We can send a commit task immediately as the worker will check the
        # remote transfer state of the file before actually committing.
        #
        self.get_upload_worker(local_file_id).send_task(task, timeout=timeout)
        return task

    def start_download(self, local_file_id: str, timeout: float=None):
        file_metadata = self.get_file_metadata(local_file_id)
        file_size = file_metadata.file_size
        transfer_status = file_metadata.remote_transfer_status
        if transfer_status != FileTransferStatus.SYNCED_DATA:
            raise FileDownloadError('Cannot download file [{}] not fully synced on remote server'.format(local_file_id), FileServerErrorCode.REMOTE_DOWNLOAD_ERROR)
        
        with self._downloads_lock:
            if self.has_download(local_file_id):
                logging.debug('File [{}] already being downloaded'.format(local_file_id))
                return
            self.update_file_download(local_file_id, 0)
            task = TransferFileTask(local_file_id, file_size)
            self.add_download_task(local_file_id, task)
            self.store().create_empty_file(local_file_id, file_size)
        
        self.get_download_worker(local_file_id).send_task(task, timeout=timeout)
    
    def wait_for_upload(self, local_file_id: str, timeout: float=None) -> FileTask:
        # TODO
        pass

    def cancel_upload(self, local_file_id: str) -> list[FileTask]:
        with self._uploads_lock:
            tasks = self.get_upload_task(local_file_id)
            if tasks is None:
                return
            for task in tasks:
                task.cancel()
            return tasks

    def cancel_download(self, local_file_id: str) -> TransferFileTask:
        with self._downloads_lock:
            task = self.get_download_task(local_file_id)
            if task is None:
                return
            task.cancel()
            return task

    def stop_upload(self, local_file_id: str, timeout: float=None) -> bool:
        tasks = self.cancel_upload(local_file_id)
        if tasks is not None:
            for task in tasks:
                try:
                    task.wait_processed(timeout)
                except:
                    pass
                if not task.is_processed():
                    raise FileUploadError('Timed out waiting for file [{}] to upload'.format(local_file_id))
            
            self.remove_upload_task(local_file_id)

    def stop_download(self, local_file_id: str, timeout: float=None) -> bool:
        task = self.cancel_download(local_file_id)
        if task is not None:
            try:
                task.wait_processed(timeout)
            except:
                pass
            if not task.is_processed():
                raise FileUploadError('Timed out waiting for file [{}] to download'.format(local_file_id))
            
            self.remove_download_task(local_file_id)

    def on_commit_file_completed(self, task: CommitFileTask):
        file_id = task.local_file_id()

        self.remove_upload_task(file_id)

    def on_transfer_file_completed(self, task: TransferFileTask):
        file_id = task.local_file_id()

        if self.has_download(file_id):
            self.remove_download_task(file_id)
   
    def start_async_workers(self, workers: list[AsyncWorker]):
        for worker in workers:
            worker.start()
            worker.wait_started()

    def start_upload_workers(self):
        logging.debug('Starting upload workers')
        self.start_async_workers(self._upload_workers)
        logging.debug('Started upload workers')

    def start_download_workers(self):
        logging.debug('Starting download workers')
        self.start_async_workers(self._download_workers)
        logging.debug('Started download workers')

    def stop_async_workers(self, workers: list[AsyncWorker]):
        for worker in workers:
            worker.stop()
            worker.send_task(PingWorkerTask())
        for worker in workers:
            worker.join()

    def stop_upload_workers(self):
        logging.debug('Stopping upload workers')
        self.stop_async_workers(self._upload_workers)
        self._upload_workers = []
        logging.debug('Stopped upload workers')

    def stop_download_workers(self):
        logging.debug('Stopping download workers')
        self.stop_async_workers(self._download_workers)
        self._download_workers = []
        logging.debug('Stopped download workers')
    
    def stop(self):
        if not self._stop.is_set():
            super().stop()
            self._completion_queue.put(PingWorkerTask(), block=True)

    def run(self):
        try:
            self.start_upload_workers()
        except Exception as e:
            logging.error('Failed to start upload workers: {}'.format(str(e)))
            self._stopped.set()
            self._started.set()
        
        try:
            self.start_download_workers()
        except Exception as e:
            logging.error('Failed to start upload workers: {}'.format(str(e)))
            self._stopped.set()
            self._started.set()
        
        self._started.set()
        logging.debug('Async controller started')

        while not self._stop.is_set():
            completed_task = self._completion_queue.get(block=True)

            if completed_task.error() is None:
                logging.debug('Task [{}] completed'.format(str(completed_task)))
            else:
                logging.debug('Task [{}] completed with error {}'.format(str(completed_task), str(completed_task.error())))

            if completed_task.task_code() == PingWorkerTask.TASK_CODE:
                logging.debug('Async controller pinged')
                continue
            if completed_task.task_code() == CommitFileTask.TASK_CODE:
                self.on_commit_file_completed(completed_task)
                continue
            if completed_task.task_code() == TransferFileTask.TASK_CODE:
                self.on_transfer_file_completed(completed_task)
                continue

            if self._stop.is_set():
                break

        try:
            self.stop_upload_workers()
        except Exception as e:
            logging.error('Failed to stop upload workers: {}'.format(str(e)))
        try:
            self.stop_download_workers()
        except Exception as e:
            logging.error('Failed to stop upload workers: {}'.format(str(e)))

        self._stopped.set()
        logging.debug('Async controller stopped')
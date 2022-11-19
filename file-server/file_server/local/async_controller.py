import configparser
from .commit_file_task import CommitFileTask
from ..daemon import Daemon
from .db.dao_factory import DAOFactory
from ..db.db_conn_mgr import DbConnectionManager
from ..error import FileServerErrorCode, FileUploadError
from ..file import File
from ..file_cache import FileCache
from .file_task import FileTask
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
        
        self._upload_tasks: dict[str, FileTask] = dict()
        self._uploads_lock: RLock = RLock()
    
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

    def has_upload(self, local_file_id: str):
        with self._uploads_lock:
            return local_file_id in self._upload_tasks
    
    def add_upload_task(self, local_file_id: str, task: FileTask):
        with self._uploads_lock:
            if self.has_upload(local_file_id):
                raise FileUploadError('File [{}] already being uploaded'.format(local_file_id))
            self._upload_tasks[local_file_id] = task

    def get_upload_task(self, local_file_id: str) -> FileTask:
        with self._uploads_lock:
            return self._upload_tasks.get(local_file_id)

    def remove_upload_task(self, local_file_id: str):
        with self._uploads_lock:
            if local_file_id in self._upload_tasks:
                self._upload_tasks.pop(local_file_id)

    def get_upload_worker(self, local_file_id: str) -> UploadWorker:
        return self._upload_workers[hash(local_file_id) % self._num_upload_workers]

    def start_upload(self, local_file_id: str, file_size: int, timeout: float=None):
        task = TransferFileTask(local_file_id, file_size, is_commit=False)
        self.add_upload_task(local_file_id, task)
        self.get_upload_worker(local_file_id).send_task(task, timeout=timeout)
        return task

    def commit_upload(self, local_file_id: str, timeout: float=None):
        # TODO: Epoch handling.
        task = CommitFileTask(local_file_id, epoch_no=1)
        self.get_upload_worker(local_file_id).send_task(task, timeout=timeout)

    def wait_for_upload(self, local_file_id: str, timeout: float=None) -> FileTask:
        # TODO
        pass

    def cancel_upload(self, local_file_id: str) -> FileTask:
        with self._uploads_lock:
            task = self.get_upload_task(local_file_id)
            if task is None:
                return
            task.cancel()
            return task

    def stop_upload(self, local_file_id: str, timeout: float=None) -> bool:
        task = self.cancel_upload(local_file_id)
        if task is not None:
            try:
                task.wait_processed()
            except:
                pass
            if not task.is_processed():
                raise FileUploadError('Timed out waiting for file [{}] to upload'.format(local_file_id))

    def on_commit_file_completed(self, task: CommitFileTask):
        self.remove_upload_task(task.local_file_id())

    def on_transfer_file_completed(self, task: TransferFileTask):
        pass
    
    def start_workers(self):
        self.start_upload_workers()
    
    def start_upload_workers(self):
        logging.debug('Starting upload workers')
        for worker in self._upload_workers:
            worker.start()
            worker.wait_started()
        logging.debug('Started upload workers')

    def start_download_workers(self):
        # TODO
        pass

    def stop_workers(self):
        self.stop_upload_workers()
    
    def stop_upload_workers(self):
        logging.debug('Stopping upload workers')
        for worker in self._upload_workers:
            worker.stop()
            worker.send_task(PingWorkerTask())
        for worker in self._upload_workers:
            worker.join()
        self._upload_workers = []
        logging.debug('Stopped upload workers')
    
    def stop_download_workers(self):
        # TODO
        pass

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
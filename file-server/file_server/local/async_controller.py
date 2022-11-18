import configparser
from ..daemon import Daemon
from .db.dao_factory import DAOFactory
from ..db.db_conn_mgr import DbConnectionManager
from ..error import FileError, FileServerErrorCode, FileUploadError
from ..file import File
from ..file_cache import FileCache
from .file_transfer_status import FileTransferStatus
import logging
from queue import Queue, Full
from threading import RLock
from .transfer_file_task import TransferFileTask
from typing import Optional, Union
from .upload_worker import UploadWorker
from ..util.file import config_bool, str_path
from ..worker_task import PingWorkerTask, WorkerTask

class AsyncController(Daemon):

    def __init__(self, remote_config: Union[dict, configparser.ConfigParser], dao_factory: DAOFactory, db_conn_mgr: DbConnectionManager, store: FileCache):
        super().__init__('async-controller')

        self._dao_factory = dao_factory
        self._db_conn_mgr = db_conn_mgr
        self._store = store
        self._uploads: dict[str, WorkerTask] = dict()
        self._lock = RLock()

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
        self._upload_queue: Queue[WorkerTask] = Queue(worker_queue_size)
        self._upload_workers: list[UploadWorker]= []
        for i in range(self._num_upload_workers):
            self._upload_workers.append(UploadWorker(dao_factory, db_conn_mgr, store, worker_index=i, task_queue=self._upload_queue, completion_queue=self._completion_queue, retry_interval=worker_retry_interval, io_timeout=worker_io_timeout))
    
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

    def send_to_upload_workers(self, task: WorkerTask, block=True, timeout=None) -> None:
        logging.debug('Sending task [{}] to upload workers'.format(str(task)))
        try:
            self._upload_queue.put(task, block=block, timeout=timeout)
        except Full:
            raise FileUploadError('Could send task [{}] to workers. Upload queue is full!'.format(str(task)), FileServerErrorCode.REMOTE_UPLOAD_ERROR)
        logging.debug('Sent task [{}] to upload workers'.format(str(task)))

    def start_async_upload(self, local_file_id: str, file_size: int) -> TransferFileTask:
        '''
            Start asynchronously uploading the given file to the remote server.
        '''
        with self._lock:
            if local_file_id in self._uploads:
                raise FileUploadError('File [{}] is already being async uploaded'.format(local_file_id))
            
            self._uploads[local_file_id] = task = TransferFileTask(local_file_id, file_size)
            
        logging.debug('Starting async upload of file [{}]'.format(local_file_id))
        
        try:
            self._upload_queue.put(task, block=True, timeout=self.worker_io_timeout())
        except Full:
            with self._lock:
                self._uploads.pop(local_file_id)
            raise FileUploadError('Upload worker queue is full!', FileServerErrorCode.REMOTE_UPLOAD_ERROR)
    
        logging.debug('Started async upload of file [{}]'.format(local_file_id))
        return task

    def is_async_upload(self, local_file_id: str) -> bool:
        '''
            Check if the given file is being asynchronously uploaded to the remote server.
        '''
        with self._lock:
            return local_file_id in self._uploads

    def stop_async_upload(self, local_file_id: str):
        with self._lock:
            if local_file_id in self._uploads:
                task = self._uploads[local_file_id]
                logging.debug('Cancelling async upload of file [{}]'.format(local_file_id))
                task.cancel()
            else:
                logging.debug('File [{}] not being async uploaded'.format(local_file_id))
                return
                
        try:
            task.wait_processed()
        except:
            pass

        with self._lock:
            logging.debug('Cancelled async upload of file [{}]'.format(local_file_id))
            if local_file_id in self._uploads:
                self._uploads.pop(local_file_id)

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
            self._upload_queue.put(PingWorkerTask(), block=True)
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

            if completed_task.task_code() == PingWorkerTask.TASK_CODE:
                logging.debug('Async controller pinged')
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
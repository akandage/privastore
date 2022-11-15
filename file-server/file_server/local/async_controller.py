import configparser
import logging
from typing import Union
from .upload_worker import UploadWorker

class AsyncController(object):

    def __init__(self, api_config: Union[dict, configparser.ConfigParser]):
        self._num_upload_workers = int(api_config.get('num-upload-workers', '1'))
        self._num_download_workers = int(api_config.get('num-download-workers', '1'))
        worker_queue_size = int(api_config.get('worker-queue-size', '100'))
        worker_retries = int(api_config.get('worker-retries', '1'))
        worker_io_timeout = int(api_config.get('worker-io-timeout', '90'))

        logging.debug('Num upload workers: [{}]'.format(self._num_upload_workers))
        logging.debug('Num download workers: [{}]'.format(self._num_download_workers))
        logging.debug('Worker queue size: [{}]'.format(worker_queue_size))
        logging.debug('Worker I/O timeout: [{}s]'.format(worker_io_timeout))
        logging.debug('Worker num retries: [{}]'.format(worker_retries))

        self._upload_workers: list[UploadWorker]= []
        for i in range(self._num_upload_workers):
            self._upload_workers.append(UploadWorker(worker_index=i, queue_size=worker_queue_size, num_retries=worker_retries, io_timeout=worker_io_timeout))
    
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
            worker.join()
        logging.debug('Stopped upload workers')
    
    def stop_download_workers(self):
        # TODO
        pass
from .daemon import Daemon
import logging
from queue import Queue
from threading import RLock
from typing import Optional
from .worker_task import PingWorkerTask, WorkerTask

class Worker(Daemon):
    
    def __init__(self, worker_name: str, worker_index: int=None, queue: Optional[Queue[WorkerTask]] = None, queue_size: int = 1, daemon: bool=True):
        name = f'{worker_name}-{worker_index}' if worker_index is not None else worker_name
        super().__init__(name, daemon)
        self._queue: Queue[WorkerTask] = queue if queue is not None else Queue(queue_size)
        self._curr_task: WorkerTask = None
        self._lock: RLock = RLock()
    
    def send_task(self, task: WorkerTask, block=True, timeout=None) -> None:
        logging.debug('Sending task [{}] to worker [{}]'.format(self.name(), str(task)))
        self._queue.put(task, block=block, timeout=timeout)
        logging.debug('Sent task [{}] to worker [{}]'.format(self.name(), str(task)))

    def cancel_current_task(self):
        with self._lock:
            if self._curr_task is not None:
                self._curr_task.cancel()
                logging.debug('Task [{}] cancelled'.format(str(self._curr_task)))

    def process_task(self, task: WorkerTask) -> None:
        raise Exception('Not implemented!')
    
    def stop(self):
        logging.debug('Requesting worker [{}] stop'.format(self.name()))
        if not self._stop.is_set():
            super().stop()
            self.cancel_current_task()
            
    def run(self):
        self._started.set()
        logging.debug('Worker [{}] started'.format(self.name()))
        while not self._stop.is_set():
            task = self._queue.get(block=True)
            task.set_worker(self)
            with self._lock:
                self._curr_task = task
            logging.debug('Worker [{}] received task [{}]'.format(self.name(), str(task)))
            if task.task_code() == PingWorkerTask.TASK_CODE:
                logging.debug('Worker [{}] pinged'.format(self.name()))
                task.set_processed()
                continue
            if task.is_cancelled():
                logging.debug('Worker [{}] ignoring cancelled task [{}]'.format(self.name(), str(task)))
                continue
            try:
                self.process_task(task)
                task.set_processed()
                with self._lock:
                    self._curr_task = None
            except Exception as e:
                logging.error('Worker [{}] - error while processing task: {}'.format(self.name(), str(e)))
                task.set_error(e)
        self._stopped.set()
        logging.debug('Worker [{}] stopped'.format(self.name()))
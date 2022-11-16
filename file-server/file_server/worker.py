from .daemon import Daemon
import logging
from queue import Queue
from threading import RLock
from typing import Optional
from .worker_task import PingWorkerTask, WorkerTask

class Worker(Daemon):
    
    def __init__(self, worker_name: str, worker_index: int=None, task_queue: Optional[Queue[WorkerTask]] = None, completion_queue: Optional[Queue[WorkerTask]] = None, queue_size: int = 1, daemon: bool=True):
        name = f'{worker_name}-{worker_index}' if worker_index is not None else worker_name
        super().__init__(name, daemon)
        self._task_queue: Queue[WorkerTask] = task_queue if task_queue is not None else Queue(queue_size)
        self._completion_queue = completion_queue
        self._curr_task: WorkerTask = None
        self._lock: RLock = RLock()
    
    def send_task(self, task: WorkerTask, block=True, timeout=None) -> None:
        logging.debug('Sending task [{}] to worker [{}]'.format(self.name(), str(task)))
        self._task_queue.put(task, block=block, timeout=timeout)
        logging.debug('Sent task [{}] to worker [{}]'.format(self.name(), str(task)))

    def cancel_current_task(self):
        with self._lock:
            if self._curr_task is not None:
                self._curr_task.cancel()
                logging.debug('Task [{}] cancelled'.format(str(self._curr_task)))

    def process_task(self, task: WorkerTask) -> None:
        raise Exception('Not implemented!')
    
    def completed_task(self, task: WorkerTask) -> None:
        if self._completion_queue is not None:
            self._completion_queue.put(task, block=True)

    def stop(self):
        logging.debug('Requesting worker [{}] stop'.format(self.name()))
        if not self._stop.is_set():
            super().stop()
            self.cancel_current_task()
            
    def run(self):
        self._started.set()
        logging.debug('Worker [{}] started'.format(self.name()))
        while not self._stop.is_set():
            task = self._task_queue.get(block=True)
            task.set_worker(self)

            with self._lock:
                self._curr_task = task

            logging.debug('Worker [{}] received task [{}]'.format(self.name(), str(task)))

            if task.task_code() == PingWorkerTask.TASK_CODE:
                logging.debug('Worker [{}] pinged'.format(self.name()))
                task.set_processed()
            elif task.is_cancelled():
                logging.debug('Worker [{}] ignoring cancelled task [{}]'.format(self.name(), str(task)))
                task.set_processed()
            else:
                try:
                    self.process_task(task)
                    task.set_processed()
                except Exception as e:
                    logging.error('Worker [{}] - error while processing task [{}]: {}'.format(self.name(), str(task), str(e)))
                    task.set_error(e)
            
            with self._lock:
                self._curr_task = None
            self.completed_task(task)

        self._stopped.set()
        logging.debug('Worker [{}] stopped'.format(self.name()))
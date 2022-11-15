from .daemon import Daemon
import logging
from threading import Event
from typing import Optional

class WorkerTask(object):

    def __init__(self):
        self._error: Exception = None
        self._cancelled: Event = Event()
        self._processed: Event = Event()
        self._worker: Daemon = None
    
    def task_code(self) -> int:
        '''
            Unique identifier for the task type.
        '''
        raise Exception('Not implemented!')

    def cancel(self):
        '''
            Cancel the task. Worker will either ignore the task or stop processing.
        '''
        self._cancelled.set()

    def set_error(self, error: Exception):
        '''
            For use by the worker. Notify listeners that an error occurred while
            processing this message.
        '''
        self._error = error
        self._processed.set()
    
    def set_processed(self):
        '''
            For use by the worker. Notify listeners that the task has been
            successfully processed.
        '''
        self._processed.set()

    def set_worker(self, worker: Daemon):
        '''
            For use by the worker. Set the worker that processed this task.
        '''
        self._worker = worker

    def wait_processed(self, timeout: float=None) -> None:
        '''
            Wait until this task is processed by the worker thread.
        '''
        logging.debug('[{}] - waiting for task to be processed'.format(str(self)))
        self._processed.wait(timeout)
        error = self.error()
        if error is not None:
            logging.error('[{}] - error processing task: {}'.format(str(self), str(error)))
            raise error
        logging.debug('[{}] - task processed by {}'.format(str(self), self.worker().name()))

    def worker(self) -> Optional[Daemon]:
        '''
            Return the worker that processed this message.
        '''
        return self._worker

    def error(self) -> Optional[Exception]:
        '''
            If an error occurred while processing this message, return it.
            Return None otherwise.
        '''
        return self._error

    def is_processed(self) -> bool:
        return self._processed.is_set()
    
    def is_cancelled(self) -> bool:
        return self._cancelled.is_set()
    
    def __str__(self):
        return 'TASK'

class PingWorkerTask(WorkerTask):
    '''
        Can be used as a ping/heartbeat or no-op with workers.
    '''

    TASK_CODE = 0

    def __init__(self):
        super().__init__()
    
    def task_code(self):
        return self.TASK_CODE

    def __str__(self):
        return 'PING_WORKER'
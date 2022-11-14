from .daemon import Daemon
import logging
from threading import Event
from typing import Optional

class WorkerMessage(object):

    def __init__(self):
        self._error: Exception = None
        self._processed: Event = Event()
        self._worker: Daemon = None
    
    def set_error(self, error: Exception):
        '''
            For use by the worker. Notify listeners that an error occurred while
            processing this message.
        '''
        self._error = error
        self._processed.set()
    
    def set_processed(self):
        '''
            For use by the worker. Notify listeners that the message has been
            successfully processed.
        '''
        self._processed.set()

    def set_worker(self, worker: Daemon):
        '''
            For use by the worker. Set the worker that processed this message.
        '''
        self._worker = worker

    def wait_processed(self, timeout: float=None) -> None:
        '''
            Wait until this message is processed by the worker thread.
        '''
        logging.debug('[{}] - waiting for message to be processed'.format(str(self)))
        self._processed.wait(timeout)
        error = self.error()
        if error is not None:
            logging.error('[{}] - error processing message: {}'.format(str(self), str(error)))
            raise error
        logging.debug('[{}] - message processed by {}'.format(str(self), self.worker().name()))

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
    
    def __str__(self):
        return 'WorkerMessage'
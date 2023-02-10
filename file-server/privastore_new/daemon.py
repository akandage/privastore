import logging
import time
from threading import Event, Thread

from .error import FileServerError

class Daemon(object):

    def __init__(self, name: str, daemon: bool=True):
        super().__init__()
        self._name = name
        self._daemon = daemon
        self._stop = Event()
        self._started = Event()
        self._stopped = Event()
        self._thread = None
    
    def name(self):
        return self._name

    def start(self):
        if self._thread is not None:
            raise FileServerError('Already started!')

        self._thread = Thread(name=self._name, target=self.run, daemon=self._daemon)
        self._thread.start()

    def wait_started(self, timeout: float=None):
        if self._thread is None:
            raise FileServerError('Not started!')
        if not self._started.wait(timeout):
            raise FileServerError('Timed out waiting for {} daemon to start!'.format(self._name))
        if self._stopped.is_set():
            raise FileServerError('Daemon {} did not start!'.format(self._name))

    def stop(self):
        if self._stop.is_set():
            return
        self._stop.set()
        logging.debug('Requested {} daemon stop'.format(self._name))
    
    def join(self, timeout: float=None):
        if self._thread is None:
            raise FileServerError('Not started!')

        logging.debug('Joining {} daemon'.format(self._name))
        start_t = time.time()
        if not self._stopped.wait(timeout):
            raise FileServerError('Timed out waiting for {} daemon to stop!'.format(self._name))
        if timeout is not None:
            timeout -= min(timeout, time.time()-start_t)
        self._thread.join(timeout)
        if self._thread.is_alive():
            raise FileServerError('Timed out joining {} daemon!'.format(self._name))
            
        self._thread = None

    def run(self):
        self._stopped.set()
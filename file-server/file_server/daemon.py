import logging
from threading import Event, Thread

class Daemon(object):

    def __init__(self, name, daemon=True):
        super().__init__()
        self._name = name
        self._daemon = daemon
        self._stop = False
        self._stopped = Event()
        self._thread = None
    
    def start(self):
        if self._thread is not None:
            raise Exception('Already started!')

        self._thread = Thread(name=self._name, target=self.run, daemon=self._daemon)
        self._thread.start()

    def stop(self):
        self._stop = True
        logging.debug('Requested {} daemon stop'.format(self._name))
    
    def join(self, timeout=None):
        if self._thread is None:
            raise Exception('Not started!')

        logging.debug('Joining {} daemon'.format(self._name))
        self._stopped.wait()
        self._thread.join(timeout)
        self._thread = None

    def run(self):
        self._stopped.set()
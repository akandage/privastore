import logging
from threading import RLock
import time

from ..daemon import Daemon
from .log_observer import LogObserver

class LogDaemon(Daemon):

    def __init__(self, store_config):
        super().__init__('log-daemon')
        self._observers = list()
        self._lock = RLock()
        self._poll_interval = float(store_config.get('log-poll-interval', 0.1))

    def add_observer(self, obs: LogObserver):
        with self._lock:
            self._observers.append(obs)
    
    def run(self):
        self._started.set()
        logging.debug('Log daemon started')
        now = last_poll_t = round(time.time())
        while not self._stop.wait(1):
            now = round(time.time())
            if now >= last_poll_t + self._poll_interval:
                try:
                    self._sessions.remove_expired_sessions()
                except Exception as e:
                    logging.error('Error polling log: {}'.format(str(e)))
                last_poll_t = now
        self._stopped.set()
        logging.debug('Log daemon stopped')
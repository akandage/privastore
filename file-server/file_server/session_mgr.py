import logging
from .session import Sessions
from .daemon import Daemon
import time

class SessionManager(Daemon):

    def __init__(self, daemon=True, cleanup_interval=60):
        super().__init__('session-manager', daemon)
        self.sessions = Sessions()
        self._cleanup_interval = cleanup_interval

    def run(self):
        self._started.set()
        logging.debug('Session manager started')
        now = last_cleanup_t = round(time.time())
        while not self._stop:
            time.sleep(0.1)
            now = round(time.time())
            if now >= last_cleanup_t + self._cleanup_interval:
                self.sessions.remove_expired_sessions()
                last_cleanup_t = now
        self._stopped.set()
        logging.debug('Session manager stopped')
        
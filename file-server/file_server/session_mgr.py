import logging
from .session import Sessions
from .daemon import Daemon
import time

class SessionManager(Daemon):

    def __init__(self, session_config, daemon=True):
        super().__init__('session-manager', daemon)
        self._session_expiry_time = int(session_config.get('session-expiry-time', 300))
        self._session_cleanup_interval = int(session_config.get('session-cleanup-interval', 60))
        self._sessions = Sessions()

        logging.debug('Session expiry time: [{}s]'.format(self._session_expiry_time))
        logging.debug('Session cleanup interval: [{}s]'.format(self._session_cleanup_interval))

    def sessions(self):
        return self._sessions
    
    def session_cleanup_interval(self):
        return self._session_cleanup_interval
    
    def session_expiry_time(self):
        return self._session_expiry_time
    
    def start_session(self, session_id):
        return self._sessions.start_session(session_id, self._session_expiry_time)
    
    def renew_session(self, session_id):
        self._sessions.renew_session(session_id, self._session_expiry_time)

    def run(self):
        self._started.set()
        logging.debug('Session manager started')
        now = last_cleanup_t = round(time.time())
        while not self._stop:
            time.sleep(0.1)
            now = round(time.time())
            if now >= last_cleanup_t + self._session_cleanup_interval:
                self.sessions.remove_expired_sessions()
                last_cleanup_t = now
        self._stopped.set()
        logging.debug('Session manager stopped')
        
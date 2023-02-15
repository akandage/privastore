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

    def sessions(self) -> Sessions:
        return self._sessions
    
    def session_cleanup_interval(self) -> int:
        return self._session_cleanup_interval
    
    def session_expiry_time(self) -> int:
        return self._session_expiry_time

    def get_session_user(self, session_id: str) -> str:
        return self._sessions.get_session_user(session_id)

    def start_session(self, username: str) -> str:
        return self._sessions.start_session(username, self._session_expiry_time)
    
    def renew_session(self, session_id: str):
        self._sessions.renew_session(session_id, self._session_expiry_time)

    def end_session(self, session_id: str):
        self._sessions.end_session(session_id)

    def run(self):
        self._started.set()
        logging.debug('Session manager started')
        now = last_cleanup_t = round(time.time())
        while not self._stop.wait(1):
            now = round(time.time())
            if now >= last_cleanup_t + self._session_cleanup_interval:
                try:
                    self._sessions.remove_expired_sessions()
                except Exception as e:
                    logging.error('Error removing expired sessions: {}'.format(str(e)))
                last_cleanup_t = now
        self._stopped.set()
        logging.debug('Session manager stopped')
        
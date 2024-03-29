from .error import SessionError
import logging
import time
from threading import RLock
from .util import format_datetime
import uuid

class Sessions(object):

    def __init__(self):
        super().__init__()
        self._sessions = dict()
        self._mutex = RLock()
    
    @staticmethod
    def validate_session(session_id: str):
        if not session_id.startswith('S-'):
            raise SessionError('Invalid session id [{}]'.format(session_id), SessionError.INVALID_SESSION_ID)
        try:
            uuid.UUID(session_id[2:])
        except:
            raise SessionError('Invalid session id [{}]'.format(session_id), SessionError.INVALID_SESSION_ID)

    def start_session(self, user_id: str, expiry_time: int=300) -> str:
        session_id = 'S-{}'.format(str(uuid.uuid4()))
        with self._mutex:
            session_exp = round(time.time() + expiry_time)
            self._sessions[session_id] = user_id, session_exp
            logging.debug('User [{}] session [{}] started (expires {})'.format(user_id, session_id, format_datetime(session_exp)))
        return session_id

    def is_valid_session(self, session_id: str) -> bool:
        with self._mutex:
            if session_id not in self._sessions:
                return False
            if round(time.time()) >= self._sessions[session_id][1]:
                return False
            return True

    def get_session_user(self, session_id: str) -> str:
        with self._mutex:
            if not self.is_valid_session(session_id):
                raise SessionError('Session id [{}] not found!'.format(session_id), SessionError.SESSION_NOT_FOUND)
            return self._sessions[session_id][0]

    def renew_session(self, session_id: str, expiry_time: int=300):
        self.validate_session(session_id)
        with self._mutex:
            if not self.is_valid_session(session_id):
                raise SessionError('Session id [{}] not found!'.format(session_id), SessionError.SESSION_NOT_FOUND)
            user_id, session_exp = self._sessions[session_id]
            session_exp = round(time.time() + expiry_time)
            self._sessions[session_id] = user_id, session_exp
            logging.debug('User [{}] session [{}] renewed (expires {})'.format(user_id, session_id, format_datetime(session_exp)))

    def end_session(self, session_id: str):
        self.validate_session(session_id)
        with self._mutex:
            if not self.is_valid_session(session_id):
                raise SessionError('Session id [{}] not found!'.format(session_id), SessionError.SESSION_NOT_FOUND)
            self._sessions.pop(session_id)
            logging.debug('Session [{}] ended'.format(session_id))
    
    def remove_expired_sessions(self) -> int:
        logging.debug('Removing expired sessions')
        sessions_removed = 0
        with self._mutex:
            now = round(time.time())
            sessions = list(self._sessions.items())
            for session_id, (_, session_exp) in sessions:
                if now >= session_exp:
                    self._sessions.pop(session_id)
                    sessions_removed += 1
        logging.debug('Removed {} expired sessions'.format(sessions_removed))
        return sessions_removed
        
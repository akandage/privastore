from .db.db_conn_mgr import DbConnectionManager
from .error import AuthenticationError, FileServerError, FileServerErrorCode
from .file_cache import FileCache
import logging
from .session_mgr import SessionManager
from .util.crypto import hash_user_password

'''
    File server controller.

    db_conn_mgr - database connection manager
    session_mgr - session store
    store - file store
'''
class Controller(object):

    def __init__(self, db_conn_mgr: DbConnectionManager, session_mgr: SessionManager, store: FileCache):
        self._auth_username = None
        self._auth_password_hash = None
        self._db_conn_mgr = db_conn_mgr
        self._session_mgr = session_mgr
        self._store = store
    
    def init_auth(self, auth_config):
        auth_type = auth_config.get('auth-type')
        if auth_type is not None:
            if auth_type == 'config':
                #
                # Read (single) user and password from configuration.
                #
                logging.debug('Using config-based authentication')
                auth_username = auth_config.get('username')
                auth_password_hash = auth_config.get('password-hash')
                if auth_username is None or auth_password_hash is None:
                    raise Exception('Authentication username and/or password hash not configured!')
                auth_password_hash = bytes.fromhex(auth_password_hash)
                if len(auth_password_hash) != 32:
                    raise Exception('Invalid password hash! Must be SHA-256 hash')
                self._auth_username = auth_username
                self._auth_password_hash = auth_password_hash
            elif auth_type == 'db':
                logging.debug('Using database-based authentication')
        else:
            raise Exception('No authentication type!')

    def db_conn_mgr(self) -> DbConnectionManager:
        return self._db_conn_mgr
    
    def session_mgr(self) -> SessionManager:
        return self._session_mgr

    def store(self) -> FileCache:
        return self._store

    def login_user(self, username: str, password: str) -> str:
        '''
            Attempt a login for the given user with given password.

            If successful, start a session for the user and return session-id.
            Otherwise throw AuthenticationError.
        '''
        if self._auth_username is None or self._auth_password_hash is None:
            raise FileServerError('Authentication username and/or password hash not configured!')
        if username != self._auth_username:
            raise AuthenticationError('User not found!', FileServerErrorCode.USER_NOT_FOUND)
        if hash_user_password(password) != self._auth_password_hash:
            raise AuthenticationError('Incorrect username or password', FileServerErrorCode.INCORRECT_PASSWORD)
        session_id = self.session_mgr().start_session(username)
        return session_id
    
    def heartbeat_session(self, session_id: str):
        self._session_mgr.renew_session(session_id)
    
    def logout_user(self, session_id: str):
        self._session_mgr.end_session(session_id)

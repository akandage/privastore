from ..error import FileUploadError, SessionError
from ..util.file import str_path
import logging

class Controller(object):

    '''
        Local file server controller.

        cache - file cache
        conn_pool - database connection pool
        sessions - session store
    '''
    def __init__(self, cache, sessions, dao_factory, conn_factory=None, conn_pool=None):
        super().__init__()
        self._cache = cache
        self._chunk_size = cache.file_chunk_size()
        self._conn_factory = conn_factory
        self._conn_pool = conn_pool
        self._sessions = sessions
        self._dao_factory = dao_factory
    
    def db_connect(self):
        if self._conn_factory:
            conn = self._conn_factory()
        else:
            conn = self._conn_pool.acquire(timeout=30)
            if conn is None:
                raise Exception('Database connection pool timeout')
        return conn

    def db_close(self, conn):
        if self._conn_factory:
            conn.close()
        else:
            self._conn_pool.release(conn)
    
    def login_user(self, username, password):
        logging.debug('User [{}] login attempt'.format(username))
        conn = self.db_connect()
        try:
            logging.debug('Acquired database connection')
            user_dao = self._dao_factory.user_dao(conn)
            user_dao.login_user(username, password)
            logging.debug('User [{}] logged in successfully'.format(username))
            session_id = self._sessions.start_session(username)
            return session_id
        finally:
            self.db_close(conn)
    
    def heartbeat_session(self, session_id):
        self._sessions.renew_session(session_id)

    def create_directory(self, path, directory_name, is_hidden=False):
        logging.debug('Create directory [{}]'.format(str_path(path + [directory_name])))
        conn = self.db_connect()
        try:
            logging.debug('Acquired database connection')
            dir_dao = self._dao_factory.directory_dao(conn)
            dir_dao.create_directory(path, directory_name, is_hidden)
            logging.debug('Created directory [{}]'.format(str_path(path + [directory_name])))
        finally:
            self.db_close(conn)

    def upload_file(self, path, file_name, file, file_size):
        logging.debug('Upload file [{}]'.format(str_path(path + [file_name])))
        conn = self.db_connect()
        try:
            logging.debug('Acquired database connection')
            dir_dao = self._dao_factory.directory_dao(conn)

            #
            # First, the new file is registered in the database with receiving
            # status.
            #
            dir_dao.create_file(path, file_name)

            try:
                #
                # Uploaded file data is stored in the cache and cannot be removed
                # or evicted from the cache until it has been synced to the remote
                # server.
                #
                pass
            except:
                pass

            logging.debug('Uploaded file [{}]'.format(str_path(path + [file_name])))
        finally:
            self.db_close(conn)

    def list_directory(self, path, show_hidden=False):
        logging.debug('List directory [{}]'.format(str_path(path)))
        conn = self.db_connect()
        try:
            logging.debug('Acquired database connection')
            dir_dao = self._dao_factory.directory_dao(conn)
            dir_entries = dir_dao.list_directory(path, show_hidden)
            logging.debug('Listed directory [{}]'.format(str_path(path)))
            return dir_entries
        finally:
            self.db_close(conn)
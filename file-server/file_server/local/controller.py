from ..error import FileUploadError, SessionError
from ..util.file import chunked_copy, str_mem_size, str_path
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

    def upload_file(self, path, file_name, file, file_size, file_version=1):
        logging.debug('Upload file [{}]'.format(str_path(path + [file_name])))
        conn = self.db_connect()
        try:
            logging.debug('Acquired database connection')

            dir_dao = self._dao_factory.directory_dao(conn)
            file_dao = self._dao_factory.file_dao(conn)

            #
            # First, the new file is registered in the database with receiving
            # status.
            #

            if file_version == 1:
                dir_dao.create_file(path, file_name)
            else:
                # TODO
                raise Exception('Not implemented!')

            upload_file = None

            try:
                #
                # Uploaded file data is stored in the cache and cannot be removed
                # or evicted from the cache until it has been synced to the remote
                # server.
                #
                upload_file = self._cache.open_file(file_size=file_size, mode='w')
                logging.debug('Opened file for writing in cache [{}]'.format(upload_file.file_id()))

                #
                # Read the file in chunks of the configured chunk size and append to
                # the file in the cache.
                #
                bytes_transferred = chunked_copy(file, upload_file, file_size, self._chunk_size)
                if bytes_transferred < file_size:
                    raise FileUploadError('Could not upload all file data! [{}/{}]'.format(str_mem_size(bytes_transferred), str_mem_size(file_size)))

                self._cache.close_file(upload_file, removable=False)
                logging.debug('File data uploaded [{}]'.format(str_mem_size(bytes_transferred)))

                #
                # Update the status of the file in the database to received and
                # record the local file id and file size.
                #
                file_dao.update_file_local(path, file_name, file_version, upload_file.file_id(), file_size)
                logging.debug('File metadata updated')
            except Exception as e:
                logging.error('Could not upload file: {}'.format(str(e)))

                #
                # Cleanup if any error occurs during the upload process.
                #

                try:
                    dir_dao.remove_file(path, file_name, delete=True)
                except Exception as e:
                    logging.error('Could not cleanup uploaded file: {}'.format(str(e)))

                if upload_file is not None:
                    try:
                        self._cache.remove_file(upload_file)
                        logging.debug('Removed file from cache')
                    except Exception as e:
                        logging.error('Could not cleanup uploaded file: {}'.format(str(e)))

                raise e

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
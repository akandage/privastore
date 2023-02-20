import logging
import os
from typing import Optional

from ..api.flask_daemon import FlaskDaemon
from ..daemon import Daemon
from ..db.conn_pool import DbConnectionPool
from .db.dao_factory import DAOFactory
from ..error import DatabaseError, FileServerError
from ..log.log_daemon import LogDaemon
from ..store.file_store import FileStore
from ..session_mgr import SessionManager

class LocalServer(Daemon):

    def __init__(self, config: dict):
        super().__init__('local-server')
        self._config = config

        db_config = config['db']
        db_type = db_config.get('db-type', 'sqlite')
        
        if db_type == 'sqlite':
            from ..db.conn_factory import sqlite_conn_factory
            db_path = db_config.get('sqlite-db-path', 'local_server.db')
            if not os.path.exists(db_path):
                raise DatabaseError('SQLite database [{}] not found!'.format(db_path))
            self._conn_factory = conn_factory = sqlite_conn_factory(db_path)

            from .db.sqlite.dao_factory import SqliteDAOFactory
            self._dao_factory = dao_factory = SqliteDAOFactory()
        else:
            raise FileServerError('Invalid database type [{}]'.format(db_type))
        
        conn_pool_size = int(db_config.get('connection-pool-size', 1))
        conn_pool_timeout = float(db_config.get('connection-pool-timeout', 30))
        self._conn_pool = conn_pool = DbConnectionPool(conn_factory, conn_pool_size, conn_pool_timeout)

        session_config = config['session']
        self._session_mgr = SessionManager(session_config)

        store_config = config['store']
        store_type = store_config.get('store-type', 'db')
        self._log_daemon = LogDaemon(store_config, conn_pool, dao_factory)
        
        if store_type == 'db':
            from ..store.db_file_store import DbFileStore
            self._file_store = file_store = DbFileStore(conn_pool, dao_factory)
        else:
            raise FileServerError('Invalid file store type [{}]'.format(store_type))

        from .api.flask_http import app

        http_config = config['api']
        self._httpd = FlaskDaemon(http_config, app)
    
    def conn_pool(self) -> DbConnectionPool:
        return self._conn_pool

    def dao_factory(self) -> DAOFactory:
        return self._dao_factory

    def file_store(self) -> FileStore:
        return self._file_store

    def session_mgr(self) -> SessionManager:
        return self._session_mgr

    def do_start(self):
        logging.debug('Starting local server')
        self._log_daemon.start()
        self._log_daemon.wait_started()
        self._session_mgr.start()
        self._session_mgr.wait_started()
        self._httpd.start()
        self._httpd.wait_started()
        logging.debug('Started local server')

    def do_stop(self):
        logging.debug('Stopping local server')
        self._log_daemon.stop()
        self._log_daemon.join()
        self._session_mgr.stop()
        self._session_mgr.join()
        self._httpd.stop()
        self._httpd.join()
        logging.debug('Stopping local server')

    def run(self):
        try:
            self.do_start()
        except Exception as e:
            logging.error('Error starting server: {}'.format(str(e)))
            self._started.set()
            self._stopped.set()
            return
        
        self._started.set()
        self._stop.wait()

        try:
            self.do_stop()
        except Exception as e:
            logging.warning('Error stopping server: {}'.format(str(e)))
        
        self._stopped.set()

local_server = None

def get_local_server(config: Optional[dict]=None):
    global local_server
    
    if not local_server:
        logging.debug('Initializing local server')
        local_server = LocalServer(config)
    
    return local_server

def clear_local_server():
    global local_server
    local_server = None
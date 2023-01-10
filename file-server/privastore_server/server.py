from .daemon import Daemon
from .db.db_conn_mgr import DbConnectionManager
from .file_cache import FileCache
import logging
import os
from .session_mgr import SessionManager
from threading import Event
from .util.logging import log_exception_stack

class Server(Daemon):

    def __init__(self, name, config):
        super().__init__(name)
        self._config = config
        self._api_daemon = None
        self._db_conn_mgr = None
        self._session_mgr = None
        self._store = None
    
    def config(self, section=None):
        if section:
            try:
                config = self._config[section]
            except:
                raise Exception('No {} configuration!'.format(section))
            return config
        return self._config

    def api_config(self):
        return self.config('api')

    def auth_config(self):
        return self.config('auth')

    def db_config(self):
        return self.config('db')

    def session_config(self):
        return self.config('session')

    def store_config(self):
        return self.config('store')

    def api_daemon(self):
        return self._api_daemon

    def db_conn_mgr(self):
        return self._db_conn_mgr
    
    def http_request_handler_factory(self):
        raise Exception('Not implemented!')

    def session_mgr(self):
        return self._session_mgr

    def store(self):
        return self._store

    def init_api(self) -> None:
        api_type = self.api_config().get('api-type', 'http')

        if api_type == 'http':
            logging.debug('Initializing HTTP API')
            from .api.http.http_daemon import HttpDaemon

            self._api_daemon = HttpDaemon(self.api_config(), self.http_request_handler_factory())
        else:
            raise Exception('Unsupported API type: {}'.format(api_type))

    def init_db(self):
        logging.debug('Initializing database')
        db_type = self.db_config().get('db-type', 'sqlite')

        if db_type == 'sqlite':
            from .db.sqlite.conn_factory import sqlite_conn_factory

            db_path = self.db_config().get('sqlite-db-path', 'local_server.db')
            if not os.path.exists(db_path):
                raise Exception('SQLite database [{}] does not exist!'.format(db_path))
            logging.debug('Using SQLite database: [{}]'.format(db_path))
            conn_factory = sqlite_conn_factory(db_path)
            self._db_conn_mgr = DbConnectionManager(self.db_config(), conn_factory)
        else:
            raise Exception('Unsupported database: {}'.format(db_type))

    def init_session(self):
        logging.debug('Initializing session')
        self._session_mgr = SessionManager(self.session_config())

    def init_store(self):
        logging.debug('Initializing file store')
        store_config = self.store_config()
        self._store = FileCache(store_config)

    def do_start(self):
        pass

    def do_stop(self):
        pass

    def run(self):
        try:
            self.do_start()
        except Exception as e:
            logging.error('Server failed to start: {}'.format(str(e)))
            log_exception_stack()
            self._stopped.set()
            self._started.set()
            return

        self._started.set()
        logging.info('Server started')
        self._stop.wait()
        logging.info('Server stopping')

        try:
            self.do_stop()
        except Exception as e:
            logging.error('Server failed to stop: {}'.format(str(e)))
            log_exception_stack()

        self._stopped.set()
        logging.info('Server stopped')
    
    def stop(self):
        super().stop()
        logging.debug('Server stop requested')

    
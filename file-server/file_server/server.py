from .daemon import Daemon
from .db.db_conn_mgr import DbConnectionManager
import logging
import os
from .session_mgr import SessionManager
from threading import Event

class Server(Daemon):

    def __init__(self, name, config):
        super().__init__(name)
        self._config = config
    
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

    def db_config(self):
        return self.config('db')

    def session_config(self):
        return self.config('session')

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

    def do_start(self):
        pass

    def do_stop(self):
        pass

    def run(self):
        self.do_start()
        self._started.set()
        logging.info('Server started')
        self._stop.wait()
        logging.info('Server stopping')
        self.do_stop()
        self._stopped.set()
        logging.info('Server stopped')
    
    def stop(self):
        super().stop()
        logging.debug('Server stop requested')

    
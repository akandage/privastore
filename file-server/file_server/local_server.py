import argparse
import configparser
import logging
import os
import signal
from threading import Event

from .local.controller import Controller
from .file_cache import FileCache
from .pool import Pool
from .session_mgr import SessionManager

def read_config(config_path):
    config = configparser.ConfigParser()
    config.read(config_path)
    return config

def config_logging(log_config):
    log_level = log_config.get('log-level', 'INFO')
    if log_level == 'CRITICAL':
        log_level = logging.CRITICAL
    elif log_level == 'ERROR':
        log_level = logging.ERROR
    elif log_level == 'WARN':
        log_level = logging.WARN
    elif log_level == 'INFO':
        log_level = logging.INFO
    elif log_level == 'DEBUG':
        log_level = logging.DEBUG
    
    # TODO: Configurability of log message format.
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(thread)d:%(funcName)s:%(filename)s:%(lineno)d - %(message)s', level=log_level)

class LocalServer(object):

    def __init__(self, config):
        self._config = config
        self._stop = Event()

    def run(self):
        db_config = self._config['db']
        db_type = db_config.get('db-type', 'sqlite')

        if db_type == 'sqlite':
            from .local.db.sqlite.setup import sqlite_conn_factory
            from .local.db.sqlite.dao_factory import SqliteDAOFactory

            db_path = db_config.get('db-path', 'local_server.db')
            if not os.path.exists(db_path):
                raise Exception('SQLite database [{}] does not exist!'.format(db_path))
            logging.debug('Using SQLite database: [{}]'.format(db_path))
            conn_factory = sqlite_conn_factory(db_path)
            dao_factory = SqliteDAOFactory()
        else:
            raise Exception('Unsupported database: {}'.format(db_type))
        
        logging.info('Starting PrivaStore local server ...')
        session_mgr = SessionManager(daemon=False)
        conn_pool_size = int(db_config.get('connection-pool-size', '5'))

        logging.debug('Initializing cache')
        cache_config = self._config['cache']
        cache = FileCache(cache_config)

        if conn_pool_size > 0:
            logging.debug('Initializing database connection pool with {} connections'.format(conn_pool_size))
            conn_pool = Pool(conn_factory, conn_pool_size)
            logging.debug('Initialized database connection pool')
            controller = Controller(cache, session_mgr.sessions, dao_factory, conn_pool=conn_pool)
        else:
            controller = Controller(cache, session_mgr.sessions, dao_factory, conn_factory=conn_factory)

        api_config = self._config['api']
        api_type = api_config.get('api-type', 'http')

        if api_type == 'http':
            from .local.api.http.http_daemon import HttpDaemon, http_request_handler_factory

            api_daemon = HttpDaemon(api_config, http_request_handler_factory(controller))
        else:
            raise Exception('Unsupported API type: {}'.format(api_type))

        session_mgr.start()
        api_daemon.start()
        logging.info('Server started')
        self._stop.wait()
        logging.info('Server stopping')
        api_daemon.stop()
        api_daemon.join()
        session_mgr.stop()
        session_mgr.join()
        logging.info('Server stopped')

    def setup_db(self):
        db_config = self._config['db']
        db_type = db_config.get('db-type', 'sqlite')

        if db_type == 'sqlite':
            from .local.db.sqlite.setup import setup_db

            logging.info('Setting up database ...')
            setup_db(db_config)
            logging.info('Done')
            return
        else:
            raise Exception('Unsupported database: {}'.format(db_type))

    def stop(self):
        if self._stop.is_set():
            return
        self._stop.set()
        logging.debug('Server stop requested')

def server_main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--setup-db', action='store_true')
    argparser.add_argument('--config', default='config.ini')
    args = argparser.parse_args()

    if os.path.exists(args.config):
        server_config = read_config(args.config)
        server = LocalServer(server_config)
    else:
        # TODO: Setup defaults here.
        raise Exception('config.ini not found!')

    config_logging(server_config['logging'])
    if args.setup_db:
        server.setup_db()
        return

    def handle_ctrl_c(signum, frame):
        print('Stopping...')
        server.stop()

    signal.signal(signal.SIGINT, handle_ctrl_c)
    server.run()

if __name__ == '__main__':
    server_main()

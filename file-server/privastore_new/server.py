import argparse
import configparser
import logging
import os
import signal
from threading import Event

from .api.flask_daemon import FlaskDaemon
from .db.conn_pool import DbConnectionPool
from .error import DatabaseError, FileServerError
from .local.server import get_local_server

def config_logging(log_level):
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

def setup_local_db(config):
    logging.info('Setting up local server database')

    db_type = config.get('db-type', 'sqlite')
    if db_type == 'sqlite':
        from .local.db.sqlite.setup import setup_db
        setup_db(config)
    else:
        raise FileServerError('Invalid database type [{}]'.format(db_type))
    
    logging.info('Set up local server database')

def start_local_server(config):
    server = get_local_server(config)
    server.start()
    def handle_ctrl_c(signum, frame):
        print('Stopping...')
        server.stop()
    signal.signal(signal.SIGINT, handle_ctrl_c)
    server.join()

def setup_remote_db(config):
    logging.info('Setting up remote server database')

def start_remote_server(config):
    logging.info('Starting remote server')

def server_main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--setup-db', action='store_true')
    argparser.add_argument('--local', action='store_true')
    argparser.add_argument('--remote', action='store_true')
    argparser.add_argument('--config', default='config.ini')
    args = argparser.parse_args()

    if os.path.exists(args.config):
        config = configparser.ConfigParser()
        config.read(args.config)
    else:
        raise FileServerError('Config file [{}] not found!'.format(args.config))
    
    config_logging(config['logging'].get('log-level', 'INFO'))

    if args.setup_db:
        db_config = config['db']

        if args.local:
            setup_local_db(db_config)
        elif args.remote:
            setup_remote_db(db_config)
        
        return

    if args.local:
        start_local_server(config)
    elif args.remote:
        start_remote_server(config)

if __name__ == '__main__':
    server_main()
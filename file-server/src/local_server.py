import argparse
import configparser
import logging
import os

from pool import *

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

def server_main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--setup-db', action='store_true')
    argparser.add_argument('--config', default='config.ini')
    args = argparser.parse_args()

    if os.path.exists(args.config):
        server_config = read_config(args.config)
    else:
        # TODO: Setup defaults here.
        raise Exception('config.ini not found!')

    config_logging(server_config['logging'])

    if args.setup_db:
        db_config = server_config['db']
        db_type = db_config.get('db-type', 'sqlite')

        if db_type == 'sqlite':
            from local.db.sqlite.setup import setup_db

        logging.info('Setting up database ...')
        setup_db(db_config)
        logging.info('Done')
        return
    
    logging.info('Starting PrivaStore local server ...')
    logging.info('Server started')

if __name__ == '__main__':
    server_main()

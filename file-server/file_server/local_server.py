import argparse
import logging
import os
import signal

from .local.controller import LocalServerController
from .file import File
from .file_cache import FileCache
from .file_chunk import get_encrypted_chunk_encoder, get_encrypted_chunk_decoder
from .server import Server
from .util.crypto import get_encryptor_factory, get_decryptor_factory
from .util.file import read_config
from .util.logging import config_logging

class LocalServer(Server):

    def __init__(self, config):
        super().__init__('local_server', config)
        self._controller = None
    
    def init_api(self):
        api_type = self.api_config().get('api-type', 'http')

        if api_type == 'http':
            logging.debug('Initializing HTTP API')
            from .api.http.http_daemon import HttpDaemon
            from .local.api.http.http_request_handler import HttpApiRequestHandler

            def http_request_handler_factory(request, client_address, server):
                return HttpApiRequestHandler(request, client_address, server, self._controller)

            self._api_daemon = HttpDaemon(self.api_config(), http_request_handler_factory)
        else:
            raise Exception('Unsupported API type: {}'.format(api_type))

    def init_db(self):
        super().init_db()
        db_type = db_type = self.db_config().get('db-type')
        if db_type == 'sqlite':
            from .local.db.sqlite.dao_factory import SqliteDAOFactory
            self._dao_factory = SqliteDAOFactory()

    def do_start(self):      
        logging.info('Starting PrivaStore local server ...')

        self.init_db()
        self.init_session()

        encrypt_config = self.config('encryption')
        key_alg = encrypt_config.get('key-algorithm', 'aes-128-cbc')
        key_bytes = encrypt_config.get('key-bytes')
        logging.info('Initializing file encryption (using [{}] algorithm)'.format(key_alg))

        if key_bytes is not None:
            key_bytes = bytes.fromhex(key_bytes)
            enc_factory = get_encryptor_factory(key_alg, key_bytes)
            dec_factory = get_decryptor_factory(key_alg, key_bytes)
            encrypt_chunk = get_encrypted_chunk_encoder(enc_factory)
            decrypt_chunk = get_encrypted_chunk_decoder(dec_factory)
        else:
            raise Exception('No encryption key!')

        logging.debug('Initializing cache')
        cache_config = self.config('store')
        self._cache = FileCache(cache_config)

        logging.debug('Initializing controller')
        self._controller = LocalServerController(self._cache, self._dao_factory, 
            self._db_conn_mgr, self._session_mgr, encode_chunk=encrypt_chunk,
            decode_chunk=decrypt_chunk)

        self.init_api()

        self._session_mgr.start()
        self._session_mgr.wait_started()
        self._api_daemon.start()
        self._api_daemon.wait_started()

    def do_stop(self):
        self._api_daemon.stop()
        self._api_daemon.join()
        self._session_mgr.stop()
        self._session_mgr.join()

    def setup_db(self):
        db_config = self.db_config()
        db_type = db_config.get('db-type', 'sqlite')

        if db_type == 'sqlite':
            from .local.db.sqlite.setup import setup_db

            logging.info('Setting up database ...')
            setup_db(db_config)
            logging.info('Done')
            return
        else:
            raise Exception('Unsupported database: {}'.format(db_type))

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

    log_config = server_config['logging']
    config_logging(log_config.get('log-level', 'INFO'))
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

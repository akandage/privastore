import argparse
import logging
import os
import signal

from .remote.controller import RemoteServerController
from .server import Server
from .util.file import read_config
from .util.logging import config_logging

class RemoteServer(Server):

    def __init__(self, config):
        super().__init__('remote_server', config)
        self._controller = None
    
    def controller(self):
        return self._controller

    def dao_factory(self):
        db_type = db_type = self.db_config().get('db-type')
        if db_type == 'sqlite':
            from .remote.db.sqlite.dao_factory import SqliteDAOFactory
            return SqliteDAOFactory()
        raise Exception('Unsupported database: {}'.format(db_type))

    def http_request_handler_factory(self):
        from .remote.api.http.http_request_handler import HttpApiRequestHandler

        def factory(request, client_address, server):
            return HttpApiRequestHandler(request, client_address, server, self.controller())

        return factory

    def do_start(self):      
        logging.info('Starting PrivaStore local server ...')

        self.init_db()
        self.init_session()
        self.init_store()

        logging.debug('Initializing controller')
        self._controller = RemoteServerController(self.dao_factory(), 
            self.db_conn_mgr(), self.session_mgr(), self.store())
        self._controller.init_auth(self.auth_config())

        self.init_api()

        self.session_mgr().start()
        self.session_mgr().wait_started()
        self.api_daemon().start()
        self.api_daemon().wait_started()

    def do_stop(self):
        self.api_daemon().stop()
        self.api_daemon().join()
        self.session_mgr().stop()
        self.session_mgr().join()

    def setup_db(self):
        db_config = self.db_config()
        db_type = db_config.get('db-type', 'sqlite')

        if db_type == 'sqlite':
            from .remote.db.sqlite.setup import setup_db

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
        server = RemoteServer(server_config)
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

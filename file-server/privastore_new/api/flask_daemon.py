from flask import Flask
import logging
from ..daemon import Daemon
import wsgiref.simple_server

class FlaskDaemon(Daemon):

    def __init__(self, http_config, app: Flask, daemon=True):
        super().__init__('flask-daemon', daemon)

        hostname: str = http_config.get('api-hostname', 'localhost')
        port: int = int(http_config.get('api-port', 8080))
        self._server = wsgiref.simple_server.make_server(hostname, port, app)
        self._server.timeout = 0.1

        # TODO: SSL.
    
    def run(self):
        self._started.set()
        logging.debug('Flask daemon started')
        while not self._stop.is_set():
            try:
                self._server.handle_request()
            except Exception as e:
                logging.error('Error handling HTTP request: {}'.format(str(e)))
        self._server.server_close()
        self._stopped.set()
        logging.debug('Flask daemon stopped')
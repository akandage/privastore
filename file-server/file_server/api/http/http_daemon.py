from http.server import ThreadingHTTPServer
import logging
from ...daemon import Daemon

class HttpDaemon(Daemon):

    def __init__(self, http_config, request_handler, daemon=True):
        super().__init__('http-api', daemon)

        self._hostname = hostname = http_config.get('api-hostname', 'localhost')
        self._port = port = int(http_config.get('api-port', 8080))
        self._server = ThreadingHTTPServer((hostname, port), request_handler)
        self._server.timeout = 0.1

        # TODO: SSL.
    
    def run(self):
        self._started.set()
        logging.debug('HTTP daemon started')
        while not self._stop.is_set():
            self._server.handle_request()
        self._server.server_close()
        self._stopped.set()
        logging.debug('HTTP daemon stopped')

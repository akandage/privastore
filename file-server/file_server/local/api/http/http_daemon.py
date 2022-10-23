from http.server import ThreadingHTTPServer
import logging
from ....daemon import Daemon
from .http_request_handler import HttpRequestHandler

def http_request_handler_factory(controller):
    return lambda request, client_address, server: HttpRequestHandler(request, client_address, server, controller)

class HttpDaemon(Daemon):

    def __init__(self, http_config, request_handler):
        super().__init__('http-api')

        self._hostname = hostname = http_config.get('api-hostname', 'localhost')
        self._port = port = int(http_config.get('api-port', 8080))
        self._server = ThreadingHTTPServer((hostname, port), request_handler)
        self._server.timeout = 0.1

        # TODO: SSL.
    
    def run(self):
        self._started.set()
        logging.debug('HTTP daemon started')
        while not self._stop:
            self._server.handle_request()
        self._stopped.set()
        logging.debug('HTTP daemon stopped')

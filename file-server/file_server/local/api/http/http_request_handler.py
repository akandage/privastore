from curses.ascii import HT
import base64
from ...error import AuthenticationError
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
import logging

class HttpRequestHandler(BaseHTTPRequestHandler):

    def __init__(self, request, client_address, server, controller):
        self._controller = controller
        super().__init__(request, client_address, server)
        
    def do_GET(self):
        path = self.path
        logging.error('Invalid path: [{}]'.format(path))
        self.send_error(HTTPStatus.NOT_FOUND)
        

    def do_POST(self):
        path = self.path
        if path == '/1/login':
            self.handle_login_user()
        else:
            logging.error('Invalid path: [{}]'.format(path))
            self.send_error(HTTPStatus.NOT_FOUND)
            
    def handle_login_user(self):
        logging.debug('Login request')
        auth_header = self.headers.get('Authorization')

        if auth_header is None or not auth_header.startswith('Basic '):
            logging.error('Missing or invalid Authorization header')
            self.send_error(HTTPStatus.BAD_REQUEST)
            return
            
        try:
            auth_header = base64.b64decode(auth_header[6:]).decode('utf-8')
            username, password = auth_header.split(':')
        except:
            logging.error('Invalid Authorization header value')
            self.send_error(HTTPStatus.BAD_REQUEST)
            return

        try:
            self._controller.login_user(username, password)
        except AuthenticationError as e:
            logging.error('Authentication error: {}'.format(str(e)))
            self.send_error(HTTPStatus.UNAUTHORIZED)
            return
        except Exception as e:
            logging.error('Internal error: {}'.format(str(e)))
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR)
            return

        self.send_response(HTTPStatus.OK)
        self.end_headers()
import base64
from ....error import AuthenticationError, DirectoryError, SessionError
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
import logging
import urllib.parse

DIRECTORY_PATH = '/1/directory'
DIRECTORY_PATH_LEN = len(DIRECTORY_PATH)
HEARTBEAT_PATH = '/1/heartbeat'
LOGIN_PATH = '/1/login'
SESSION_ID_HEADER = 'x-privastore-session-id'

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
        if path == LOGIN_PATH:
            self.handle_login_user()
        else:
            logging.error('Invalid path: [{}]'.format(path))
            self.send_error(HTTPStatus.NOT_FOUND)

    def do_PUT(self):
        path = self.path
        if path.startswith(DIRECTORY_PATH):
            self.handle_create_directory()
        elif path == HEARTBEAT_PATH:
            self.handle_heartbeat_session()
        else:
            logging.error('Invalid path: [{}]'.format(path))
            self.send_error(HTTPStatus.NOT_FOUND)

    def get_session_id(self):
        session_id = self.headers.get(SESSION_ID_HEADER)

        if session_id is None:
            logging.error('Missing {} header'.format(SESSION_ID_HEADER))
            self.send_error(HTTPStatus.BAD_REQUEST)
            return
        
        return session_id

    def heartbeat_session(self, session_id):
        try:
            self._controller.heartbeat_session(session_id)
            return True
        except SessionError as e:
            logging.error(str(e))
            msg = str(e).lower()
            if 'not found' in msg:
                self.send_error(HTTPStatus.UNAUTHORIZED)
            else:
                self.send_error(HTTPStatus.BAD_REQUEST)
            return False
        except Exception as e:
            self.handle_internal_error(e)
            return False

    def parse_directory_path(self, url_path):
        url_path = url_path[DIRECTORY_PATH_LEN+1:].split('/')
        return list(map(urllib.parse.unquote, url_path))

    def handle_directory_error(self, e):
        logging.error('Directory error: {}'.format(str(e)))
        msg = str(e).lower()
        if 'invalid path' in msg:
            self.send_error(HTTPStatus.NOT_FOUND)
        if 'exists in path' in msg:
            self.send_error(HTTPStatus.CONFLICT)
        else:
            self.send_error(HTTPStatus.BAD_REQUEST)

    def handle_internal_error(self, e):
        logging.error('Internal error: {}'.format(str(e)))
        self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR)

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
            session_id = self._controller.login_user(username, password)
        except AuthenticationError as e:
            logging.error('Authentication error: {}'.format(str(e)))
            self.send_error(HTTPStatus.UNAUTHORIZED)
            return
        except Exception as e:
            self.handle_internal_error(e)
            return

        self.send_response(HTTPStatus.OK)
        self.send_header(SESSION_ID_HEADER, session_id)
        self.end_headers()
    
    def handle_heartbeat_session(self):
        logging.debug('Heartbeat request')

        session_id = self.get_session_id()
        if session_id is None:
            return
        
        if not self.heartbeat_session(session_id):
            return

        self.send_response(HTTPStatus.OK)
        self.end_headers()
    
    def handle_create_directory(self):
        logging.debug('Create directory request')

        session_id = self.get_session_id()
        if session_id is None:
            return
        
        if not self.heartbeat_session(session_id):
            return

        try:
            path = self.parse_directory_path(self.path)
            directory_name = path.pop()
        except:
            self.send_error(HTTPStatus.BAD_REQUEST)
            return
        
        try:
            self._controller.create_directory(path, directory_name)
        except DirectoryError as e:
            self.handle_directory_error(e)
            return
        except Exception as e:
            self.handle_internal_error(e)
            pass

        self.send_response(HTTPStatus.OK)
        self.end_headers()
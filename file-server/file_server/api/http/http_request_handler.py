import base64
from ...controller import Controller
from ...error import AuthenticationError, FileError, FileServerError, FileServerErrorCode, SessionError
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
import logging
import urllib
import urllib.parse
from ...util.sock import SocketWrapper
from typing import Optional

AUTHORIZATION_HEADER = 'Authorization'
CONNECTION_HEADER = 'Connection'
CONNECTION_CLOSE = 'close'
CONTENT_TYPE_HEADER = 'Content-Type'
CONTENT_TYPE_JSON = 'application/json'
CONTENT_LENGTH_HEADER = 'Content-Length'
SESSION_ID_HEADER = 'x-privastore-session-id'

HEARTBEAT_PATH = '/1/heartbeat'
LOGIN_PATH = '/1/login'
LOGOUT_PATH = '/1/logout'

class BaseHttpApiRequestHandler(BaseHTTPRequestHandler):

    def __init__(self, request, client_address, server, controller: Controller):
        self._controller = controller
        self.auth_username: Optional[str] = None
        self.auth_password: Optional[str] = None
        self.content_len: Optional[int] = None
        self.url_path: Optional[str] = None
        self.url_query: Optional[dict[str, list[str]]] = None
        super().__init__(request, client_address, server)
    
    def controller(self) -> Controller:
        return self._controller

    def do_GET(self):
        if not self.parse_path():
            return

        logging.error('Invalid path: [{}]'.format(self.url_path))
        self.send_error_response(HTTPStatus.NOT_FOUND)
        
    def do_POST(self):
        if not self.parse_path():
            return

        if self.url_path == LOGIN_PATH:
            self.handle_login_user()
        elif self.url_path == LOGOUT_PATH:
            self.handle_logout_user()
        else:
            logging.error('Invalid path: [{}]'.format(self.url_path))
            self.send_error_response(HTTPStatus.NOT_FOUND)

    def do_PUT(self):
        if not self.parse_path():
            return

        elif self.url_path == HEARTBEAT_PATH:
            self.handle_heartbeat_session()
        else:
            logging.error('Invalid path: [{}]'.format(self.url_path))
            self.send_error_response(HTTPStatus.NOT_FOUND)

    def handle_file_error(self, e: FileError):
        logging.error('File error: {}'.format(str(e)))
        if e.error_code() == FileServerErrorCode.FILE_NOT_FOUND:
            self.send_error_response(HTTPStatus.NOT_FOUND, e)
        elif e.error_code() == FileServerErrorCode.FILE_EXISTS:
            self.send_error_response(HTTPStatus.CONFLICT, e)
        elif e.error_code() == FileServerErrorCode.FILE_IS_DIRECTORY:
            self.send_error_response(HTTPStatus.CONFLICT, e)
        else:
            self.send_error_response(HTTPStatus.BAD_REQUEST, e)

    def handle_internal_error(self, e: Exception):
        logging.error('Internal error: {}'.format(str(e)))
        self.send_error_response(HTTPStatus.INTERNAL_SERVER_ERROR, e)

    def handle_session_error(self, e: SessionError):
            logging.error('Session error: {}'.format(str(e)))
            if e.error_code() == FileServerErrorCode.SESSION_NOT_FOUND:
                self.send_error_response(HTTPStatus.UNAUTHORIZED, e)
            else:
                self.send_error_response(HTTPStatus.BAD_REQUEST, e)

    def parse_path(self):
        if self.url_path is not None and self.url_query is not None:
            return True

        try:
            url_parsed = urllib.parse.urlparse(self.path)

            # Only use the path and query-string for APIs.
            if url_parsed.fragment != '' or url_parsed.params != '':
                raise Exception()
            url_path = url_parsed.path
            if url_path == '':
                raise Exception()
            url_query = url_parsed.query
            if url_query != '':
                url_query = urllib.parse.parse_qs(url_query)
            else:
                url_query = dict()

            self.url_path = url_path
            self.url_query = url_query
            return True
        except:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid URL path')
            return False

    def parse_content_length(self):
        if self.content_len is not None:
            return True

        content_len = self.headers.get(CONTENT_LENGTH_HEADER)

        if content_len is None:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Missing {} header'.format(CONTENT_LENGTH_HEADER))
            return False

        try:
            content_len = int(content_len)
            if content_len < 0:
                raise Exception()
            self.content_len = content_len
            return True
        except:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid {} header value'.format(CONTENT_LENGTH_HEADER))
            return False

    def parse_basic_auth(self):
        if self.auth_username is not None and self.auth_password is not None:
            return True

        auth_header = self.headers.get(AUTHORIZATION_HEADER)

        if auth_header is None or not auth_header.startswith('Basic '):
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Missing or invalid {} header'.format(AUTHORIZATION_HEADER))
            return False
            
        try:
            auth_header = base64.b64decode(auth_header[6:]).decode('utf-8')
            username, password = auth_header.split(':')
            self.auth_username = username
            self.auth_password = password
            return True
        except:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid {} header value'.format(AUTHORIZATION_HEADER))
            return False

    def wrap_sockets(self):
        '''
            Wrap the read end of the connection so we can track how much of the
            body has been read so far.

        '''
        self.rfile = SocketWrapper(self.rfile)

    def read_body(self):
        content_len = 0

        try:
            content_len = int(self.headers.get(CONTENT_LENGTH_HEADER))
            logging.debug('Body length={}'.format(content_len))
        except:
            pass

        # Try not to read past the end of the request body.
        if isinstance(self.rfile, SocketWrapper):
            bytes_read = self.rfile.bytes_read()
            content_len = content_len - bytes_read
            logging.debug('Already read={}'.format(bytes_read))

        if content_len > 0:
            try:
                body_read = 0
                while body_read < content_len:
                    data = self.rfile.read(min(content_len - body_read, 4096))
                    data_len = len(data)
                    if data_len == 0:
                        raise Exception('Unexpected EOF')
                    body_read += data_len
                    logging.debug('Read {} bytes'.format(body_read))
            except Exception as e:
                logging.warn('Could not read HTTP request body: {}'.format(str(e)))

    def send_error_response(self, code: int, error: Exception=None):
        # May not have read the complete body if provided.
        self.read_body()

        if error is not None:
            error_msg = str(error)
            if isinstance(error, FileServerError):
                error_code = error.error_code()
                logging.error('Error [{}] - {}'.format(error_code, error_msg))
            else:
                error_code = str(code)
                logging.error('Error [HTTP {}] - {}'.format(error_code, error_msg))
                
            body = '{{"error":"{}", "msg":"{}"}}'.format(error_code, error_msg).encode('utf-8')
        else:
            body = b''

        self.send_response(code)
        self.send_header(CONTENT_LENGTH_HEADER, len(body))
        self.send_header(CONNECTION_HEADER, CONNECTION_CLOSE)
        self.end_headers()
        if body:
            self.wfile.write(body)
    
    def get_session_id(self) -> str:
        session_id = self.headers.get(SESSION_ID_HEADER)

        if session_id is None:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Missing {} header'.format(SESSION_ID_HEADER))
            return
        
        return session_id

    def heartbeat_session(self, session_id):
        try:
            self.controller().heartbeat_session(session_id)
            return True
        except SessionError as e:
            self.handle_session_error(e)
            return False
        except Exception as e:
            self.handle_internal_error(e)
            return False
    
    def handle_login_user(self):
        '''

            Handle the user login API.
            Username and password provided via HTTP basic authentication.

            Method: POST
            Path: /1/login
            Request Headers:
                Authorization: Basic <base64-encoded "username:password">
            
            Response Headers:
                x-privastore-session-id: <session-id>

        '''
        logging.debug('Login request')
        self.wrap_sockets()
        self.read_body()

        if not self.parse_basic_auth():
            return

        try:
            session_id = self.controller().login_user(self.auth_username, self.auth_password)
        except AuthenticationError as e:
            self.send_error_response(HTTPStatus.UNAUTHORIZED, e)
            return
        except Exception as e:
            self.handle_internal_error(e)
            return

        self.send_response(HTTPStatus.OK)
        self.send_header(CONTENT_LENGTH_HEADER, '0')
        self.send_header(CONNECTION_HEADER, CONNECTION_CLOSE)
        self.send_header(SESSION_ID_HEADER, session_id)
        self.end_headers()
    
    def handle_heartbeat_session(self):
        '''

            Handle the heartbeat session API.
            Send a heartbeat to renew the session.

            Method: PUT
            Path: /1/heartbeat
            Request Headers:
                x-privastore-session-id: <session-id>

        '''
        logging.debug('Heartbeat request')
        self.wrap_sockets()
        self.read_body()

        session_id = self.get_session_id()
        if session_id is None:
            return
        
        if not self.heartbeat_session(session_id):
            return

        self.send_response(HTTPStatus.OK)
        self.send_header(CONTENT_LENGTH_HEADER, '0')
        self.send_header(CONNECTION_HEADER, CONNECTION_CLOSE)
        self.end_headers()
    
    def handle_logout_user(self):
        '''

            Handle logout user API.
            End the user session.

            Method: PUT
            Path: /1/logout
            Request Headers:
                x-privastore-session-id: <session-id>
            
        '''
        logging.debug('Logout request')
        self.wrap_sockets()
        self.read_body()

        session_id = self.get_session_id()
        if session_id is None:
            return

        try:
            self.controller().logout_user(session_id)
        except SessionError as e:
            self.handle_session_error(e)
            return
        except Exception as e:
            self.handle_internal_error(e)
            return

        self.send_response(HTTPStatus.OK)
        self.send_header(CONTENT_LENGTH_HEADER, '0')
        self.send_header(CONNECTION_HEADER, CONNECTION_CLOSE)
        self.end_headers()

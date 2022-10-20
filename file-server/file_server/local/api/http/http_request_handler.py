import base64
from ....error import AuthenticationError, DirectoryError, FileError, SessionError
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
import json
import logging
import urllib.parse
from ....util.sock import SocketWrapper

DIRECTORY_PATH = '/1/directory'
DIRECTORY_PATH_LEN = len(DIRECTORY_PATH)
HEARTBEAT_PATH = '/1/heartbeat'
LOGIN_PATH = '/1/login'
UPLOAD_PATH = '/1/upload'
UPLOAD_PATH_LEN = len(UPLOAD_PATH)
DOWNLOAD_PATH = '/1/download'
DOWNLOAD_PATH_LEN = len(DOWNLOAD_PATH)
AUTHORIZATION_HEADER = 'Authorization'
CONNECTION_HEADER = 'Connection'
CONNECTION_CLOSE = 'close'
CONTENT_TYPE_HEADER = 'Content-Type'
CONTENT_TYPE_JSON = 'application/json'
CONTENT_LENGTH_HEADER = 'Content-Length'
SESSION_ID_HEADER = 'x-privastore-session-id'

class HttpRequestHandler(BaseHTTPRequestHandler):

    def __init__(self, request, client_address, server, controller):
        self._controller = controller
        super().__init__(request, client_address, server)
        
    def do_GET(self):
        if not self.parse_path():
            return

        if self.url_path.startswith(DIRECTORY_PATH):
            self.handle_list_directory()
        else:
            logging.error('Invalid path: [{}]'.format(self.url_path))
            self.send_error_response(HTTPStatus.NOT_FOUND)
        
    def do_POST(self):
        if not self.parse_path():
            return

        if self.url_path == LOGIN_PATH:
            self.handle_login_user()
        elif self.url_path.startswith(UPLOAD_PATH):
            self.handle_upload_file()
        elif self.url_path.startswith(DOWNLOAD_PATH):
            self.handle_download_file()
        else:
            logging.error('Invalid path: [{}]'.format(self.url_path))
            self.send_error_response(HTTPStatus.NOT_FOUND)

    def do_PUT(self):
        if not self.parse_path():
            return

        if self.url_path.startswith(DIRECTORY_PATH):
            self.handle_create_directory()
        elif self.url_path == HEARTBEAT_PATH:
            self.handle_heartbeat_session()
        else:
            logging.error('Invalid path: [{}]'.format(self.url_path))
            self.send_error_response(HTTPStatus.NOT_FOUND)

    def wrap_socket(self):
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
        try:
            bytes_read = self.rfile.bytes_read()
            content_len = content_len - bytes_read
            logging.debug('Already read={}'.format(bytes_read))
        except:
            pass

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

    def send_error_response(self, code, error_msg=''):
        # May not have read the complete body if provided.
        self.read_body()

        if error_msg:
            logging.error(error_msg)
            body = '{{error:"{}"}}'.format(error_msg).encode('utf-8')
        else:
            body = b''

        self.send_response(code)
        self.send_header(CONTENT_LENGTH_HEADER, len(body))
        self.send_header(CONNECTION_HEADER, CONNECTION_CLOSE)
        self.end_headers()
        if body:
            self.wfile.write(body)
        # self.wfile.flush()

    def get_session_id(self):
        session_id = self.headers.get(SESSION_ID_HEADER)

        if session_id is None:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Missing {} header'.format(SESSION_ID_HEADER))
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
                self.send_error_response(HTTPStatus.UNAUTHORIZED, str(e))
            else:
                self.send_error_response(HTTPStatus.BAD_REQUEST, str(e))
            return False
        except Exception as e:
            self.handle_internal_error(e)
            return False

    def parse_content_length(self):
        content_len =  self.headers.get(CONTENT_LENGTH_HEADER)

        if content_len is None:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Missing {} header'.format(CONTENT_LENGTH_HEADER))
            return

        try:
            content_len = int(content_len)
            if content_len < 0:
                raise Exception()
        except:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid {} header value'.format(CONTENT_LENGTH_HEADER))
            return
        
        return content_len

    def parse_directory_path(self, path):
        if len(path) == 0:
            raise Exception('Path is empty')
        if path[0] != '/':
            raise Exception('Invalid path')

        if len(path) > 1:
            path = path[1:].split('/')
            if '' in path:
                raise Exception('Invalid path. One or more path components is empty')
            return list(map(urllib.parse.unquote, path))
        return []

    def parse_path(self):
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

    def handle_directory_error(self, e):
        logging.error('Directory error: {}'.format(str(e)))
        msg = str(e).lower()
        if 'invalid path' in msg:
            self.send_error_response(HTTPStatus.NOT_FOUND, str(e))
        elif 'exists in path' in msg:
            self.send_error_response(HTTPStatus.CONFLICT, str(e))
        else:
            self.send_error_response(HTTPStatus.BAD_REQUEST, str(e))

    def handle_file_error(self, e):
        logging.error('File error: {}'.format(str(e)))
        msg = str(e).lower()
        if 'not found' in msg:
            self.send_error_response(HTTPStatus.NOT_FOUND, str(e))
        elif 'exists in path' in msg or 'is a directory' in msg:
            self.send_error_response(HTTPStatus.CONFLICT, str(e))
        else:
            self.send_error_response(HTTPStatus.BAD_REQUEST, str(e))

    def handle_internal_error(self, e):
        logging.error('Internal error: {}'.format(str(e)))
        self.send_error_response(HTTPStatus.INTERNAL_SERVER_ERROR, str(e))

    def handle_login_user(self):
        logging.debug('Login request')
        self.wrap_socket()
        self.read_body()
        auth_header = self.headers.get(AUTHORIZATION_HEADER)

        if auth_header is None or not auth_header.startswith('Basic '):
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Missing or invalid {} header'.format(AUTHORIZATION_HEADER))
            return
            
        try:
            auth_header = base64.b64decode(auth_header[6:]).decode('utf-8')
            username, password = auth_header.split(':')
        except:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid {} header value'.format(AUTHORIZATION_HEADER))
            return

        try:
            session_id = self._controller.login_user(username, password)
        except AuthenticationError as e:
            self.send_error_response(HTTPStatus.UNAUTHORIZED, str(e))
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
        logging.debug('Heartbeat request')
        self.wrap_socket()
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
    
    def handle_create_directory(self):
        logging.debug('Create directory request')
        self.wrap_socket()
        self.read_body()

        session_id = self.get_session_id()
        if session_id is None:
            return
        
        if not self.heartbeat_session(session_id):
            return

        try:
            path = self.parse_directory_path(self.url_path[DIRECTORY_PATH_LEN:])
            directory_name = path.pop()
        except:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid directory path')
            return
        
        try:
            self._controller.create_directory(path, directory_name)
        except DirectoryError as e:
            self.handle_directory_error(e)
            return
        except Exception as e:
            self.handle_internal_error(e)
            return

        self.send_response(HTTPStatus.OK)
        self.send_header(CONTENT_LENGTH_HEADER, '0')
        self.send_header(CONNECTION_HEADER, CONNECTION_CLOSE)
        self.end_headers()
    
    def handle_list_directory(self):
        logging.debug('List directory request')
        self.wrap_socket()
        self.read_body()
        session_id = self.get_session_id()
        if session_id is None:
            return
        
        if not self.heartbeat_session(session_id):
            return

        try:
            path = self.parse_directory_path(self.url_path[DIRECTORY_PATH_LEN:])
        except:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid directory path')
            return
        
        try:
            dir_entries = self._controller.list_directory(path)
            dir_entries = json.dumps(dir_entries).encode('utf-8')
        except DirectoryError as e:
            self.handle_directory_error(e)
            return
        except Exception as e:
            self.handle_internal_error(e)
            return

        self.send_response(HTTPStatus.OK)
        self.send_header(CONTENT_TYPE_HEADER, CONTENT_TYPE_JSON)
        self.send_header(CONTENT_LENGTH_HEADER, str(len(dir_entries)))
        self.send_header(CONNECTION_HEADER, CONNECTION_CLOSE)
        self.end_headers()
        self.wfile.write(dir_entries)
    
    def handle_upload_file(self):
        logging.debug('Upload file')
        self.wrap_socket()
        session_id = self.get_session_id()
        if session_id is None:
            return
        
        if not self.heartbeat_session(session_id):
            return

        try:
            path = self.parse_directory_path(self.url_path[UPLOAD_PATH_LEN:])
            file_name = path.pop()
        except:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid directory path or filename')
            return
        
        file_size = self.parse_content_length()
        if file_size is None:
            return
        elif file_size == 0:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Missing file data')
            return

        try:
            self._controller.upload_file(path, file_name, self.rfile, file_size)
        except DirectoryError as e:
            self.handle_directory_error(e)
            return
        except FileError as e:
            self.handle_file_error(e)
            return
        except Exception as e:
            self.handle_internal_error(e)
            return

        self.send_response(HTTPStatus.OK)
        self.send_header(CONTENT_LENGTH_HEADER, '0')
        self.send_header(CONNECTION_HEADER, CONNECTION_CLOSE)
        self.end_headers()
    
    def handle_download_file(self):
        logging.debug('Download file')
        self.wrap_socket()
        session_id = self.get_session_id()
        if session_id is None:
            return
        
        if not self.heartbeat_session(session_id):
            return

        try:
            path = self.parse_directory_path(self.url_path[DOWNLOAD_PATH_LEN:])
            file_name = path.pop()
        except:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid directory path or filename')
            return
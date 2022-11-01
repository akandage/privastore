from ....error import AuthenticationError, DirectoryError, FileError, SessionError
from http import HTTPStatus
from ....api.http.http_request_handler import BaseHttpApiRequestHandler
from ....api.http.http_request_handler import CONNECTION_HEADER, CONNECTION_CLOSE, CONTENT_TYPE_HEADER, CONTENT_TYPE_JSON, CONTENT_LENGTH_HEADER
import json
import logging
import urllib.parse

DIRECTORY_PATH = '/1/directory'
DIRECTORY_PATH_LEN = len(DIRECTORY_PATH)
HEARTBEAT_PATH = '/1/heartbeat'
LOGIN_PATH = '/1/login'
UPLOAD_PATH = '/1/upload'
UPLOAD_PATH_LEN = len(UPLOAD_PATH)
DOWNLOAD_PATH = '/1/download'
DOWNLOAD_PATH_LEN = len(DOWNLOAD_PATH)
SESSION_ID_HEADER = 'x-privastore-session-id'

class HttpApiRequestHandler(BaseHttpApiRequestHandler):

    def __init__(self, request, client_address, server, controller):
        self._controller = controller
        super().__init__(request, client_address, server)
        
    def do_GET(self):
        if not self.parse_path():
            return

        if self.url_path.startswith(DIRECTORY_PATH):
            self.handle_list_directory()
        elif self.url_path.startswith(DOWNLOAD_PATH):
            self.handle_download_file()
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
        self.wrap_sockets()
        self.read_body()

        if not self.parse_basic_auth():
            return

        try:
            session_id = self._controller.login_user(self.auth_username, self.auth_password)
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
    
    def handle_create_directory(self):
        logging.debug('Create directory request')
        self.wrap_sockets()
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
        self.wrap_sockets()
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
        self.wrap_sockets()
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
        
        if not self.parse_content_length():
            return
        elif self.content_len == 0:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Missing file data')
            return

        try:
            self._controller.upload_file(path, file_name, self.rfile, self.content_len)
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
        self.wrap_sockets()
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
        
        try:
            file_version = self.url_query.get('version')
            if file_version is not None:
                file_version = int(file_version)

                if file_version < 1:
                    self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid file version')
                    return
        except:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid file version')
            return

        try:
            timeout = self.url_query.get('timeout')
            if timeout is not None:
                timeout = int(timeout)

                if timeout <= 0:
                    self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid timeout value')
                    return
        except:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid timeout value')
            return
        
        try:
            self._controller.download_file(path, file_name, self.wfile, file_version, timeout=timeout, api_callback=self.send_download_file_headers)
        except DirectoryError as e:
            self.handle_directory_error(e)
        except FileError as e:
            self.handle_file_error(e)
        except Exception as e:
            self.handle_internal_error(e)
    
    def send_download_file_headers(self, file_id, file_type, file_size):
        logging.debug('Send download file headers [{}] [{}] [{}]'.format(file_id, file_type.mime_type, file_size))
        self.send_response(HTTPStatus.OK)
        self.send_header(CONTENT_TYPE_HEADER, file_type.mime_type)
        self.send_header(CONTENT_LENGTH_HEADER, str(file_size))
        self.end_headers()
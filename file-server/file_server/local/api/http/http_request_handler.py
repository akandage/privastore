from ...controller import LocalServerController
from ....error import DirectoryError, FileError, FileServerErrorCode
from http import HTTPStatus
from ....api.http.http_request_handler import BaseHttpApiRequestHandler
from ....api.http.http_request_handler import CONNECTION_HEADER, CONNECTION_CLOSE, CONTENT_TYPE_HEADER, CONTENT_TYPE_JSON, CONTENT_LENGTH_HEADER
import json
from ....key import Key
import logging
import urllib.parse
from ....util.logging import log_exception_stack

DIRECTORY_PATH = '/1/directory'
DIRECTORY_PATH_LEN = len(DIRECTORY_PATH)
FILE_PATH = '/1/file'
FILE_PATH_LEN = len(FILE_PATH)
UPLOAD_PATH = '/1/upload'
UPLOAD_PATH_LEN = len(UPLOAD_PATH)
DOWNLOAD_PATH = '/1/download'
DOWNLOAD_PATH_LEN = len(DOWNLOAD_PATH)

class HttpApiRequestHandler(BaseHttpApiRequestHandler):

    def __init__(self, request, client_address, server, controller: LocalServerController):
        super().__init__(request, client_address, server, controller)

    def controller(self) -> LocalServerController:
        return self._controller

    def do_GET(self):
        if not self.parse_path():
            return

        if self.url_path.startswith(DIRECTORY_PATH):
            self.handle_list_directory()
        elif self.url_path.startswith(DOWNLOAD_PATH):
            self.handle_download_file()
        elif self.url_path.startswith(FILE_PATH):
            self.handle_get_file_metadata()
        else:
            super().do_GET()
        
    def do_POST(self):
        if not self.parse_path():
            return

        if self.url_path.startswith(UPLOAD_PATH):
            self.handle_upload_file()
        else:
            super().do_POST()

    def do_PUT(self):
        if not self.parse_path():
            return

        if self.url_path.startswith(DIRECTORY_PATH):
            self.handle_create_directory()
        else:
            super().do_PUT()

    def parse_directory_path(self, path: str) -> list[str]:
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

    def handle_directory_error(self, e: DirectoryError):
        logging.error('Directory error: {}'.format(str(e)))
        if e.error_code() == FileServerErrorCode.DIRECTORY_NOT_FOUND:
            self.send_error_response(HTTPStatus.NOT_FOUND, e)
        elif e.error_code() == FileServerErrorCode.DIRECTORY_EXISTS:
            self.send_error_response(HTTPStatus.CONFLICT, e)
        elif e.error_code() == FileServerErrorCode.INVALID_PATH:
            self.send_error_response(HTTPStatus.NOT_FOUND, e)
        else:
            self.send_error_response(HTTPStatus.BAD_REQUEST, e)

    def handle_create_directory(self):
        '''
        
            Handle the create directory API.
            Create directory under the path.
            Parent directories must already exist.
            
            Method: PUT
            Path: /1/directory/<path>/<directory-name>
            Request Headers:
                x-privastore-session-id: <session-id>
            
            Examples:
                Create a directory "foo" under the root directory.
                PUT /1/directory/foo

                Create a directory "baz" under the path /foo/bar
                PUT /1/directory/foo/bar/baz
        '''
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
            self.controller().create_directory(path, directory_name)
        except DirectoryError as e:
            self.handle_directory_error(e)
            return
        except Exception as e:
            self.handle_internal_error(e)
            log_exception_stack()
            return

        self.send_response(HTTPStatus.OK)
        self.send_header(CONTENT_LENGTH_HEADER, '0')
        self.send_header(CONNECTION_HEADER, CONNECTION_CLOSE)
        self.end_headers()
    
    def handle_list_directory(self):
        '''
        
            Handle the list directory API.
            List the directory and file entries under the path.
            Response body will be listing in JSON format.

            Method: GET
            Path: /1/directory/<path>
            Request Headers:
                x-privastore-session-id: <session-id>
            
            Examples:
                List "/foo/bar" containing two directories "dir_1" and "dir_2"
                and a file "README.txt".

                GET /1/directory/foo/bar
                Response Body:
                    [
                        ['d', 'dir_1'],
                        ['d', 'dir_2'],
                        ['f', 'README.txt']
                    ]

        '''
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
            dir_entries = self.controller().list_directory(path)
            dir_entries = json.dumps(dir_entries).encode('utf-8')
        except DirectoryError as e:
            self.handle_directory_error(e)
            return
        except Exception as e:
            self.handle_internal_error(e)
            log_exception_stack()
            return

        self.send_response(HTTPStatus.OK)
        self.send_header(CONTENT_TYPE_HEADER, CONTENT_TYPE_JSON)
        self.send_header(CONTENT_LENGTH_HEADER, str(len(dir_entries)))
        self.send_header(CONNECTION_HEADER, CONNECTION_CLOSE)
        self.end_headers()
        self.wfile.write(dir_entries)
    
    def handle_get_file_metadata(self):
        '''
        
            Handle get file metadata API.

            Method: GET
            Path: /1/file/<path>
            Request Headers:
                x-privastore-session-id: <session-id>
            
            Examples:
                Get file /foo/bar metadata.

                GET /1/file/foo/bar
                Reponse Body:
                {
                    "mime-type": "application/octet-stream",
                    "versions":
                    [
                        {
                            "version": 1,
                            "file-size": 1572864,
                            "size-on-disk": 1572864,
                            "total-chunks": 2,
                            "local-file-id": "F-5c0875e8-3551-41f6-9e44-bb8af4f1718e",
                            "remote-file-id": "F-d2dc4f34-ae68-4a67-9227-ef1dc53f8f92",
                            "local-transfer-status": "SYNCED_DATA",
                            "remote-transfer-status": "SYNCED_DATA"
                        }
                    ]
                }

        '''
        logging.debug('Get file metadata')
        self.wrap_sockets()
        session_id = self.get_session_id()
        if session_id is None:
            return
        
        if not self.heartbeat_session(session_id):
            return

        try:
            path = self.parse_directory_path(self.url_path[FILE_PATH_LEN:])
            file_name = path.pop()
        except:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid directory path or filename')
            return
        
        try:
            file_metadata = self.controller().get_file_metadata(path, file_name)
            file_metadata = json.dumps(file_metadata).encode('utf-8')
        except DirectoryError as e:
            self.handle_directory_error(e)
            return
        except FileError as e:
            self.handle_file_error(e)
            return
        except Exception as e:
            self.handle_internal_error(e)
            log_exception_stack()
            return

        self.send_response(HTTPStatus.OK)
        self.send_header(CONTENT_TYPE_HEADER, CONTENT_TYPE_JSON)
        self.send_header(CONTENT_LENGTH_HEADER, str(len(file_metadata)))
        self.send_header(CONNECTION_HEADER, CONNECTION_CLOSE)
        self.end_headers()
        self.wfile.write(file_metadata)

    def handle_upload_file(self):
        '''

            Handle upload file API.

            Method: POST
            Path: /1/upload/<path>[?key=<key-id>]
            Request Headers:
                Content-Length: <file-size (bytes)>
                Content-Type: <mime-type>
                x-privastore-session-id: <session-id>
            Request Body:
                <file bytes>
            
        '''
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
            key_id = self.url_query.get('key')
            if key_id is not None:
                key_id = key_id[-1]

                if not Key.is_valid_key_id(key_id):
                    self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid encryption key!')
                    return
            else:
                # Default to encrypting using system key.
                key_id = 'system'
        except:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid encryption key!')
            return

        try:
            # TODO: Upload a new file version.
            self.controller().upload_file(path, file_name, self.rfile, self.content_len, 1, key_id)
        except DirectoryError as e:
            self.handle_directory_error(e)
            return
        except FileError as e:
            self.handle_file_error(e)
            return
        except Exception as e:
            self.handle_internal_error(e)
            log_exception_stack()
            return

        self.send_response(HTTPStatus.OK)
        self.send_header(CONTENT_LENGTH_HEADER, '0')
        self.send_header(CONNECTION_HEADER, CONNECTION_CLOSE)
        self.end_headers()
    
    def handle_download_file(self):
        '''

            Handle download file API.
            Optionally provide a version of file to download in query-string.

            Method: GET
            Path: /1/download/<path>[?version=<v>]
            Request Headers:
                x-privastore-session-id: <session-id>
            
            Response Headers:
                Content-Length: <file-size (in bytes)>
                Content-Type: <mime-type>
            Response Body:
                <file-bytes>

        '''
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
                file_version = int(file_version[-1])

                if file_version < 1:
                    self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid file version')
                    return
        except:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid file version')
            return
        
        try:
            self.controller().download_file(path, file_name, self.wfile, file_version, api_callback=self.send_download_file_headers)
        except DirectoryError as e:
            self.handle_directory_error(e)
        except FileError as e:
            self.handle_file_error(e)
        except Exception as e:
            self.handle_internal_error(e)
            log_exception_stack()
    
    def send_download_file_headers(self, file_id, file_type, file_size):
        logging.debug('Send download file headers [{}] [{}] [{}]'.format(file_id, file_type.mime_type, file_size))
        self.send_response(HTTPStatus.OK)
        self.send_header(CONTENT_TYPE_HEADER, file_type.mime_type)
        self.send_header(CONTENT_LENGTH_HEADER, str(file_size))
        self.end_headers()
from ...controller import RemoteServerController
from ....error import EpochError, FileServerErrorCode, RemoteFileError
from ....file import File, FILE_ID_LENGTH
from http import HTTPStatus
from ....api.http.http_request_handler import BaseHttpApiRequestHandler
from ....api.http.http_request_handler import CONNECTION_HEADER, CONNECTION_CLOSE, CONTENT_TYPE_HEADER, CONTENT_TYPE_JSON, CONTENT_LENGTH_HEADER
import json
import logging
import urllib.parse

EPOCH_NO_HEADER = 'x-privastore-epoch-no'
FILE_ID_HEADER = 'x-privastore-remote-file-id'
FILE_ID_HEADER_VALUE = '/1/file/{}'

EPOCH_PATH = '/1/epoch/'
EPOCH_PATH_LEN = len(EPOCH_PATH)
REMOTE_FILE_PATH = '/1/file/'
REMOTE_FILE_PATH_LEN = len(REMOTE_FILE_PATH)

class HttpApiRequestHandler(BaseHttpApiRequestHandler):

    def __init__(self, request, client_address, server, controller: RemoteServerController):
        super().__init__(request, client_address, server, controller)
    
    def controller(self) -> RemoteServerController:
        return self._controller

    def do_GET(self):
        if not self.parse_path():
            return
        
        if self.url_path.startswith(REMOTE_FILE_PATH):
            self.handle_remote_file_read()
        else:
            super().do_GET()

    def do_POST(self):
        if not self.parse_path():
            return

        if self.url_path.startswith(REMOTE_FILE_PATH):
            self.handle_create_remote_file()
        else:
            super().do_POST()

    def do_PUT(self):
        if not self.parse_path():
            return

        if self.url_path.startswith(REMOTE_FILE_PATH):
            self.handle_remote_file_append()
        else:
            super().do_PUT()

    def get_epoch_no_from_header(self) -> int:
        epoch_no = self.headers.get(EPOCH_NO_HEADER)

        if epoch_no is None:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Missing {} header'.format(EPOCH_NO_HEADER))
            return
        
        try:
            epoch_no = int(epoch_no)
            if epoch_no < 1:
                self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid {} header value. Must be >= 1'.format(EPOCH_NO_HEADER))
                return
        except:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid {} header value'.format(EPOCH_NO_HEADER))
            return

        return epoch_no

    def get_remote_file_id(self):
        try:
            end = REMOTE_FILE_PATH_LEN + FILE_ID_LENGTH
            file_id = self.url_path[REMOTE_FILE_PATH_LEN:end]
            if not File.is_valid_file_id(file_id):
                self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid remote file id!')
                return
            return file_id
        except:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid remote file id!')
            return

    def handle_epoch_error(self, e: EpochError) -> None:
        logging.error('Epoch error: {}'.format(str(e)))
        error_code = e.error_code()
        if error_code == FileServerErrorCode.EPOCH_IS_OVER:
            self.send_error_response(HTTPStatus.CONFLICT, e)
        elif error_code == FileServerErrorCode.INVALID_EPOCH_NO:
            self.send_error_response(HTTPStatus.BAD_REQUEST, e)
        else:
            self.send_error_response(HTTPStatus.BAD_REQUEST, e)

    def handle_create_remote_file(self):
        '''

            Handle the create remote file API.
            Method: POST
            Path: /1/file[?size=<file-size (in bytes)>]
            Request Headers:
                x-privastore-session-id: <session-id>
                x-privastore-epoch-no: <epoch-no>
            
            Response Headers:
                Location: /1/file/<file-id>
        '''
        logging.debug('Create remote file request')
        self.wrap_sockets()
        self.read_body()

        session_id = self.get_session_id()
        if session_id is None:
            return
        
        if not self.heartbeat_session(session_id):
            return

        epoch_no = self.get_epoch_no_from_header()
        if epoch_no is None:
            return

        remote_id = File.generate_file_id()

        file_size = self.url_query.get('size')
        if file_size is not None:
            try:
                file_size = int(file_size[-1])
                if file_size < 0:
                    self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid remote file size value. Must be >= 0')
                    return
            except:
                self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid remote file size value')
                return
        else:
            file_size = 0

        try:
            self.controller().create_file(epoch_no, remote_id, file_size)
        except EpochError as e:
            self.handle_epoch_error(e)
        except RemoteFileError as e:
            self.handle_file_error(e)
        except Exception as e:
            self.handle_internal_error(e)
        
        self.send_response(HTTPStatus.OK)
        self.send_header(CONTENT_LENGTH_HEADER, '0')
        self.send_header(CONNECTION_HEADER, CONNECTION_CLOSE)
        self.send_header(FILE_ID_HEADER, FILE_ID_HEADER_VALUE.format(remote_id))
        self.end_headers()

    def handle_commit_remote_file(self):
        pass

    def handle_remote_file_append(self):
        pass

    def handle_remote_file_read(self):
        pass
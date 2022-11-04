from ....error import AuthenticationError, DirectoryError, FileError, SessionError
from http import HTTPStatus
from ....api.http.http_request_handler import BaseHttpApiRequestHandler
from ....api.http.http_request_handler import CONNECTION_HEADER, CONNECTION_CLOSE, CONTENT_TYPE_HEADER, CONTENT_TYPE_JSON, CONTENT_LENGTH_HEADER
import json
import logging
import urllib.parse

REMOTE_FILE_PATH = '/1/file/'
REMOTE_FILE_PATH_LEN = len(REMOTE_FILE_PATH)

class HttpApiRequestHandler(BaseHttpApiRequestHandler):

    def __init__(self, request, client_address, server, controller):
        super().__init__(request, client_address, server, controller)
    
    def do_GET(self):
        if not self.parse_path():
            return

        super().do_GET()

    def do_POST(self):
        super().do_POST()

    def do_PUT(self):
        super().do_PUT()

    def handle_append_file_chunks(self):
        pass

    def handle_read_file_chunk(self):
        pass
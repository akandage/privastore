import base64
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
import logging
import urllib
from ...util.sock import SocketWrapper

AUTHORIZATION_HEADER = 'Authorization'
CONNECTION_HEADER = 'Connection'
CONNECTION_CLOSE = 'close'
CONTENT_TYPE_HEADER = 'Content-Type'
CONTENT_TYPE_JSON = 'application/json'
CONTENT_LENGTH_HEADER = 'Content-Length'

class BaseHttpApiRequestHandler(BaseHTTPRequestHandler):

    def __init__(self, request, client_address, server):
        super().__init__(request, client_address, server)
    
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

    def parse_content_length(self):
        content_len =  self.headers.get(CONTENT_LENGTH_HEADER)

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
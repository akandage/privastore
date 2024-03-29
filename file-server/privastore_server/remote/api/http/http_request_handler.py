from ...controller import RemoteServerController
from ....error import EpochError, FileError, FileCacheError, FileServerErrorCode, RemoteFileError
from ....file import File, FILE_ID_LENGTH
from http import HTTPStatus
from ....api.http.http_request_handler import BaseHttpApiRequestHandler
from ....api.http.http_request_handler import CONNECTION_HEADER, CONNECTION_CLOSE, CONTENT_TYPE_HEADER, CONTENT_TYPE_JSON, CONTENT_LENGTH_HEADER
import json
import logging
from ....util.file import read_all

EPOCH_NO_HEADER = 'x-privastore-epoch-no'
FILE_ID_HEADER = 'x-privastore-remote-file-id'

EPOCH_PATH = '/1/epoch'
EPOCH_PATH_LEN = len(EPOCH_PATH)
REMOTE_FILE_PATH = '/1/file'
REMOTE_FILE_PATH_LEN = len(REMOTE_FILE_PATH)
COMMIT_PATH_SUFFIX = '/commit'
METADATA_PATH_SUFFIX = '/metadata'

class HttpApiRequestHandler(BaseHttpApiRequestHandler):

    def __init__(self, request, client_address, server, controller: RemoteServerController):
        super().__init__(request, client_address, server, controller)
    
    def controller(self) -> RemoteServerController:
        return self._controller

    def do_GET(self):
        if not self.parse_path():
            return
        
        if self.url_path.startswith(REMOTE_FILE_PATH):
            if self.url_path.endswith(METADATA_PATH_SUFFIX):
                self.handle_get_remote_file_metadata()
            else:
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

        if self.url_path.startswith(EPOCH_PATH):
            self.handle_end_epoch()
        elif self.url_path.startswith(REMOTE_FILE_PATH):
            if self.url_path.endswith(COMMIT_PATH_SUFFIX):
                self.handle_commit_remote_file()
            else:
                self.handle_remote_file_write()
        else:
            super().do_PUT()

    def do_DELETE(self):
        if not self.parse_path():
            return
        
        if self.url_path.startswith(REMOTE_FILE_PATH):
            self.handle_remove_file()
        else:
            super().do_DELETE()

    def get_chunk_num(self) -> int:
        chunk_num = self.url_query.get('chunk')

        if chunk_num is not None:
            try:
                chunk_num = int(chunk_num[-1])
                if chunk_num < 1:
                    self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid chunk number. Must be >= 1')
                    return
            except:
                self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid chunk number')
                return
        else:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Missing chunk number')
            return

        return chunk_num

    def get_epoch_no_from_path(self) -> int:
        try:
            prefix_len = EPOCH_PATH_LEN+1
            return self.parse_epoch_no(self.url_path[prefix_len:])
        except:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid path')
            return

    def get_epoch_no_from_header(self) -> int:
        epoch_no = self.headers.get(EPOCH_NO_HEADER)

        if epoch_no is None:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Missing {} header'.format(EPOCH_NO_HEADER))
            return
        
        return self.parse_epoch_no(epoch_no)

    def parse_epoch_no(self, val) -> int:
        try:
            epoch_no = int(val)
            if epoch_no < 1:
                self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid epoch value. Must be >= 1')
                return
        except:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Invalid epoch value')
            return

        return epoch_no

    def get_remote_file_id(self) -> str:
        try:
            prefix_len = REMOTE_FILE_PATH_LEN+1
            end = prefix_len + FILE_ID_LENGTH
            file_id = self.url_path[prefix_len:end]
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
            
            Response Headers:
                x-privastore-remote-file-id: <file-id>

        '''
        logging.debug('Create remote file request')
        self.wrap_sockets()
        self.read_body()

        session_id = self.get_session_id()
        if session_id is None:
            return
        
        if not self.heartbeat_session(session_id):
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
            self.controller().create_file(remote_id, file_size)
        except EpochError as e:
            self.handle_epoch_error(e)
            return
        except (FileError, FileCacheError, RemoteFileError) as e:
            self.handle_file_error(e)
            return
        except Exception as e:
            self.handle_internal_error(e)
            return
        
        self.send_response(HTTPStatus.OK)
        self.send_header(CONTENT_LENGTH_HEADER, '0')
        self.send_header(CONNECTION_HEADER, CONNECTION_CLOSE)
        self.send_header(FILE_ID_HEADER, remote_id)
        self.end_headers()

    def handle_commit_remote_file(self):
        '''

            Handle the commit remote file API.
            Method: PUT
            Path: /1/file/<file-id>/commit
            Request Headers:
                x-privastore-session-id: <session-id>
                x-privastore-epoch-no: <epoch-no>

        '''
        logging.debug('Commit remote file request')
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

        remote_id = self.get_remote_file_id()
        if remote_id is None:
            return

        try:
            self.controller().commit_file(epoch_no, remote_id)
        except EpochError as e:
            self.handle_epoch_error(e)
            return
        except (FileError, FileCacheError, RemoteFileError) as e:
            self.handle_file_error(e)
            return
        except Exception as e:
            self.handle_internal_error(e)
            return
        
        self.send_response(HTTPStatus.OK)
        self.send_header(CONTENT_LENGTH_HEADER, '0')
        self.send_header(CONNECTION_HEADER, CONNECTION_CLOSE)
        self.end_headers()

    def handle_get_remote_file_metadata(self):
        '''

            Handle the get remote file metadata API.
            Method: GET
            Path: /1/file/<file-id>/metadata
            Request Headers:
                x-privastore-session-id: <session-id>
            
            Response Body:
                {
                    "remote-file-id": <file-id>,
                    "file-size": <allocated size (in bytes)>,
                    "file-chunks": <current number of file chunks>,
                    "is-committed": <committed flag>,
                    "created-epoch-no": <created epoch no>,
                    "removed-epoch-no": <removed epoch no (if deleted)>
                }

        '''
        logging.debug('Get remote file metadata request')
        self.wrap_sockets()
        self.read_body()

        session_id = self.get_session_id()
        if session_id is None:
            return
        
        if not self.heartbeat_session(session_id):
            return
        
        remote_id = self.get_remote_file_id()
        if remote_id is None:
            return

        try:
            file_metadata = self.controller().get_file_metadata(remote_id)
            file_metadata = json.dumps(file_metadata).encode('utf-8')
        except RemoteFileError as e:
            self.handle_file_error(e)
            return
        except Exception as e:
            self.handle_internal_error(e)
            return
        
        self.send_response(HTTPStatus.OK)
        self.send_header(CONTENT_TYPE_HEADER, CONTENT_TYPE_JSON)
        self.send_header(CONTENT_LENGTH_HEADER, str(len(file_metadata)))
        self.send_header(CONNECTION_HEADER, CONNECTION_CLOSE)
        self.end_headers()
        self.wfile.write(file_metadata)

    def handle_remote_file_write(self):
        '''

            Handle the remote file write API.
            Append a chunk to the file i.e. chunk-number must be == next chunk number.
            File chunk numbers use 1-based indexing.
            Method: PUT
            Path: /1/file/<file-id>?chunk=<chunk-number>
            Request Headers:
                x-privastore-session-id: <session-id>

        '''
        logging.debug('Append remote file chunk request')
        self.wrap_sockets()

        session_id = self.get_session_id()
        if session_id is None:
            return
        
        if not self.heartbeat_session(session_id):
            return

        remote_id = self.get_remote_file_id()
        if remote_id is None:
            return
        
        chunk_num = self.get_chunk_num()
        if chunk_num is None:
            return
        
        if not self.parse_content_length():
            return

        max_chunk_size = self.controller().store().file_chunk_size()
        if self.content_len == 0:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'File chunk cannot be empty')
            return
        if self.content_len > max_chunk_size:
            self.send_error_response(HTTPStatus.BAD_REQUEST, FileError('File chunk too large', FileServerErrorCode.FILE_CHUNK_TOO_LARGE))
            return
        
        chunk = read_all(self.rfile, self.content_len)
        if len(chunk) < self.content_len:
            self.send_error_response(HTTPStatus.BAD_REQUEST, 'Not all chunk bytes could be read!')
            return
        
        try:
            self.controller().append_to_file(remote_id, chunk_num, chunk)
        except EpochError as e:
            self.handle_epoch_error(e)
            return
        except (FileError, FileCacheError, RemoteFileError) as e:
            self.handle_file_error(e)
            return
        except Exception as e:
            self.handle_internal_error(e)
            return
        
        self.send_response(HTTPStatus.OK)
        self.send_header(CONTENT_LENGTH_HEADER, '0')
        self.send_header(CONNECTION_HEADER, CONNECTION_CLOSE)
        self.end_headers()

    def handle_remote_file_read(self):
        '''
            Handle the remote file read API.
            Read a chunk from a (committed) remote file.
            Method: GET
            Path: /1/file/<file-id>?chunk=<chunk-number>
            Request Headers:
                x-privastore-session-id: <session-id>
                x-privastore-epoch-no: <epoch-no>

            Response Body:
                <chunk bytes>
            
        '''
        logging.debug('Read remote file chunk request')
        self.wrap_sockets()
        self.read_body()

        session_id = self.get_session_id()
        if session_id is None:
            return
        
        if not self.heartbeat_session(session_id):
            return
        
        remote_id = self.get_remote_file_id()
        if remote_id is None:
            return
        
        chunk_num = self.get_chunk_num()
        if chunk_num is None:
            return
        
        try:
            chunk = self.controller().read_from_file(remote_id, chunk_num)
        except EpochError as e:
            self.handle_epoch_error(e)
            return
        except (FileError, FileCacheError, RemoteFileError) as e:
            self.handle_file_error(e)
            return
        except Exception as e:
            self.handle_internal_error(e)
            return
        
        self.send_response(HTTPStatus.OK)
        self.send_header(CONTENT_LENGTH_HEADER, len(chunk))
        self.send_header(CONNECTION_HEADER, CONNECTION_CLOSE)
        self.end_headers()
        self.wfile.write(chunk)

    def handle_remove_file(self):
        '''

            Handle the remove remote file API.
            Method: DELETE
            Path: /1/file/<file-id>
            Request Headers:
                x-privastore-session-id: <session-id>

        '''
        logging.debug('Remove remote file request')
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
        
        remote_id = self.get_remote_file_id()
        if remote_id is None:
            return

        try:
            self.controller().remove_file(epoch_no, remote_id)
        except EpochError as e:
            self.handle_epoch_error(e)
            return
        except RemoteFileError as e:
            self.handle_file_error(e)
            return
        except Exception as e:
            self.handle_internal_error(e)
            return
        
        self.send_response(HTTPStatus.OK)
        self.send_header(CONTENT_LENGTH_HEADER, '0')
        self.send_header(CONNECTION_HEADER, CONNECTION_CLOSE)
        self.end_headers()

    def handle_end_epoch(self):
        '''

            Handle the end epoch API.
            End the given epoch and place a marker file by attaching a remote
            file id.
            Method: PUT
            Path: /1/epoch/<epoch-no>[?marker-id=<file-id>]
            Request Headers:
                x-privastore-session-id: <session-id>

        '''
        logging.debug('End epoch request')
        self.wrap_sockets()
        self.read_body()

        session_id = self.get_session_id()
        if session_id is None:
            return
        
        if not self.heartbeat_session(session_id):
            return

        epoch_no = self.get_epoch_no_from_path()
        if epoch_no is None:
            return
        
        marker_id = self.url_query.get('marker-id')
        if marker_id is not None:
            marker_id = marker_id[-1]

        try:
            self.controller().end_epoch(epoch_no, marker_id)
        except EpochError as e:
            self.handle_epoch_error(e)
            return
        except RemoteFileError as e:
            self.handle_file_error(e)
            return
        except Exception as e:
            self.handle_internal_error(e)
            return
        
        self.send_response(HTTPStatus.OK)
        self.send_header(CONTENT_LENGTH_HEADER, '0')
        self.send_header(CONNECTION_HEADER, CONNECTION_CLOSE)
        self.end_headers()
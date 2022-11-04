from ..controller import Controller
from ..error import FileDownloadError, FileUploadError
from ..file import File
from .file_transfer_status import FileTransferStatus
from ..util.file import chunked_copy, str_mem_size, str_path
import logging

class LocalServerController(Controller):

    '''
        Local file server controller.

        dao_factory - DAO factory
        db_conn_mgr - database connection manager
        session_mgr - session store
        store - file store
        encode_chunk - file chunk encoder
        decode_chunk - file chunk decoder
    '''
    def __init__(self, dao_factory, db_conn_mgr, session_mgr, store, encode_chunk, decode_chunk):
        super().__init__(db_conn_mgr, session_mgr, store)
        self._dao_factory = dao_factory
        self._encode_chunk = encode_chunk
        self._decode_chunk = decode_chunk
        
    def login_user(self, username, password):
        logging.debug('User [{}] login attempt'.format(username))
        conn = self.db_conn_mgr().db_connect()
        try:
            logging.debug('Acquired database connection')
            user_dao = self._dao_factory.user_dao(conn)
            user_dao.login_user(username, password)
            logging.debug('User [{}] logged in successfully'.format(username))
            session_id = self.session_mgr().start_session(username)
            return session_id
        finally:
            self.db_conn_mgr().db_close(conn)

    def create_directory(self, path, directory_name, is_hidden=False):
        logging.debug('Create directory [{}]'.format(str_path(path + [directory_name])))
        conn = self.db_conn_mgr().db_connect()
        try:
            logging.debug('Acquired database connection')
            dir_dao = self._dao_factory.directory_dao(conn)
            dir_dao.create_directory(path, directory_name, is_hidden)
            logging.debug('Created directory [{}]'.format(str_path(path + [directory_name])))
        finally:
            self.db_conn_mgr().db_close(conn)

    def upload_file(self, path, file_name, file, file_size, file_version=1):
        logging.debug('Upload file [{}] version [{}] size [{}]'.format(str_path(path + [file_name]), file_version, file_size))
        conn = self.db_conn_mgr().db_connect()
        try:
            logging.debug('Acquired database connection')

            dir_dao = self._dao_factory.directory_dao(conn)
            file_dao = self._dao_factory.file_dao(conn)

            if file_version == 1:
                #
                # Register a new file in the database with a single version.
                #
                dir_dao.create_file(path, file_name)
            else:
                # TODO
                raise Exception('Not implemented!')

            upload_file = None

            try:
                #
                # Set a unique id (UUID) for the file and record its size.
                #
                local_file_id = File.generate_file_id()
                file_dao.update_file_local(path, file_name, file_version, local_file_id, file_size, size_on_disk=0, total_chunks=0, transfer_status=FileTransferStatus.RECEIVING)

                #
                # Uploaded file data is stored in the cache and cannot be removed
                # or evicted from the cache until it has been synced to the remote
                # server.
                #
                upload_file = self.store().write_file(local_file_id, file_size=file_size, encode_chunk=self._encode_chunk, decode_chunk=self._decode_chunk)
                logging.debug('Opened file for writing in cache [{}]'.format(upload_file.file_id()))

                #
                # Read the file in chunks of the configured chunk size and append to
                # the file in the cache.
                #
                bytes_transferred = chunked_copy(file, upload_file, file_size, self.store().file_chunk_size())
                if bytes_transferred < file_size:
                    raise FileUploadError('Could not upload all file data! [{}/{}]'.format(str_mem_size(bytes_transferred), str_mem_size(file_size)))

                self.store().close_file(upload_file, removable=False, writable=False)
                logging.debug('File data uploaded [{}]'.format(str_mem_size(bytes_transferred)))

                size_on_disk = upload_file.size_on_disk()
                logging.debug('File size on disk [{}]'.format(size_on_disk))
                total_chunks = upload_file.total_chunks()
                logging.debug('File total chunks [{}]'.format(total_chunks))

                #
                # Update the status of the file in the database to received and
                # record the local file id and file size.
                #
                file_dao.update_file_local(path, file_name, file_version, upload_file.file_id(), file_size, size_on_disk, total_chunks, transfer_status=FileTransferStatus.RECEIVED)
                logging.debug('File metadata updated')
            except Exception as e:
                logging.error('Could not upload file: {}'.format(str(e)))

                #
                # Cleanup if any error occurs during the upload process.
                #

                try:
                    dir_dao.remove_file(path, file_name, delete=True)
                except Exception as e:
                    logging.error('Could not cleanup uploaded file in db: {}'.format(str(e)))

                if upload_file is not None:
                    try:
                        self.store().close_file(upload_file)
                    except Exception as e:
                        logging.error('Could not close uploaded file in cache: {}'.format(str(e)))

                    try:
                        self.store().remove_file(upload_file)
                        logging.debug('Removed file from cache')
                    except Exception as e:
                        logging.error('Could not remove uploaded file from cache: {}'.format(str(e)))

                raise e

            logging.debug('Uploaded file [{}]'.format(str_path(path + [file_name])))
        finally:
            self.db_conn_mgr().db_close(conn)

    def download_file(self, path, file_name, file, file_version=None, api_callback=None):
        logging.debug('Download file [{}] version [{}]'.format(str_path(path + [file_name]), file_version))
        conn = self.db_conn_mgr().db_connect()
        try:
            logging.debug('Acquired database connection')

            file_dao = self._dao_factory.file_dao(conn)
            file_metadata = file_dao.get_file_version_metadata(path, file_name, file_version)
            logging.debug('Retrieved file metadata')
            file_type = file_metadata.file_type
            file_id = file_metadata.local_id
            file_size = file_metadata.file_size
            transfer_status = file_metadata.transfer_status

            if transfer_status == FileTransferStatus.RECEIVING:
                raise FileDownloadError('File upload not complete!')
            elif transfer_status == FileTransferStatus.RECEIVING_FAILED:
                raise FileDownloadError('File upload failed!')
            
            #
            # First, try reading the file from the cache if it is already
            # present there.
            #
            download_file = self.store().read_file(file_id, encode_chunk=self._encode_chunk, decode_chunk=self._decode_chunk)

            if download_file is None:
                #
                # TODO: Handle cache miss.
                #
                raise Exception('Not implemented!')

            #
            # Cache hit, send the file to the client.
            #
            logging.debug('Opened file [{}] for reading in cache'.format(download_file.file_id()))

            try:
                if api_callback is not None:
                    #
                    # Notify the API of the file-type, file-size etc. in case it
                    # needs to send headers before we transfer the actual file.
                    #
                    api_callback(file_id, file_type, file_size)

                bytes_transferred = chunked_copy(download_file, file, file_size, self.store().file_chunk_size())
                if bytes_transferred < file_size:
                    raise FileDownloadError('Could not download all file data! [{}/{}]'.format(str_mem_size(bytes_transferred), str_mem_size(file_size)))
            finally:
                try:
                    self.store().close_file(download_file)
                except Exception as e:
                    logging.warn('Could not close download file in cache: {}'.format(str(e)))
            
            logging.debug('File data downloaded [{}]'.format(str_mem_size(bytes_transferred)))
        finally:
            self.db_conn_mgr().db_close(conn)

    def list_directory(self, path, show_hidden=False):
        logging.debug('List directory [{}]'.format(str_path(path)))
        conn = self.db_conn_mgr().db_connect()
        try:
            logging.debug('Acquired database connection')
            dir_dao = self._dao_factory.directory_dao(conn)
            dir_entries = dir_dao.list_directory(path, show_hidden)
            logging.debug('Listed directory [{}]'.format(str_path(path)))
            return dir_entries
        finally:
            self.db_conn_mgr().db_close(conn)
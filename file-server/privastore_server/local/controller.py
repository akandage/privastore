from .async_controller import AsyncController
from ..controller import Controller
from .db.dao_factory import DAOFactory
from ..db.db_conn_mgr import DbConnectionManager
from .database import DbWrapper
from ..error import FileCacheError, FileDeleteError, FileDownloadError, FileServerErrorCode, FileUploadError
from ..file import File
from ..file_cache import FileCache
from ..file_chunk import chunk_encoder, chunk_decoder, default_chunk_encoder, default_chunk_decoder, get_encrypted_chunk_encoder, get_encrypted_chunk_decoder
from .file_task import FileTask
from .file_transfer_status import FileTransferStatus
from .file_type import FileType
from ..key import Key
from ..session_mgr import SessionManager
from ..util.file import chunked_copy, str_mem_size, str_path, write_all
from ..util.crypto import get_encryptor_factory, get_decryptor_factory
from ..util.logging import log_exception_stack
import logging
from threading import RLock
from typing import BinaryIO, Callable, Optional

class LocalServerController(Controller):

    '''
        Local file server controller.

        dao_factory - DAO factory
        db_conn_mgr - database connection manager
        session_mgr - session store
        store - file store
    '''
    def __init__(self, async_controller: AsyncController, dao_factory: DAOFactory, db_conn_mgr: DbConnectionManager, session_mgr: SessionManager, store: FileCache):
        super().__init__(db_conn_mgr, session_mgr, store)
        self._async_controller = async_controller
        self._dao_factory = dao_factory
        self._db = DbWrapper(dao_factory, db_conn_mgr)
        self._chunk_encryptors: dict[str, chunk_encoder] = dict()
        self._chunk_enc_lock: RLock = RLock()
        self._chunk_decryptors: dict[str, chunk_decoder] = dict()
        self._chunk_dec_lock: RLock = RLock()

    def async_controller(self):
        return self._async_controller

    def dao_factory(self):
        return self.db().dao_factory()

    def db(self):
        return self._db

    def remote_enabled(self):
        return self._async_controller.remote_enabled()

    def get_key(self, key_id: str) -> Key:
        conn = self.db_conn_mgr().db_connect()
        try:
            return self.dao_factory().key_dao(conn).get_key(key_id)
        finally:
            self.db_conn_mgr().db_close(conn)

    def chunk_encryptor(self, key_id: Optional[str]=None) -> chunk_encoder:
        with self._chunk_enc_lock:
            if key_id is None or key_id == 'null':
                # Default key is the "null" key or no encryption.
                return default_chunk_encoder
            chunk_enc = self._chunk_encryptors.get(key_id)
            if chunk_enc is not None:
                return chunk_enc

            logging.debug('Initializing file encryption with key id [{}]'.format(key_id))
            key = self.get_key(key_id)
            enc_factory = get_encryptor_factory(key.algorithm(), key.key_bytes())
            chunk_enc = get_encrypted_chunk_encoder(enc_factory)
            self._chunk_encryptors[key_id] = chunk_enc
            logging.debug('Initialized file encryption with {}'.format(str(key)))
            return chunk_enc

    def chunk_decryptor(self, key_id: Optional[str]=None) -> chunk_decoder:
        with self._chunk_dec_lock:
            if key_id is None or key_id == 'null':
                # Default key is the "null" key or no encryption.
                return default_chunk_decoder
            chunk_dec = self._chunk_decryptors.get(key_id)
            if chunk_dec is not None:
                return chunk_dec

            logging.debug('Initializing file decryption with key id [{}]'.format(key_id))
            key = self.get_key(key_id)
            dec_factory = get_decryptor_factory(key.algorithm(), key.key_bytes())
            chunk_dec = get_encrypted_chunk_decoder(dec_factory)
            self._chunk_decryptors[key_id] = chunk_dec
            logging.debug('Initialized file decryption with {}'.format(str(key)))
            return chunk_dec

    def init_store(self):
        conn = self.db_conn_mgr().db_connect()
        try:
            file_dao = self.dao_factory().file_dao(conn)
            unsynced_local_files = file_dao.list_unsynced_files(local=True)
            num_files_removed = 0

            for file in unsynced_local_files:
                file_id = file.file_id
                version = file.version
                local_id = file.local_id

                file_dao.remove_file_version(file_id, version)

                if local_id is not None:
                    file_dao.remove_file_data(local_id)

                    try:
                        self.store().remove_file_by_id(local_id)
                        num_files_removed += 1
                    except Exception as e:
                        logging.warning('Could not remove file [{}] from cache: {}'.format(local_id, str(e)))

            unsynced_remote_files = file_dao.list_unsynced_files(remote=True)
            for file in unsynced_remote_files:
                self.store().set_file_removable(file.local_id, False)
                
            logging.debug('Cleaned up [{}] files in cache'.format(num_files_removed))
        finally:
            self.db_conn_mgr().db_close(conn)

    def login_user(self, username: str, password: str):
        logging.debug('User [{}] login attempt'.format(username))
        conn = self.db_conn_mgr().db_connect()
        try:
            logging.debug('Acquired database connection')
            user_dao = self.dao_factory().user_dao(conn)
            user_dao.login_user(username, password)
            logging.debug('User [{}] logged in successfully'.format(username))
            session_id = self.session_mgr().start_session(username)
            return session_id
        finally:
            self.db_conn_mgr().db_close(conn)

    def create_directory(self, path: list[str], directory_name: str, is_hidden: bool=False):
        logging.debug('Create directory [{}]'.format(str_path(path + [directory_name])))
        conn = self.db_conn_mgr().db_connect()
        try:
            logging.debug('Acquired database connection')
            dir_dao = self.dao_factory().directory_dao(conn)
            dir_dao.create_directory(path, directory_name, is_hidden)
            logging.debug('Created directory [{}]'.format(str_path(path + [directory_name])))
        finally:
            self.db_conn_mgr().db_close(conn)

    def upload_file(self, path: list[str], file_name: str, file: BinaryIO, file_size: int, file_version: int, key_id: str, sync: bool=True):
        logging.debug('Upload file [{}] version [{}] size [{}]'.format(str_path(path + [file_name]), file_version, file_size))

        if file_version == 1:
            #
            # Register a new file in the database with a single version.
            #
            self.db().create_file(path, file_name)
        else:
            # TODO
            raise Exception('Not implemented!')

        upload_file: File = None
        chunk_encryptor: chunk_encoder = self.chunk_encryptor(key_id)
        file_uploaded: bool = False

        try:
            #
            # Set a unique id (UUID) for the file and record its size.
            #
            local_file_id = File.generate_file_id()
            self.db().update_file_local(path, file_name, file_version, local_file_id, key_id, file_size, size_on_disk=0, total_chunks=0, transfer_status=FileTransferStatus.TRANSFERRING_DATA)
            logging.debug('Set local file id [{}]'.format(local_file_id))

            #
            # Uploaded file data is stored in the cache and cannot be removed
            # or evicted from the cache until it has been synced to the remote
            # server.
            #
            upload_file = self.store().write_file(local_file_id, alloc_space=file_size, encode_chunk=chunk_encryptor)
            logging.debug('Opened file for writing in cache [{}]'.format(upload_file.file_id()))

            if self.remote_enabled():
                if sync:
                    # TODO: Configure timeout.
                    self.async_controller().start_upload(local_file_id, file_size, timeout=30)

            #
            # Read the file in chunks of the configured chunk size and append to
            # the file in the cache.
            #
            bytes_read = 0
            while bytes_read < file_size:
                # TODO: Config this from read buffer size.
                data = file.read(min(file_size - bytes_read, 64*1024))
                data_len = len(data)
                if data_len == 0:
                    raise FileUploadError('Could not read all upload file data!', FileServerErrorCode.IO_ERROR)
                bytes_read += data_len
                upload_file.write(data)
            upload_file.flush()

            # bytes_transferred = chunked_copy(file, upload_file, file_size, self.store().file_chunk_size())
            if bytes_read < file_size:
                raise FileUploadError('Could not upload all file data! [{}/{}]'.format(str_mem_size(bytes_read), str_mem_size(file_size)))

            self.store().close_file(upload_file, removable=False, writable=False)
            file_uploaded = True
            logging.debug('File data uploaded [{}]'.format(str_mem_size(file_size)))

            size_on_disk = upload_file.size_on_disk()
            logging.debug('File size on disk [{}]'.format(size_on_disk))
            total_chunks = upload_file.total_chunks()
            logging.debug('File total chunks [{}]'.format(total_chunks))

            #
            # Update the status of the file in the database to received and
            # record the local file id and file size.
            #
            self.db().update_file_local(path, file_name, file_version, upload_file.file_id(), key_id, file_size, size_on_disk, total_chunks, transfer_status=FileTransferStatus.SYNCED_DATA)
            logging.debug('File metadata updated')

            #
            # If this is a synced upload, make sure all data has made it to the
            # the remote server before returning a response to the caller.
            #
            if self.remote_enabled():
                if sync:
                    # TODO: Configure timeout.
                    self.async_controller().commit_upload(local_file_id, timeout=30)
        except Exception as e:
            logging.error('Could not upload file: {}'.format(str(e)))
            log_exception_stack()

            #
            # Cleanup if any error occurs during the upload process.
            #

            if self.remote_enabled():
                try:
                    self.async_controller().stop_upload(local_file_id)
                except Exception as e:
                    logging.warn('Could not stop file [{}] upload: {}'.format(str(e)))

            if upload_file is not None:
                if not file_uploaded:
                    try:
                        self.store().close_file(upload_file)
                    except Exception as e1:
                        logging.error('Could not close uploaded file in cache: {}'.format(str(e1)))

                try:
                    self.store().set_file_removable(upload_file.file_id(), True)
                    logging.debug('Set file removable')
                except Exception as e1:
                    logging.error('Could not remove uploaded file from cache: {}'.format(str(e1)))

                try:
                    self.store().remove_file(upload_file)
                    logging.debug('Removed file from cache')
                except Exception as e1:
                    logging.error('Could not remove uploaded file from cache: {}'.format(str(e1)))

            try:
                self.db().remove_file(path, file_name)
                logging.debug('Removed uploaded file from db')
            except Exception as e1:
                logging.error('Could not cleanup uploaded file in db: {}'.format(str(e1)))

            raise e

        logging.debug('Uploaded file [{}]'.format(str_path(path + [file_name])))

    def download_file(self, path: list[str], file_name: str, file: BinaryIO, file_version: Optional[int]=None, api_callback: Optional[Callable[[str, FileType, int], None]]=None, metadata_only: bool=False):
        logging.debug('Download file [{}] version [{}]'.format(str_path(path + [file_name]), file_version))

        file_metadata = self.db().get_file_version_metadata(path, file_name, file_version)
        logging.debug('Retrieved file metadata')

        file_type = file_metadata.file_type
        file_id = file_metadata.local_id
        key_id = file_metadata.key_id
        file_size = file_metadata.file_size
        transfer_status = file_metadata.local_transfer_status
        total_chunks = file_metadata.total_chunks

        logging.debug('File local transfer status [{}]'.format(transfer_status.name))
        if transfer_status != FileTransferStatus.SYNCED_DATA:
            raise FileDownloadError('Cannot download file [{}]. File is not fully uploaded'.format(file_id), FileServerErrorCode.FILE_NOT_READABLE)
        
        chunk_decryptor = self.chunk_decryptor(key_id)

        #
        # First, try reading the file from the cache if it is already
        # present there.
        #
        download_file = self.store().read_file(file_id, decode_chunk=chunk_decryptor)

        if download_file is None:
            #
            # Cache miss, download the file into the cache.
            #
            logging.debug('Cache miss, starting download')
            self.async_controller().start_download(file_id, timeout=30)
            download_file = self.store().read_file(file_id, decode_chunk=chunk_decryptor)

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

            if metadata_only:
                logging.debug('Metadata sent. Done')
                return

            bytes_transferred = 0
            try:
                for _ in range(total_chunks):
                    chunk_data = download_file.read_chunk()
                    bytes_transferred += write_all(file, chunk_data)

                if bytes_transferred < file_size:
                    logging.error('Could not download all file data! [{}/{}]'.format(str_mem_size(bytes_transferred), str_mem_size(file_size)))
            except Exception as e:
                logging.error('Could not download all file data! [{}/{}]: {}'.format(str_mem_size(bytes_transferred), str_mem_size(file_size)), str(e))
                log_exception_stack()
        finally:
            try:
                self.store().close_file(download_file)
            except Exception as e:
                logging.warn('Could not close download file in cache: {}'.format(str(e)))
        
        logging.debug('File data downloaded [{}]'.format(str_mem_size(bytes_transferred)))

    def remove_file_check_readers_cb(self, path: list[str], file_name: str, version: Optional[int], local_id: str, remote_id: str):
        try:
            if self.store().file_has_readers(local_id):
                raise FileDeleteError('Cannot delete file [{}] version [{}]. File has readers!'.format(str_path(path + [file_name]), version))
        except FileCacheError as e:
            if e.error_code() == FileServerErrorCode.FILE_NOT_FOUND:
                return
            raise e

    def remove_file(self, path: list[str], file_name: str, file_version: Optional[int]=None):
        logging.debug('Remove file [{}] version [{}]'.format(str_path(path + [file_name]), file_version))

        file_metadata = self.db().get_file_version_metadata(path, file_name, file_version)
        logging.debug('Retrieved file metadata')

        file_id = file_metadata.local_id
        transfer_status = file_metadata.local_transfer_status

        logging.debug('File local transfer status [{}]'.format(transfer_status.name))
        if transfer_status != FileTransferStatus.SYNCED_DATA:
            raise FileDeleteError('Cannot remove file [{}]. File is not fully uploaded'.format(file_id), FileServerErrorCode.FILE_NOT_REMOVABLE)

        if file_version is not None:
            raise FileDeleteError('Not implemented!')
        else:
            self.db().remove_file(path, file_name, remove_file_cb=self.remove_file_check_readers_cb)
            logging.debug('Removed file')

            # TODO: Done asynchronously by the async controller.
            self.async_controller().remove_orphaned_files()

    def list_directory(self, path: list[str], show_hidden: bool=False):
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
    
    def get_file_metadata(self, path: list[str], file_name: str, show_hidden: bool=False):
        logging.debug('Get file metadata [{}]'.format(str_path(path + [file_name])))
        conn = self.db_conn_mgr().db_connect()
        try:
            logging.debug('Acquired database connection')
            file_dao = self._dao_factory.file_dao(conn)
            # TODO: Return metadata for multiple file versions.
            file_metadata = file_dao.get_file_version_metadata(path, file_name)
            logging.debug('Retrieved file metadata [{}]'.format(str_path(path + [file_name])))
            # TODO: Retrieve mime-type.
            return {
                "mime-type": "application/octet-stream",
                "versions":[
                    {
                        "version": file_metadata.version,
                        "file-size": file_metadata.file_size,
                        "size-on-disk": file_metadata.size_on_disk,
                        "total-chunks": file_metadata.total_chunks,
                        "local-file-id": file_metadata.local_id,
                        "remote-file-id": file_metadata.remote_id,
                        "local-transfer-status": file_metadata.local_transfer_status.name,
                        "remote-transfer-status": file_metadata.remote_transfer_status.name
                    }
                ]
            }
        finally:
            self.db_conn_mgr().db_close(conn)
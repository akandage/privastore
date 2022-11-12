from ..controller import Controller
from .db.dao_factory import DAOFactory
from ..db.db_conn_mgr import DbConnectionManager
from ..error import FileServerErrorCode, RemoteFileError
from ..file_cache import FileCache
import logging
from ..session_mgr import SessionManager
from ..util.file import str_mem_size
from typing import Optional

class RemoteServerController(Controller):

    def __init__(self, dao_factory: DAOFactory, db_conn_mgr: DbConnectionManager, session_mgr: SessionManager, store: FileCache):
        super().__init__(db_conn_mgr, session_mgr, store)
        self._dao_factory = dao_factory
    
    def dao_factory(self) -> DAOFactory:
        return self._dao_factory
    
    def create_file(self, remote_id: str, file_size: int) -> None:
        logging.debug('Create remote file [{}] file-size [{}]'.format(remote_id, str_mem_size(file_size)))
        conn = self.db_conn_mgr().db_connect()
        
        try:
            logging.debug('Acquired database connection')

            file_dao = self.dao_factory().file_dao(conn)
            file_dao.create_file(remote_id, file_size)
            self.store().create_empty_file(remote_id, file_size, removable=True)

            logging.debug('Created remote file [{}]'.format(remote_id))
        finally:
            self.db_conn_mgr().db_close(conn)
    
    def append_to_file(self, remote_id: str, chunk_num: int, chunk: bytes) -> None:
        logging.debug('Append to remote file [{}] chunk-size [{}]'.format(remote_id, str_mem_size(len(chunk))))
        conn = self.db_conn_mgr().db_connect()
        
        try:
            logging.debug('Acquired database connection')

            file_dao = self.dao_factory().file_dao(conn)
            file_metadata = file_dao.get_file_metadata(remote_id)
            if file_metadata.is_committed:
                raise RemoteFileError('Cannot append chunk to committed remote file [{}]'.format(remote_id), FileServerErrorCode.FILE_IS_COMMITTED)

            file = self.store().append_file(remote_id)

            try:               
                next_chunk_num = file.total_chunks()+1
                if chunk_num != next_chunk_num:
                    raise RemoteFileError('Cannot write file [{}] chunk [{}]. Next chunk is [{}]'.format(remote_id, chunk_num, next_chunk_num), FileServerErrorCode.INVALID_CHUNK_NUM)
                
                file_alloc_size = file_metadata.file_size
                file_size = file.size_on_disk() + len(chunk)
                if file_size > file_alloc_size:
                    raise RemoteFileError('Cannot write file [{}] chunk [{}]. File size [{}] would exceed allocated size [{}]'.format(remote_id, chunk_num, file_size, file_alloc_size), FileServerErrorCode.FILE_TOO_LARGE)

                file.append_chunk(chunk)
                logging.debug('Appended chunk')
                file_dao.file_modified(remote_id)
                logging.debug('Updated file modified timestamp')
            finally:
                self.store().close_file(file, writable=True)

            logging.debug('Appended to remote file [{}]'.format(remote_id))
        finally:
            self.db_conn_mgr().db_close(conn)

    def read_from_file(self, remote_id: str, chunk_num: int) -> bytes:
        logging.debug('Read chunk [{}] from remote file [{}]'.format(chunk_num, remote_id))
        conn = self.db_conn_mgr().db_connect()
        
        try:
            logging.debug('Acquired database connection')

            file_dao = self.dao_factory().file_dao(conn)
            file_metadata = file_dao.get_file_metadata(remote_id)
            if not file_metadata.is_committed:
                raise RemoteFileError('Cannot read from uncommitted remote file [{}]'.format(remote_id), FileServerErrorCode.FILE_IS_UNCOMMITTED)

            file = self.store().read_file(remote_id)

            try:
                # Seek just before the chunk to read.
                file.seek_chunk(chunk_num-1)
                chunk = file.read_chunk()
                if len(chunk) == 0:
                    raise RemoteFileError('Chunk [{}] not found'.format(chunk_num), FileServerErrorCode.INVALID_CHUNK_NUM)
                logging.debug('Read chunk size [{}]'.format(len(chunk)))

                return chunk
            finally:
                self.store().close_file(file)
        finally:
            self.db_conn_mgr().db_close(conn)

    def commit_file(self, epoch_no: int, remote_id: str) -> None:
        logging.debug('Commit remote file [{}] epoch [{}]'.format(remote_id, epoch_no))
        conn = self.db_conn_mgr().db_connect()
        
        try:
            logging.debug('Acquired database connection')

            epoch_dao = self.dao_factory().epoch_dao(conn)
            file_dao = self.dao_factory().file_dao(conn)

            file_metadata = file_dao.get_file_metadata(remote_id)
            if file_metadata.is_committed:
                logging.debug('File [{}] already committed'.format(remote_id))
                return

            file = self.store().append_file(remote_id)
            committed = False

            try:
                epoch_dao.check_valid_epoch(epoch_no)
                file_alloc_size = file_metadata.file_size
                file_size = file.size_on_disk()
                if file_size < file_alloc_size:
                    raise RemoteFileError('Cannot commit remote file [{}]. File size [{}] < allocated size [{}]'.format(remote_id, str_mem_size(file_size), str_mem_size(file_alloc_size)), FileServerErrorCode.FILE_TOO_SMALL)
                self.store().close_file(file, writable=False)
                committed = True
                file_dao.commit_file(epoch_no, remote_id)
            finally:
                if not committed:
                    self.store().close_file(file, writable=True)

            
            logging.debug('Committed remote file [{}]'.format(remote_id))
        finally:
            self.db_conn_mgr().db_close(conn)

    def get_file_metadata(self, remote_id: str) -> dict:
        logging.debug('Get remote file metadata [{}]'.format(remote_id))
        conn = self.db_conn_mgr().db_connect()
        
        try:
            logging.debug('Acquired database connection')

            file_dao = self.dao_factory().file_dao(conn)
            file_metadata_db = file_dao.get_file_metadata(remote_id)
            file_metadata_store = self.store().file_metadata(remote_id)

            logging.debug('Retrieved remote file metadata [{}]'.format(remote_id))

            return {
                'remote-file-id': remote_id,
                'file-size': file_metadata_db.file_size,
                'file-chunks': file_metadata_store.file_chunks,
                'is-committed': file_metadata_db.is_committed,
                'created-epoch-no': file_metadata_db.created_epoch,
                'removed-epoch-no': file_metadata_db.removed_epoch
            }
        finally:
            self.db_conn_mgr().db_close(conn)
    
    def remove_file(self, epoch_no: int, remote_id: str) -> None:
        logging.debug('Remove remote file [{}] epoch [{}]'.format(remote_id, epoch_no))
        conn = self.db_conn_mgr().db_connect()
        
        try:
            logging.debug('Acquired database connection')

            file_dao = self.dao_factory().file_dao(conn)
            file_dao.remove_file(epoch_no, remote_id, remove_file_cb=lambda remote_id: self.store().remove_file_by_id(remote_id, ignore_removable=True))

            logging.debug('Removed remote file [{}]'.format(remote_id))
        finally:
            self.db_conn_mgr().db_close(conn)

    def end_epoch(self, epoch_no: int, marker_id: Optional[str] = None) -> None:
        logging.debug('End epoch [{}] marker file id [{}]'.format(epoch_no, marker_id))
        conn = self.db_conn_mgr().db_connect()
        
        try:
            logging.debug('Acquired database connection')

            epoch_dao = self.dao_factory().epoch_dao(conn)
            epoch_dao.end_epoch(epoch_no, marker_id, remove_file_cb=lambda remote_id: self.store().remove_file_by_id(remote_id, ignore_removable=True))

            logging.debug('Epoch [{}] ended'.format(epoch_no))
        finally:
            self.db_conn_mgr().db_close(conn)
        

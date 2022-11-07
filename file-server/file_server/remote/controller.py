from ..controller import Controller
from .db.dao_factory import DAOFactory
from ..db.db_conn_mgr import DbConnectionManager
from ..file_cache import FileCache
import logging
from ..session_mgr import SessionManager
from ..util.file import str_mem_size

class RemoteServerController(Controller):

    def __init__(self, dao_factory: DAOFactory, db_conn_mgr: DbConnectionManager, session_mgr: SessionManager, store: FileCache):
        super().__init__(db_conn_mgr, session_mgr, store)
        self._dao_factory = dao_factory
    
    def dao_factory(self) -> DAOFactory:
        return self._dao_factory
    
    def create_file(self, epoch_no: int, remote_id: str, file_size: int) -> None:
        logging.debug('Create remote file [{}] epoch [{}] file-size [{}]'.format(remote_id, epoch_no, str_mem_size(file_size)))
        conn = self.db_conn_mgr().db_connect()
        
        try:
            logging.debug('Acquired database connection')

            file_dao = self.dao_factory().file_dao(conn)
            file_dao.create_file(epoch_no, remote_id, file_size)
            self.store().touch_file(remote_id, file_size)

            logging.debug('Created remote file [{}]'.format(remote_id))
        finally:
            self.db_conn_mgr().db_close(conn)
    
    def commit_file(self, epoch_no: int, remote_id: str) -> None:
        logging.debug('Commit remote file [{}] epoch [{}]'.format(remote_id, epoch_no))
        conn = self.db_conn_mgr().db_connect()
        
        try:
            logging.debug('Acquired database connection')

            file_dao = self.dao_factory().file_dao(conn)
            file_dao.commit_file(epoch_no, remote_id)

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
    
    def end_epoch(self, epoch_no: int, marker_id: str) -> None:
        logging.debug('End epoch [{}]'.format(epoch_no))
        conn = self.db_conn_mgr().db_connect()
        
        try:
            logging.debug('Acquired database connection')

            epoch_dao = self.dao_factory().epoch_dao(conn)
            epoch_dao.end_epoch(epoch_no, marker_id)

            logging.debug('Epoch [{}] ended'.format(epoch_no))
        finally:
            self.db_conn_mgr().db_close(conn)
        

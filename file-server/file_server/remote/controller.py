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

            logging.debug('Created remote file [{}]'.format(remote_id))
        finally:
            self.db_conn_mgr().db_close(conn)
        

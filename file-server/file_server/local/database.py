from .db.dao_factory import DAOFactory
from ..db.db_conn_mgr import DbConnectionManager
from .db.file_dao import FileVersionMetadata
from .file_transfer_status import FileTransferStatus
from typing import Callable, Optional

class DbWrapper(object):
    '''
        Wrapper for database calls.

        TODO: Maybe refactor this away somehow.
    '''

    def __init__(self, dao_factory: DAOFactory, db_conn_mgr: DbConnectionManager):
        self._dao_factory = dao_factory
        self._db_conn_mgr = db_conn_mgr

    def dao_factory(self):
        return self._dao_factory

    def db_conn_mgr(self):
        return self._db_conn_mgr

    def get_file_metadata(self, local_id: str) -> 'FileVersionMetadata':
        conn = self.db_conn_mgr().db_connect()
        try:
            return self.dao_factory().file_dao(conn).get_file_version_metadata(local_id=local_id)
        finally:
            self.db_conn_mgr().db_close(conn)

    def update_file_remote(self, local_id: str, remote_id: Optional[str]=None, transfer_status: FileTransferStatus=FileTransferStatus.NONE, transferred_chunks: int=0):
        conn = self.db_conn_mgr().db_connect()
        try:
            self.dao_factory().file_dao(conn).update_file_remote(local_id, remote_id, transfer_status, transferred_chunks)
        finally:
            self.db_conn_mgr().db_close(conn)

    def update_file_download(self, local_id: str, transferred_chunks: int=0):
        conn = self.db_conn_mgr().db_connect()
        try:
            self.dao_factory().file_dao(conn).update_file_download(local_id, transferred_chunks)
        finally:
            self.db_conn_mgr().db_close(conn)
    
    def remove_file_version(self, local_id: str):
        conn = self.db_conn_mgr().db_connect()
        try:
            self.dao_factory().file_dao(conn).remove_file_version(local_id)
        finally:
            self.db_conn_mgr().db_close(conn)
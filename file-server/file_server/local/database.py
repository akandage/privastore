from .db.dao_factory import DAOFactory
from ..db.db_conn_mgr import DbConnectionManager
from .db.file_dao import FileVersionMetadata
from .file_transfer_status import FileTransferStatus
from .file_type import FileType
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

    def create_file(self, path: list[str], file_name: str, file_type=FileType.BINARY_DATA, is_hidden: bool=False):
        conn = self.db_conn_mgr().db_connect()
        try:
            return self.dao_factory().directory_dao(conn).create_file(path, file_name, file_type, is_hidden)
        finally:
            self.db_conn_mgr().db_close(conn)

    def get_file_version_metadata(self, path: Optional[list[str]]=None, file_name: Optional[str]=None, version:Optional[int]=None, local_id: Optional[str]=None) -> 'FileVersionMetadata':
        conn = self.db_conn_mgr().db_connect()
        try:
            return self.dao_factory().file_dao(conn).get_file_version_metadata(path, file_name, version, local_id)
        finally:
            self.db_conn_mgr().db_close(conn)

    def get_local_file_metadata(self, local_id: str) -> 'FileVersionMetadata':
        return self.get_file_version_metadata(local_id=local_id)

    def update_file_local(self, path: list[str], file_name: str, version: int, local_id: str, key_id: str, file_size: int, size_on_disk: int, total_chunks: int, transfer_status: FileTransferStatus=FileTransferStatus.NONE) -> None:
        conn = self.db_conn_mgr().db_connect()
        try:
            self.dao_factory().file_dao(conn).update_file_local(path, file_name, version, local_id, key_id, file_size, size_on_disk, total_chunks, transfer_status)
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
    
    def remove_file(self, path: list[str], file_name: str, delete: bool=False, remove_file_cb: Optional[Callable[[str, str], None]]=None, is_hidden: bool=False):
        conn = self.db_conn_mgr().db_connect()
        try:
            self.dao_factory().directory_dao(conn).remove_file(path, file_name, delete, remove_file_cb, is_hidden)
        finally:
            self.db_conn_mgr().db_close(conn)

    def remove_file_version(self, local_id: str):
        conn = self.db_conn_mgr().db_connect()
        try:
            self.dao_factory().file_dao(conn).remove_file_version(local_id)
        finally:
            self.db_conn_mgr().db_close(conn)
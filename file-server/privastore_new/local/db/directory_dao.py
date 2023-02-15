from typing import Optional

from ...db.conn import DbConnection
from ...db.dao import DataAccessObject
from ...directory import Directory, DirectoryEntry
from ...error import NotImplementedError

class DirectoryDAO(DataAccessObject):

    def __init__(self, conn: DbConnection):
        super().__init__(conn)
    
    def get_root_directory(self, owner: str) -> Directory:
        raise NotImplementedError()

    def path_to_directory(self, dir_uid: str, owner: str) -> list[str]:
        raise NotImplementedError()

    def create_directory(self, parent_uid: str, name: str, owner: str) -> Directory:
        raise NotImplementedError()
    
    def list_directory(self, dir_uid: str, owner: str, limit: Optional[int]=None, offset: Optional[int]=None, sort: Optional[str]=None) -> list[DirectoryEntry]:
        raise NotImplementedError()
    
    def remove_directory(self, dir_uid: str, owner: str) -> None:
        raise NotImplementedError()
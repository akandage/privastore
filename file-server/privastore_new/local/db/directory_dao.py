from typing import Optional

from ...db.conn import DbConnection
from ...db.dao import DataAccessObject
from ...directory import Directory
from ...error import NotImplementedError

class DirectoryDAO(DataAccessObject):

    def __init__(self, conn: DbConnection):
        super().__init__(conn)
    
    def create_directory(self, path: list[str], name: str, owner: str) -> None:
        raise NotImplementedError()
    
    def list_directory(self, path: list[str], owner: str, limit: Optional[int]=None, offset: Optional[int]=None, sort: Optional[str]=None) -> list[Directory]:
        raise NotImplementedError()
    
    def remove_directory(self, path: list[str], name: str, owner: str) -> None:
        raise NotImplementedError()
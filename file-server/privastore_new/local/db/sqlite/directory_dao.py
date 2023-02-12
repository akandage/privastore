import logging
from typing import Optional

from ....crypto.util import hash_user_password
from ....db.conn import SqliteConnection
from ..directory_dao import DirectoryDAO
from ....directory import Directory
from ....error import DirectoryError

class SqliteDirectoryDAO(DirectoryDAO):

    def __init__(self, conn: SqliteConnection):
        super().__init__(conn)
    
    def conn(self) -> SqliteConnection:
        return super().conn()
    
    def get_root_directory(self, owner: str) -> int:
        cur = self.conn().cursor()
        try:
            res = cur.execute('SELECT id FROM ps_directory WHERE name = ? AND owner = ?', ('/', owner))
            if res is None:
                raise DirectoryError('User [{}] root directory not found!'.format(owner))
            return res[0]
        finally:
            cur.close()

    def get_directory_id(self, path: list[str], owner: str) -> int:
        curr_id = self.get_root_directory(owner)
        traversed = []
        for name in path:
            cur = self.conn().cursor()
            try:
                res = cur.execute('''
                    SELECT D.id 
                    FROM ps_directory AS D INNER JOIN ps_directory_link AS L ON D.id = L.child_id
                    WHERE L.parent_id = ? AND D.name = ? AND D.owner = ?
                ''', (curr_id, name, owner))
                if res is None:
                    raise DirectoryError('Directory [{}] not found in path [{}]'.format(name, traversed), DirectoryError.INVALID_PATH)
                traversed.append(name)
                curr_id = res[0]
            finally:
                cur.close()
        return curr_id

    def create_directory(self, path: list[str], name: str, owner: str) -> None:
        return super().create_directory(path, name, owner)
    
    def list_directory(self, path: list[str], owner: str, limit: Optional[int] = None, offset: Optional[int] = None, sort: Optional[str] = None) -> list[Directory]:
        return super().list_directory(path, owner, limit, offset, sort)
    
    def remove_directory(self, path: list[str], name: str, owner: str) -> None:
        return super().remove_directory(path, name, owner)
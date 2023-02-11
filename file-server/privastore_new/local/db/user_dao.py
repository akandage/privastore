from ...db.conn import DbConnection
from ...db.dao import DataAccessObject
from ...error import NotImplementedError

class UserDAO(DataAccessObject):

    def __init__(self, conn: DbConnection):
        super().__init__(conn)
    
    def login_user(self, username: str, password: str) -> None:
        raise NotImplementedError()
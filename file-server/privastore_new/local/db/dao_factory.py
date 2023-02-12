from ...db.conn import DbConnection
from ...error import NotImplementedError
from .directory_dao import DirectoryDAO
from .user_dao import UserDAO

class DAOFactory(object):

    def __init__(self):
        super().__init__()
    
    def directory_dao(self, conn: DbConnection) -> DirectoryDAO:
        raise NotImplementedError()

    def user_dao(self, conn: DbConnection) -> UserDAO:
        raise NotImplementedError()
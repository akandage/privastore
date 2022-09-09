from ..dao_factory import DAOFactory
from .directory_dao import SqliteDirectoryDAO
from .user_dao import SqliteUserDAO

class SqliteDAOFactory(DAOFactory):

    def __init__(self):
        super().__init__()
    
    def user_dao(self, conn):
        return SqliteUserDAO(conn)
    
    def directory_dao(self, conn):
        return SqliteDirectoryDAO(conn)
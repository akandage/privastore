from ..dao_factory import DAOFactory
from .user_dao import SqliteUserDAO

class SqliteDAOFactory(DAOFactory):

    def __init__(self):
        super().__init__()
    
    def user_dao(self, conn):
        return SqliteUserDAO(conn)
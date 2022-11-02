from ..dao_factory import DAOFactory
from .file_dao import SqliteFileDAO

class SqliteDAOFactory(DAOFactory):

    def __init__(self):
        super().__init__()
    
    def file_dao(self, conn):
        return SqliteFileDAO(conn)
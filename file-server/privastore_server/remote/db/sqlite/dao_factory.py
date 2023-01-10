from ..dao_factory import DAOFactory
from .epoch_dao import SqliteEpochDAO
from .file_dao import SqliteFileDAO

class SqliteDAOFactory(DAOFactory):

    def __init__(self):
        super().__init__()
    
    def epoch_dao(self, conn):
        return SqliteEpochDAO(conn)

    def file_dao(self, conn):
        return SqliteFileDAO(conn)
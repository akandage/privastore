from ....db.conn import SqliteConnection
from ....db.file_data_dao import FileDataDAOFactory
from ..dao_factory import DAOFactory
from .directory_dao import SqliteDirectoryDAO
from .file_data_dao import SqliteFileDataDAO
from .log_dao import SqliteLogDAO
from .user_dao import SqliteUserDAO

class SqliteDAOFactory(DAOFactory, FileDataDAOFactory):

    def __init__(self):
        super().__init__()
    
    def directory_dao(self, conn: SqliteConnection) -> SqliteDirectoryDAO:
        return SqliteDirectoryDAO(conn)

    def file_data_dao(self, conn: SqliteConnection) -> SqliteFileDataDAO:
        return SqliteFileDataDAO(conn)

    def log_dao(self, conn: SqliteConnection) -> SqliteLogDAO:
        return SqliteLogDAO(conn)

    def user_dao(self, conn: SqliteConnection) -> SqliteUserDAO:
        return SqliteUserDAO(conn)
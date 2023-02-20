from ...db.conn import DbConnection
from ...error import NotImplementedError
from .directory_dao import DirectoryDAO
from ...db.file_data_dao import FileDataDAOFactory
from ...db.log_dao import LogDAO, LogDAOFactory
from .user_dao import UserDAO

class DAOFactory(FileDataDAOFactory, LogDAOFactory):

    def __init__(self):
        super().__init__()
    
    def directory_dao(self, conn: DbConnection) -> DirectoryDAO:
        raise NotImplementedError()

    def user_dao(self, conn: DbConnection) -> UserDAO:
        raise NotImplementedError()
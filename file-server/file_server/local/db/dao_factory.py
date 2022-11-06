from .directory_dao import DirectoryDAO
from .file_dao import FileDAO
from .user_dao import UserDAO
import sqlite3

class DAOFactory(object):

    def __init__(self):
        super().__init__()

    def user_dao(self, conn: sqlite3.Connection) -> UserDAO:
        raise Exception('Not implemented!')
    
    def file_dao(self, conn: sqlite3.Connection) -> FileDAO:
        raise Exception('Not implemented!')
    
    def directory_dao(self, conn: sqlite3.Connection) -> DirectoryDAO:
        raise Exception('Not implemented!')
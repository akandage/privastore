from .directory_dao import DirectoryDAO
from .file_dao import FileDAO
from .key_dao import KeyDAO
from .log_dao import LogDAO
from .remote_dao import RemoteDAO
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
    
    def key_dao(self, conn: sqlite3.Connection) -> KeyDAO:
        raise Exception('Not implemented!')

    def log_dao(self, conn: sqlite3.Connection) -> LogDAO:
        raise Exception('Not implemented!')

    def remote_dao(self, conn: sqlite3.Connection) -> RemoteDAO:
        raise Exception('Not implemented!')
from .epoch_dao import EpochDAO
from .file_dao import FileDAO
import sqlite3

class DAOFactory(object):

    def __init__(self):
        super().__init__()

    def epoch_dao(self, conn: sqlite3.Connection) -> EpochDAO:
        raise Exception('Not implemented!')

    def file_dao(self, conn: sqlite3.Connection) -> FileDAO:
        raise Exception('Not implemented!')
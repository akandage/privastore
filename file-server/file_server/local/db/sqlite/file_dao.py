from ..file_dao import FileDAO
from ....error import FileError
import logging

class SqliteFileDAO(FileDAO):

    def __init__(self, conn):
        super().__init__(conn)
import logging
from ....error import EpochError, FileServerErrorCode
from ..log_dao import LogDAO

class SqliteLogDAO(LogDAO):

    def __init__(self, conn):
        super().__init__(conn)
from ..epoch_dao import EpochDAO
from ....error import EpochError
import logging
import time

class SqliteEpochDAO(EpochDAO):

    def __init__(self, conn):
        super().__init__(conn)
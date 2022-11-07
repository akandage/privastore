from ...db.dao import DataAccessObject
import sqlite3

class EpochDAO(DataAccessObject):

    def __init__(self, conn):
        super().__init__(conn)
    
    def end_epoch(self, epoch_no: int, marker_id: str) -> None:
        raise Exception('Not implemented!')
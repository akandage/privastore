from ...db.dao import DataAccessObject
import sqlite3
from typing import Optional

class EpochDAO(DataAccessObject):

    def __init__(self, conn):
        super().__init__(conn)
    
    def end_epoch(self, epoch_no: int, marker_id: Optional[str] = None) -> None:
        raise Exception('Not implemented!')
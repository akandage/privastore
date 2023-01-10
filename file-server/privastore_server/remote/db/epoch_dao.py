from ...db.dao import DataAccessObject
import sqlite3
from typing import Callable, Optional

class EpochDAO(DataAccessObject):

    def __init__(self, conn):
        super().__init__(conn)
    
    def check_valid_epoch(self, epoch_no: int) -> None:
        raise Exception('Not implemented!')

    def current_epoch(self) -> int:
        raise Exception('Not implemented!')

    def end_epoch(self, epoch_no: int, marker_id: Optional[str] = None, remove_file_cb: Optional[Callable[[str], None]] = None) -> None:
        raise Exception('Not implemented!')
from ...db.dao import DataAccessObject
from ..log_entry_type import LogEntryType
from typing import Optional

class LogDAO(DataAccessObject):

    def __init__(self, conn):
        super().__init__(conn)
    
    def get_first_epoch(self) -> int:
        raise Exception('Not implemented!')

    def get_current_epoch(self) -> int:
        raise Exception('Not implemented!')
    
    def get_last_epoch(self) -> int:
        raise Exception('Not implemented!')
    
    def add_log_entry(self, type: LogEntryType, entry: dict) -> None:
        raise Exception('Not implemented!')
    
    def get_log_entry(self, seq_no: int) -> Optional[dict]:
        raise Exception('Not implemented!')
    
    def get_epoch_log_entries(self, epoch_no: int) -> list[dict]:
        raise Exception('Not implemented!')
    
    def truncate_log(self, low_watermark: int) -> None:
        raise Exception('Not implemented!')
    
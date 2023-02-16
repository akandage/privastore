from typing import Optional

from ..log.log_entry_type import LogEntryType
from ...error import NotImplementedError

class LogDAO(object):

    def __init__(self):
        super().__init__()
    
    def start_epoch(self) -> int:
        raise NotImplementedError()
    
    def get_first_epoch(self) -> int:
        raise NotImplementedError()

    def get_last_epoch(self) -> int:
        raise NotImplementedError()
    
    def create_log_entry(self, type: LogEntryType, entry: dict) -> int:
        raise NotImplementedError()
    
    def truncate_to_epoch(self, epoch_no: int) -> None:
        raise NotImplementedError()
from typing import Optional

from ..log.log_entry import LogEntry
from ...error import NotImplementedError

class LogDAO(object):

    def __init__(self):
        super().__init__()
    
    def get_first_seq_no(self) -> int:
        raise NotImplementedError()

    def get_last_seq_no(self) -> int:
        raise NotImplementedError()
    
    def create_log_entry(self, entry: LogEntry) -> int:
        raise NotImplementedError()
    
    def get_log_entry(self, seq_no: int) -> LogEntry:
        raise NotImplementedError()

    def truncate_log(self, seq_no: int) -> None:
        raise NotImplementedError()
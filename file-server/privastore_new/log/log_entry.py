from typing import Optional

from .log_entry_type import LogEntryType

class LogEntry(object):

    def __init__(self, type: LogEntryType, entry: dict, seq_no: Optional[int]=None):
        super().__init__()
        self._type = type
        self._entry = entry
        self._seq_no = seq_no
    
    def type(self) -> LogEntryType:
        return self._type
    
    def entry(self) -> dict:
        return self._entry
    
    def seq_no(self) -> Optional[int]:
        return self._seq_no
    
    def to_dict(self):
        return {
            'seq-no': self.seq_no(),
            'entry-type': self.type().name,
            'entry': self.entry()
        }
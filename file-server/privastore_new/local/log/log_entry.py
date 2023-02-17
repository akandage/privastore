from .log_entry_type import LogEntryType

class LogEntry(object):

    def __init__(self, type: LogEntryType, entry: dict):
        super().__init__()
        self._type = type
        self._entry = entry
    
    def type(self) -> LogEntryType:
        return self._type
    
    def entry(self) -> dict:
        return self._entry
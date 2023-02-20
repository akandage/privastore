from ..error import NotImplementedError
from .log_entry import LogEntry

class LogObserver(object):

    def __init__(self):
        super().__init__()
    
    def on_log_entry(self, entry: LogEntry) -> None:
        raise NotImplementedError()
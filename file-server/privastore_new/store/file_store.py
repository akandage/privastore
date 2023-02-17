from typing import Optional

from ..error import NotImplementedError
from .file_handle import FileHandle

class FileStore(object):

    def __init__(self):
        pass

    def open_for_reading(self, uid: str, blocking: Optional[bool]=False) -> FileHandle:
        raise NotImplementedError()

    def open_for_writing(self) -> FileHandle:
        raise NotImplementedError()
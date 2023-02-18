from typing import Optional

from ...directory import Directory, DirectoryEntry
from ...file import File
from ...error import NotImplementedError

class DirectoryDAO(object):

    def __init__(self):
        super().__init__()
    
    def get_root_directory(self, owner: str) -> Directory:
        raise NotImplementedError()

    def path_to_directory(self, dir_uid: str, owner: str) -> list[str]:
        raise NotImplementedError()

    def create_directory(self, parent_uid: str, name: str, owner: str) -> Directory:
        raise NotImplementedError()
    
    def create_file(self, parent_uid: str, name: str, mime_type: str, owner: str) -> File:
        raise NotImplementedError()

    def file_exists(self, parent_uid: str, name: str, owner: str) -> bool:
        raise NotImplementedError()

    def list_directory(self, dir_uid: str, owner: str, limit: Optional[int]=None, offset: Optional[int]=None, sort: Optional[str]=None) -> list[DirectoryEntry]:
        raise NotImplementedError()
    
    def remove_file(self, file_uid: str, owner: str) -> None:
        raise NotImplementedError()

    def remove_directory(self, dir_uid: str, owner: str) -> None:
        raise NotImplementedError()
from collections import namedtuple
from ...db.dao import DataAccessObject
from typing import Callable, Optional

RemoteFileMetadata = namedtuple('RemoteFileMetadata', ['is_committed', 'created_timestamp', 'modified_timestamp', 'created_epoch', 'removed_epoch'])

class FileDAO(DataAccessObject):

    def __init__(self, conn):
        super().__init__(conn)
    
    def create_file(self, remote_id: str, file_size: int) -> None:
        raise Exception('Not implemented!')
    
    def get_file_metadata(self, remote_id: str) -> 'RemoteFileMetadata':
        raise Exception('Not implemented!')

    def file_modified(self, remote_id: str) -> None:
        raise Exception('Not implemented!')

    def commit_file(self, epoch_no: int, remote_id: str) -> None:
        raise Exception('Not implemented!')

    def remove_file(self, epoch_no: int, remote_id: str, remove_file_cb: Optional[Callable[[str], None]] = None) -> None:
        raise Exception('Not implemented!')
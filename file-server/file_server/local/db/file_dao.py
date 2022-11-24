from collections import namedtuple
from ...db.dao import DataAccessObject
from ..file_transfer_status import FileTransferStatus
from typing import Optional

FileVersionMetadata = namedtuple('FileVersionMetadata', ['file_type', 'version', 'local_id', 'remote_id', 'file_size', 'size_on_disk', 'total_chunks', 'local_transfer_status', 'remote_transfer_status'])

class FileDAO(DataAccessObject):

    def __init__(self, conn):
        super().__init__(conn)
    
    def get_file_version_metadata(self, path: Optional[list[str]]=None, file_name: Optional[str]=None, version:Optional[int]=None, local_id: Optional[str]=None) -> 'FileVersionMetadata':
        raise Exception('Not implemented!')
    
    def update_file_local(self, path: list[str], file_name: str, version: int, local_id: str, file_size: int, size_on_disk: int, total_chunks: int, transfer_status: FileTransferStatus=FileTransferStatus.NONE) -> None:
        raise Exception('Not implemented!')

    def update_file_remote(self, local_id: str, remote_id: Optional[str]=None, transfer_status: FileTransferStatus=FileTransferStatus.NONE) -> None:
        raise Exception('Not implemented!')
    
    def update_file_transfer_status(self, path: list[str], file_name: str, version: int, local_transfer_status: Optional[FileTransferStatus]=None, remote_transfer_status: Optional[FileTransferStatus]=None):
        raise Exception('Not implemented!')
    
    def remove_file_version(self, local_id: str) -> None:
        raise Exception('Not implemented!')
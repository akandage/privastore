from collections import namedtuple
from ...db.dao import DataAccessObject
from ..file_transfer_status import FileTransferStatus
from typing import Optional, Type

FileVersionMetadata = namedtuple('FileVersionMetadata', ['file_type', 'version', 'local_id', 'remote_id', 'file_size', 'size_on_disk', 'total_chunks', 'transfer_status'])

class FileDAO(DataAccessObject):

    def __init__(self, conn):
        super().__init__(conn)
    
    def get_file_version_metadata(self, path: str, file_name: str, version:Optional[str]=None) -> tuple:
        raise Exception('Not implemented!')
    
    def update_file_local(self, path: str, file_name: str, version: int, local_id: str, file_size: int, size_on_disk: int, total_chunks: int, transfer_status: FileTransferStatus=FileTransferStatus.RECEIVED) -> None:
        raise Exception('Not implemented!')

    def update_file_remote(self, path: str, file_name: str, version: int, remote_id: str, transfer_status: FileTransferStatus) -> None:
        raise Exception('Not implemented!')
    
    def update_file_transfer_status(self, path: str, file_name: str, version: int, transfer_status: FileTransferStatus) -> None:
        raise Exception('Not implemented!')
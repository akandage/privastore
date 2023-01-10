from collections import namedtuple
from ...db.dao import DataAccessObject
from ..file_transfer_status import FileTransferStatus
from typing import Optional

FileMetadata = namedtuple('FileVersionMetadata', ['local_id', 'remote_id', 'key_id', 'file_size', 'size_on_disk', 'total_chunks', 'uploaded_chunks', 'downloaded_chunks', 'local_transfer_status', 'remote_transfer_status'])
FileVersionMetadata = namedtuple('FileVersionMetadata', ['file_id', 'file_type', 'version', 'local_id', 'remote_id', 'key_id', 'file_size', 'size_on_disk', 'total_chunks', 'uploaded_chunks', 'downloaded_chunks', 'local_transfer_status', 'remote_transfer_status'])

class FileDAO(DataAccessObject):

    def __init__(self, conn):
        super().__init__(conn)
    
    def get_file_version_metadata(self, path: list[str], file_name: str, version: Optional[int]=None) -> 'FileVersionMetadata':
        raise Exception('Not implemented!')
    
    def get_file_metadata(self, local_id: str) -> 'FileMetadata':
        raise Exception('Not implemented!')

    def list_unsynced_files(self, local: bool, remote: bool) -> list['FileVersionMetadata']:
        raise Exception('Not implemented!')

    def list_orphaned_file_data(self) -> list['FileMetadata']:
        raise Exception('Not implemented!')

    def update_file_local(self, path: list[str], file_name: str, version: int, local_id: str, key_id: str, file_size: int, size_on_disk: int, total_chunks: int, transfer_status: FileTransferStatus=FileTransferStatus.NONE) -> None:
        raise Exception('Not implemented!')

    def update_file_remote(self, local_id: str, remote_id: Optional[str]=None, transfer_status: FileTransferStatus=FileTransferStatus.NONE, transferred_chunks: int=0) -> None:
        raise Exception('Not implemented!')
    
    def update_file_download(self, local_id: str, transferred_chunks: int=0) -> None:
        raise Exception('Not implemented!')

    def update_file_transfer_status(self, path: list[str], file_name: str, version: int, local_transfer_status: Optional[FileTransferStatus]=None, remote_transfer_status: Optional[FileTransferStatus]=None):
        raise Exception('Not implemented!')

    def remove_file_data(self, local_id: str) -> None:
        raise Exception('Not implemented!')
    
    def remove_file_version(self, file_id: int, version: int) -> None:
        raise Exception('Not implemented!')
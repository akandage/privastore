from ...db.dao import DataAccessObject
from ..file_transfer_status import FileTransferStatus

class FileDAO(DataAccessObject):

    def __init__(self, conn):
        super().__init__(conn)
    
    def get_file_version_metadata(self, path, file_name, version=None):
        raise Exception('Not implemented!')
    
    def update_file_local(self, path, file_name, version, local_id, file_size, size_on_disk, total_chunks, transfer_status=FileTransferStatus.RECEIVED):
        raise Exception('Not implemented!')

    def update_file_remote(self, path, file_name, version, remote_id, transfer_status):
        raise Exception('Not implemented!')
    
    def update_file_transfer_status(self, path, file_name, version, transfer_status):
        raise Exception('Not implemented!')
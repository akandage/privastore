from ...db.dao import DataAccessObject

class FileDAO(DataAccessObject):

    def __init__(self, conn):
        super().__init__(conn)
    
    def create_file(self, epoch_no: int, remote_id: str, file_size: int) -> None:
        raise Exception('Not implemented!')
    
    def get_file_metadata(self, remote_id: str) -> None:
        raise Exception('Not implemented!')

    def file_modified(self, epoch_no: int, remote_id: str) -> None:
        raise Exception('Not implemented!')

    def commit_file(self, epoch_no: int, remote_id: str) -> None:
        raise Exception('Not implemented!')

    def remove_file(self, epoch_no: int, remote_id: str) -> None:
        raise Exception('Not implemented!')
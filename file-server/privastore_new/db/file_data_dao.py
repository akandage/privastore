from .conn import DbConnection
from ..error import NotImplementedError
from ..file import FileData

class FileDataDAO(object):

    def __init__(self):
        super().__init__()
    
    def create_file_data(self, uid: str) -> int:
        raise NotImplementedError()
    
    def get_file_data(self, uid: str) -> FileData:
        raise NotImplementedError()

    def append_chunk(self, fd_id: int, chunk_data: bytes) -> int:
        raise NotImplementedError()
    
    def read_chunk(self, fd_id: int, chunk_id: int) -> bytes:
        raise NotImplementedError()
    
    def set_file_data_synced(self, fd_id: int, is_synced: bool) -> None:
        raise NotImplementedError()
    
    def set_file_data_writable(self, fd_id: int, is_writable: bool) -> None:
        raise NotImplementedError()
    
    def remove_file_data(self, uid: str) -> None:
        raise NotImplementedError()

class FileDataDAOFactory(object):

    def __init__(self):
        super().__init__()
    
    def file_data_dao(self, conn: DbConnection) -> FileDataDAO:
        raise NotImplementedError()
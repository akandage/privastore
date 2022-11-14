from ..error import FileError
from ..file import File
from ..worker_msg import WorkerMessage

class TransferFileMessage(WorkerMessage):

    def __init__(self, file_id: str, chunk_offset: int = 0):
        super().__init__()
        if file_id is None or not File.is_valid_file_id(file_id):
            raise FileError('Invalid file id!')
        self._file_id = file_id
        self._chunk_offset = chunk_offset
    
    def file_id(self) -> str:
        return self._file_id
    
    def chunk_offset(self) -> int:
        return self._chunk_offset

    def __str__(self):
        return 'TransferFileMessage file-id=[{}] chunk-offset=[{}]'.format(self.file_id(), self.chunk_offset())
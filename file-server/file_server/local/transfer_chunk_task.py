from ..error import FileError
from ..file import File
from ..util.file import str_mem_size
from ..worker_task import WorkerTask

class TransferChunkTask(WorkerTask):

    TASK_CODE = 1

    def __init__(self, file_id: str, chunk_data: bytes, chunk_offset: int = 0):
        super().__init__()
        if file_id is None or not File.is_valid_file_id(file_id):
            raise FileError('Invalid file id!')
        if chunk_data is None:
            raise FileError('No chunk data!')
        self._file_id = file_id
        self._chunk_data = chunk_data
        self._chunk_offset = chunk_offset
    
    def file_id(self) -> str:
        return self._file_id
    
    def chunk_data(self) -> bytes:
        return self._chunk_data

    def chunk_offset(self) -> int:
        return self._chunk_offset

    def __str__(self):
        return 'TRANSFER_CHUNK file-id=[{}] chunk-length=[{}] chunk-offset=[{}]'.format(self.file_id(), str_mem_size(len(self.chunk_data())), self.chunk_offset())
from ..error import FileError
from .file_task import FileTask
from ..util.file import str_mem_size
from ..worker_task import WorkerTask

class TransferChunkTask(FileTask):

    TASK_CODE = 2

    def __init__(self, local_file_id: str, chunk_data: bytes, chunk_offset: int = 0):
        super().__init__(local_file_id)
        self._chunk_data = chunk_data
        self._chunk_offset = chunk_offset
    
    def task_code(self):
        return self.TASK_CODE
    
    def task_name(self):
        return 'TRANSFER_CHUNK'

    def chunk_data(self) -> bytes:
        return self._chunk_data

    def chunk_len(self) -> int:
        return len(self._chunk_data)

    def chunk_offset(self) -> int:
        return self._chunk_offset

    def __str__(self):
        s = super().__str__()
        s += ' chunk-len=[{}] chunk-offset=[{}]'.format(str_mem_size(self.chunk_len()), self.chunk_offset())
        return s
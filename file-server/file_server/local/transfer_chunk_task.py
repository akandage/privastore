from ..error import FileError
from .transfer_file_task import TransferFileTask
from ..util.file import str_path
from ..worker_task import WorkerTask

class TransferChunkTask(TransferFileTask):

    TASK_CODE = 1

    def __init__(self, file_path: list[str], file_name: str, file_version: int, chunk_data: bytes, chunk_offset: int = 0):
        super().__init__(file_path, file_name, file_version)
        self._chunk_data = chunk_data
        self._chunk_offset = chunk_offset
       
    def chunk_data(self) -> bytes:
        return self._chunk_data

    def chunk_len(self) -> int:
        return len(self._chunk_data)

    def chunk_offset(self) -> int:
        return self._chunk_offset

    def __str__(self):
        return 'TRANSFER_CHUNK file-id=[{}] file-name=[{}] file-version=[{}] chunk-size=[{}] chunk-offset=[{}]'.format(str_path(self.file_path() + [self.file_name()]), self.file_name(), self.file_version(), self.chunk_len(), self.chunk_offset())
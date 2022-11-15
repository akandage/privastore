from ..error import FileError
from ..file import File
from ..worker_task import WorkerTask

class TransferFileTask(WorkerTask):

    TASK_CODE = 2

    def __init__(self, file_id: str, chunk_offset: int = 0, is_commit: bool = True):
        super().__init__()
        if file_id is None or not File.is_valid_file_id(file_id):
            raise FileError('Invalid file id!')
        self._file_id = file_id
        self._chunk_offset = chunk_offset
        self._is_commit = is_commit
    
    def file_id(self) -> str:
        return self._file_id
    
    def chunk_offset(self) -> int:
        return self._chunk_offset

    def is_commit(self) -> bool:
        return self._is_commit

    def __str__(self):
        return 'TRANSFER_FILE file-id=[{}] chunk-offset=[{}]'.format(self.file_id(), self.chunk_offset())
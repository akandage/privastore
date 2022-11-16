from ..util.file import str_path
from ..worker_task import WorkerTask

class TransferFileTask(WorkerTask):

    TASK_CODE = 2

    def __init__(self, file_path: list[str], file_name: str, file_version: int):
        super().__init__()
        self._file_path = file_path
        self._file_name = file_name
        self._file_version = file_version
    
    def file_path(self) -> str:
        return self._file_path

    def file_name(self) -> str:
        return self.file_name
    
    def file_version(self) -> int:
        return self._file_version

    def __str__(self):
        return 'TRANSFER_FILE file-id=[{}] file-name=[{}] file-version=[{}]'.format(str_path(self.file_path() + [self.file_name()]), self.file_version())
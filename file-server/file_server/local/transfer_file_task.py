from .file_task import FileTask
from ..util.file import str_mem_size

class TransferFileTask(FileTask):

    TASK_CODE = 3

    def __init__(self, file_path: list[str], file_name: str, file_version: int, file_size: int, local_file_id: str):
        super().__init__(file_path, file_name, file_version)
        self._file_size = file_size
        self._local_file_id = local_file_id

    def task_code(self):
        return self.TASK_CODE
    
    def task_name(self):
        return 'TRANSFER_FILE'

    def file_size(self) -> int:
        return self._file_size

    def local_file_id(self) -> str:
        return self._local_file_id

    def __str__(self):
        s = super().__str__()
        s += ' file-size=[{}] local-file-id=[{}]'.format(str_mem_size(self.file_size()), self.local_file_id())
        return s
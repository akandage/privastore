from .file_task import FileTask
from ..util.file import str_mem_size

class TransferFileTask(FileTask):

    TASK_CODE = 3

    def __init__(self, local_file_id: str, file_size: int, is_commit: bool):
        super().__init__(local_file_id)
        self._file_size = file_size
        self._is_commit = is_commit

    def task_code(self):
        return self.TASK_CODE
    
    def task_name(self):
        return 'TRANSFER_FILE'

    def file_size(self) -> int:
        return self._file_size

    def is_commit(self) -> bool:
        return self._is_commit

    def __str__(self):
        s = super().__str__()
        s += ' file-size=[{}] is-commit=[{}]'.format(str_mem_size(self.file_size()), self.is_commit())
        return s
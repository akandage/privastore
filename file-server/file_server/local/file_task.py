from ..util.file import str_path
from ..worker_task import WorkerTask

class FileTask(WorkerTask):

    TASK_CODE = 1

    def __init__(self, file_path: list[str], file_name: str, file_version: int):
        super().__init__()
        self._file_path = file_path
        self._file_name = file_name
        self._file_version = file_version
    
    def task_code(self):
        return self.TASK_CODE
    
    def task_name(self):
        return 'FILE_TASK'

    def file_path(self) -> str:
        return self._file_path

    def file_name(self) -> str:
        return self.file_name
    
    def file_version(self) -> int:
        return self._file_version

    def __str__(self):
        return '{} path=[{}] file-version=[{}] file-size=[{}]'.format(self.task_name(), str_path(self.file_path() + [self.file_name()]), 
            self.file_version())
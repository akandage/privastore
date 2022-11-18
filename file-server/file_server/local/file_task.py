from ..error import FileError
from ..file import File
from ..util.file import str_path
from ..worker_task import WorkerTask

class FileTask(WorkerTask):

    TASK_CODE = 1

    def __init__(self, local_file_id: str):
        super().__init__()
        if not File.is_valid_file_id(local_file_id):
            raise FileError('Invalid local file id!')
        self._local_file_id = local_file_id
    
    def task_code(self):
        return self.TASK_CODE
    
    def task_name(self):
        return 'FILE_TASK'
   
    def local_file_id(self) -> str:
        return self._local_file_id

    def __str__(self):
        return '{} local-file-id=[{}]'.format(self.task_name(), self.local_file_id())
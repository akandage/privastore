from .file_task import FileTask
from ..util.file import str_mem_size

class DeleteFileTask(FileTask):

    TASK_CODE = 5

    def __init__(self, local_file_id: str):
        super().__init__(local_file_id)

    def task_code(self):
        return self.TASK_CODE
    
    def task_name(self):
        return 'DELETE_FILE'
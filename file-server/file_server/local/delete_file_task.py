from .file_task import FileTask
from ..util.file import str_mem_size

class DeleteFileTask(FileTask):

    TASK_CODE = 5

    def __init__(self, local_file_id: str, epoch_no: int):
        super().__init__(local_file_id)
        self._epoch_no = epoch_no

    def task_code(self):
        return self.TASK_CODE
    
    def task_name(self):
        return 'DELETE_FILE'
    
    def epoch_no(self):
        return self._epoch_no
    
    def __str__(self):
        s = super().__str__()
        s += ' epoch-no=[{}]'.format(self.epoch_no())
        return s
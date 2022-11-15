from ..daemon import Daemon
from ..error import FileError, UploadWorkerError
from ..file import File
from .transfer_chunk_task import TransferChunkTask
from .transfer_file_task import TransferFileTask
from ..worker import Worker
from ..worker_task import WorkerTask

class UploadWorker(Worker):

    def __init__(self, worker_index=None, queue_size=1, num_retries=1, io_timeout=90):
        super().__init__(worker_name='upload-worker', worker_index=worker_index, queue_size=queue_size)
        self._num_retries = num_retries
        self._io_timeout = io_timeout
    
    def num_retries(self):
        return self._num_retries
    
    def io_timeout(self):
        return self._io_timeout

    def process_task(self, task: WorkerTask) -> None:
        if task.task_code() == TransferChunkTask.TASK_CODE:
            self.do_transfer_chunk(task)
        if task.task_code() == TransferFileTask.TASK_CODE:
            self.do_transfer_file(task)
        raise UploadWorkerError('Unrecognized task code [{}]'.format(task.task_code()))
    
    def do_transfer_chunk(self, task: TransferChunkTask) -> None:
        pass

    def do_transfer_file(self, task: TransferFileTask) -> None:
        pass
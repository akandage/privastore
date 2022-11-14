from ..daemon import Daemon
from ..error import FileError, UploadWorkerError
from ..file import File

class UploadWorker(Daemon):

    def __init__(self, worker_id=None):
        worker_name = 'upload-worker-{}'.format(worker_id) if worker_id is not None else 'upload-worker'
        super().__init__(worker_name)
import uuid

from .directory import DirectoryEntry
from .error import FileError

class File(DirectoryEntry):

    def __init__(self, *args):
        super().__init__(*args)
    
    @staticmethod
    def generate_uid():
        return 'D-' + str(uuid.uuid4())

    @staticmethod
    def validate_uuid(uid: str):
        if not uid.startswith('D-'):
            raise FileError('Invalid directory id [{}]'.format(uid), FileError.INVALID_FILE_ID)
        try:
            uuid.UUID(uid[2:])
        except:
            raise FileError('Invalid directory id [{}]'.format(uid), FileError.INVALID_FILE_ID)

class FileData(object):

    def __init__(self, id: int, uid: str, size: int, total_blocks: int, created_timestamp: int, modified_timestamp: int, is_writable: bool, is_synced: bool):
        super().__init__()
        self._id = id
        self._uid = uid
        self._size = size
        self._total_blocks = total_blocks
        self._created_timestamp = created_timestamp
        self._modified_timestamp = modified_timestamp
        self._is_writable = is_writable
        self._is_synced = is_synced
    
    def id(self):
        return self._id

    def uid(self):
        return self._uid
    
    def size(self):
        return self._size
    
    def total_blocks(self):
        return self._total_blocks

    def created_timestamp(self):
        return self._created_timestamp
    
    def modified_timestamp(self):
        return self._modified_timestamp

    def is_writable(self):
        return self._is_writable
    
    def is_synced(self):
        return self._is_synced
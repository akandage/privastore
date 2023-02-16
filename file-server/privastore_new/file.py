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
import time
import uuid

from .error import DirectoryError
from .util import format_datetime

class DirectoryEntry(object):

    def __init__(self, name: str, uid: str, created_timestamp: int, modified_timestamp: int, owner: str):
        self._name = name
        self._uid = uid
        self._created_timestamp = created_timestamp
        self._modified_timestamp = modified_timestamp
        self._owner = owner
    
    def name(self):
        return self._name

    def uid(self):
        return self._uid

    def created_timestamp(self):
        return self._created_timestamp
    
    def created_time(self) -> time.struct_time:
        return time.gmtime(self._created_timestamp)

    def modified_timestamp(self):
        return self._modified_timestamp
    
    def modified_time(self) -> time.struct_time:
        return time.gmtime(self._created_timestamp)
    
    def owner(self):
        return self._owner
    
    def to_dict(self):
        return {
            'name': self.name(),
            'uid:': self.uid(),
            'created_timestamp': self.created_timestamp(),
            'created_time': format_datetime(self.created_time()),
            'modified_timestamp': self.created_timestamp(),
            'modified_time': format_datetime(self.created_time()),
            'owner': self.owner()
        }

class Directory(DirectoryEntry):

    def __init__(self, *args):
        super().__init__(*args)
    
    @staticmethod
    def generate_uid():
        return 'D-' + str(uuid.uuid4())

    @staticmethod
    def validate_uuid(uid: str):
        if not uid.startswith('D-'):
            raise DirectoryError('Invalid directory id [{}]'.format(uid), DirectoryError.INVALID_DIRECTORY_ID)
        try:
            uuid.UUID(uid[2:])
        except:
            raise DirectoryError('Invalid directory id [{}]'.format(uid), DirectoryError.INVALID_DIRECTORY_ID)
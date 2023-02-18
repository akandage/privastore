import uuid

from .directory import DirectoryEntry
from .error import FileError

class File(DirectoryEntry):

    def __init__(self, name: str, uid: str, mime_type: str, parent_uid: str, abs_path: list[str], created_timestamp: int, modified_timestamp: int, owner: str, versions: list['FileVersion']):
        super().__init__(name, uid, parent_uid, abs_path, created_timestamp, modified_timestamp, owner)
        self._mime_type = mime_type
        self._versions = versions
    
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

    def mime_type(self):
        return self._mime_type

    def versions(self):
        return self._versions

    def to_dict(self):
        d = super().to_dict()
        d['mime-type'] = self.mime_type()
        versions = list()
        d['versions'] = versions
        
        for version in self.versions():
            versions.append(version.to_dict())
        
        return d

class FileVersion(object):

    def __init__(self, file_uid: str, version: int, file_data: 'FileData'):
        super().__init__()
        self._file_uid = file_uid
        self._version = version
        self._file_data = file_data
    
    def file_uid(self):
        return self._file_uid
    
    def version(self):
        return self._version
    
    def file_data(self):
        return self._file_data
    
    def to_dict(self):
        d = {
            'file-uid': self.file_uid(),
            'version': self.version(),
        }
        fd = self.file_data()

        if fd is not None:
            d['file-data-uid'] = fd.uid()
            d['size'] = fd.size()
            d['total-chunks'] = fd.total_chunks()
            d['created-timestamp'] = fd.created_timestamp()
            d['modified-timestamp'] = fd.modified_timestamp()

        return d

class FileData(object):

    def __init__(self, id: int, uid: str, size: int, total_chunks: int, created_timestamp: int, modified_timestamp: int, is_writable: bool, is_synced: bool):
        super().__init__()
        self._id = id
        self._uid = uid
        self._size = size
        self._total_chunks = total_chunks
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
    
    def total_chunks(self):
        return self._total_chunks

    def created_timestamp(self):
        return self._created_timestamp
    
    def modified_timestamp(self):
        return self._modified_timestamp

    def is_writable(self):
        return self._is_writable
    
    def is_synced(self):
        return self._is_synced
from enum import Enum

class FileType(Enum):
    '''
        Generic binary data.
    '''
    BINARY_DATA = (1, 'application/octet-stream')

    def __new__(cls, value, mime_type):
        obj = object.__new__(cls)
        obj._value_ = value
        obj.mime_type = mime_type
        return obj
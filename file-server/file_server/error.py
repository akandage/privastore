from http import HTTPStatus

class FileServerErrorCode:

    '''
        File server error codes.
    '''

    USER_NOT_FOUND = "USER_NOT_FOUND"
    INCORRECT_PASSWORD = "INCORRECT_PASSWORD"
    DIRECTORY_NAME_EMPTY = "DIRECTORY_NAME_EMPTY"
    DIRECTORY_EXISTS = "DIRECTORY_EXISTS"
    DIRECTORY_NOT_FOUND = "DIRECTORY_NOT_FOUND"
    EPOCH_IS_OVER = "EPOCH_IS_OVER"
    FILE_EXISTS = "FILE_EXISTS"
    FILE_NAME_EMPTY = "FILE_NAME_EMPTY"
    FILE_NOT_FOUND = "FILE_NOT_FOUND"
    FILE_NOT_WRITABLE = "FILE_NOT_WRITABLE"
    FILE_NOT_REMOVABLE = "FILE_NOT_REMOVABLE"
    FILE_VERSION_NOT_FOUND = "FILE_VERSION_NOT_FOUND"
    FILE_IS_CORRUPT = "FILE_IS_CORRUPT"
    FILE_IS_COMMITTED = "FILE_IS_COMMITTED"
    FILE_IS_UNCOMMITTED = "FILE_IS_UNCOMMITTED"
    FILE_IS_DIRECTORY = "FILE_IS_DIRECTORY"
    FILE_STORE_FULL = "FILE_STORE_FULL"
    FILE_CHUNK_TOO_LARGE = "FILE_CHUNK_TOO_LARGE"
    FILE_TOO_SMALL = "FILE_TOO_SMALL"
    FILE_TOO_LARGE = "FILE_TOO_LARGE"
    INSUFFICIENT_SPACE = "INSUFFICIENT_SPACE"
    IO_ERROR = "IO_ERROR"
    IO_TIMEOUT = "IO_TIMEOUT"
    SESSION_NOT_FOUND = "SESSION_NOT_FOUND"
    INVALID_CHUNK_NUM = "INVALID_CHUNK_NUM"
    INVALID_EPOCH_NO = "INVALID_EPOCH_NO"
    INVALID_FILE_ID = "INVALID_FILE_ID"
    INVALID_FILE_SIZE = "INVALID_FILE_SIZE"
    INVALID_PATH = "INVALID_PATH"
    INVALID_SEEK_OFFSET = "INVALID_SEEK_OFFSET"
    INVALID_SESSION_ID = "INVALID_SESSION_ID"
    INTERNAL_ERROR = "INTERNAL_ERROR"

class FileServerError(Exception):
    def __init__(self, msg: str, error_code: int=FileServerErrorCode.INTERNAL_ERROR):
        super().__init__(msg)
        self._error_code = error_code

    def error_code(self):
        return self._error_code

class AuthenticationError(FileServerError):
    def __init__(self, msg: str, error_code: int=FileServerErrorCode.INTERNAL_ERROR):
        super().__init__(msg, error_code)

class DirectoryError(FileServerError):
    def __init__(self, msg: str, error_code: int=FileServerErrorCode.INTERNAL_ERROR):
        super().__init__(msg, error_code)

class EpochError(FileServerError):
    def __init__(self, msg: str, error_code: int=FileServerErrorCode.INTERNAL_ERROR):
        super().__init__(msg, error_code)

class FileCacheError(FileServerError):
    def __init__(self, msg: str, error_code: int=FileServerErrorCode.INTERNAL_ERROR):
        super().__init__(msg, error_code)

class FileError(FileServerError):
    def __init__(self, msg: str, error_code: int=FileServerErrorCode.INTERNAL_ERROR):
        super().__init__(msg, error_code)

class FileChunkError(FileServerError):
    def __init__(self, msg: str, error_code: int=FileServerErrorCode.INTERNAL_ERROR):
        super().__init__(msg, error_code)

class FileDownloadError(FileServerError):
    def __init__(self, msg: str, error_code: int=FileServerErrorCode.INTERNAL_ERROR):
        super().__init__(msg, error_code)

class FileUploadError(FileServerError):
    def __init__(self, msg: str, error_code: int=FileServerErrorCode.INTERNAL_ERROR):
        super().__init__(msg, error_code)

class RemoteFileError(FileServerError):
    def __init__(self, msg: str, error_code: int=FileServerErrorCode.INTERNAL_ERROR):
        super().__init__(msg, error_code)

class SessionError(FileServerError):
    def __init__(self, msg: str, error_code: int=FileServerErrorCode.INTERNAL_ERROR):
        super().__init__(msg, error_code)
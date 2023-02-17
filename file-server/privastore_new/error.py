
class FileServerError(Exception):

    '''
        File server error codes.
    '''

    INTERNAL_ERROR = "INTERNAL_ERROR"

    def __init__(self, msg: str, error_code: str=INTERNAL_ERROR):
        super().__init__(msg)
        self._error_code = error_code

    def error_code(self) -> str:
        return self._error_code
    
    def to_dict(self) -> dict:
        return {'error-code': self.error_code(), 'message': str(self)}

class AuthenticationError(FileServerError):

    '''
        Authentication error codes.
    '''

    USER_NOT_FOUND = "USER_NOT_FOUND"
    INCORRECT_PASSWORD = "INCORRECT_PASSWORD"

    def __init__(self, msg: str, error_code: str=FileServerError.INTERNAL_ERROR):
        super().__init__(msg, error_code)


class DatabaseError(FileServerError):

    '''
        Database error codes.
    '''

    CONNECTION_POOL_TIMEOUT = "CONNECTION_POOL_TIMEOUT"

    def __init__(self, msg: str, error_code: str=FileServerError.INTERNAL_ERROR):
        super().__init__(msg, error_code)

class DirectoryError(FileServerError):

    '''
        Directory error codes.
    '''

    INVALID_PATH = "INVALID_PATH"
    INVALID_DIRECTORY_NAME = "INVALID_DIRECTORY_NAME"
    DIRECTORY_NOT_FOUND = "DIRECTORY_NOT_FOUND"
    DIRECTORY_EXISTS = "DIRECTORY_EXISTS"
    INVALID_DIRECTORY_ID = "INVALID_DIRECTORY_ID"

    def __init__(self, msg: str, error_code: str=FileServerError.INTERNAL_ERROR):
        super().__init__(msg, error_code)

class FileError(FileServerError):

    '''
        File error codes.
    '''

    INVALID_FILE_ID = "INVALID_FILE_ID"
    INVALID_FILE_NAME = "INVALID_FILE_NAME"
    FILE_NOT_FOUND = "FILE_NOT_FOUND"
    FILE_EXISTS = "FILE_EXISTS"

    def __init__(self, msg: str, error_code: str=FileServerError.INTERNAL_ERROR):
        super().__init__(msg, error_code)

class HttpError(FileServerError):

    '''
        HTTP error codes.
    '''

    BAD_REQUEST = "BAD_REQUEST"

    def __init__(self, msg: str, error_code: str=BAD_REQUEST):
        super().__init__(msg, error_code)

class KeyError(FileServerError):

    '''
        Key error codes.
    '''

    INVALID_ALGORITHM = "INVALID_ALGORITHM"

    def __init__(self, msg: str, error_code: str=FileServerError.INTERNAL_ERROR):
        super().__init__(msg, error_code)

class LogError(FileServerError):

    '''
        Log error codes.
    '''

    def __init__(self, msg: str, error_code: str=FileServerError.INTERNAL_ERROR):
        super().__init__(msg, error_code)

class NotImplementedError(FileServerError):

    def __init__(self, msg: str='Method not implemented!', error_code: str=FileServerError.INTERNAL_ERROR):
        super().__init__(msg, error_code)

class SessionError(FileServerError):

    '''
        Session error codes.
    '''

    INVALID_SESSION_ID = "INVALID_SESSION_ID"
    SESSION_NOT_FOUND = "SESSION_NOT_FOUND"

    def __init__(self, msg: str, error_code: str=FileServerError.INTERNAL_ERROR):
        super().__init__(msg, error_code)
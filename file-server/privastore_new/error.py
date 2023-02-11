
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

class HttpError(FileServerError):

    '''
        HTTP error codes.
    '''

    BAD_REQUEST = "BAD_REQUEST"

    def __init__(self, msg: str, error_code: str=BAD_REQUEST):
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
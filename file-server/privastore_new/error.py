
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

class DatabaseError(FileServerError):

    '''
        Database error codes.
    '''

    CONNECTION_POOL_TIMEOUT = "CONNECTION_POOL_TIMEOUT"

    def __init__(self, msg: str, error_code: str=CONNECTION_POOL_TIMEOUT):
        super().__init__(msg, error_code)

class NotImplementedError(FileServerError):

    def __init__(self, msg: str='Method not implemented!', error_code: str=FileServerError.INTERNAL_ERROR):
        super().__init__(msg, error_code)
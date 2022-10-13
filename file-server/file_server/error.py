class AuthenticationError(Exception):
    def __init__(self, msg):
        super().__init__(msg)

class DirectoryError(Exception):
    def __init__(self, msg):
        super().__init__(msg)

class FileCacheError(Exception):
    def __init__(self, msg):
        super().__init__(msg)

class FileError(Exception):
    def __init__(self, msg):
        super().__init__(msg)

class FileChunkError(Exception):
    def __init__(self, msg):
        super().__init__(msg)

class FileUploadError(Exception):
    def __init__(self, msg):
        super().__init__(msg)

class SessionError(Exception):
    def __init__(self, msg):
        super().__init__(msg)
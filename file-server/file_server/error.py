class AuthenticationError(Exception):
    def __init__(self, msg):
        super().__init__(msg)

class DirectoryError(Exception):
    def __init__(self, msg):
        super().__init__(msg)

class SessionError(Exception):
    def __init__(self, msg):
        super().__init__(msg)
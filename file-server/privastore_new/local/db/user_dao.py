from ...error import NotImplementedError

class UserDAO(object):

    def __init__(self):
        super().__init__()
    
    def login_user(self, username: str, password: str) -> None:
        raise NotImplementedError()
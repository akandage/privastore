from .dao import DataAccessObject

class UserDAO(DataAccessObject):

    def __init__(self, conn):
        super().__init__(conn)
    
    def login_user(username, password):
        raise Exception('Not implemented!')
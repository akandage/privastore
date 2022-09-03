from .dao import DataAccessObject

class UserDAO(DataAccessObject):

    def __init__(self, conn):
        super().__init__(conn)
    
    '''
        Login user with given username and password.

        Throws AuthenticationError if user is not found or password is incorrect.
    '''
    def login_user(self, username, password):
        raise Exception('Not implemented!')
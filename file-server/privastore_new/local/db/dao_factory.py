from ...db.conn import DbConnection
from ...error import NotImplementedError
from .user_dao import UserDAO

class DAOFactory(object):

    def __init__(self):
        super().__init__()
    
    def user_dao(self, conn: DbConnection) -> UserDAO:
        raise NotImplementedError()
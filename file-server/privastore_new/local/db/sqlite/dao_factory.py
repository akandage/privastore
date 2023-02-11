from ....db.conn import SqliteConnection
from ..dao_factory import DAOFactory
from ..user_dao import UserDAO

class SqliteDAOFactory(DAOFactory):

    def __init__(self):
        super().__init__()
    
    def user_dao(self, conn: SqliteConnection) -> UserDAO:
        return super().user_dao(conn)
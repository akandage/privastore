import logging

from ....crypto.util import hash_user_password
from ....db.conn import SqliteConnection
from ....error import AuthenticationError
from ..user_dao import UserDAO

class SqliteUserDAO(UserDAO):

    def __init__(self, conn: SqliteConnection):
        super().__init__(conn)
    
    def conn(self) -> SqliteConnection:
        return super().conn()

    def login_user(self, username: str, password: str) -> None:
        logging.debug('Login user')
        self.conn().begin_transaction()
        cur = self.conn().cursor()
        try:
            try:
                cur.execute('SELECT password_hash FROM ps_user_account WHERE username = ?', (username,))
                res = cur.fetchone()
                self.commit()
            except Exception as e:
                logging.error('Query error {}'.format(str(e)))
                self.rollback_nothrow()
                raise e
        finally:
            try:
                cur.close()
            except:
                pass
        
        if res:
            password_hash, = res
            if password_hash != hash_user_password(password):
                raise AuthenticationError('Incorrect username or password!', error_code=AuthenticationError.INCORRECT_PASSWORD)
        else:
            raise AuthenticationError('User not found!', error_code=AuthenticationError.USER_NOT_FOUND)
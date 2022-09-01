import logging
from ...error import AuthenticationError
from ..user_dao import UserDAO
from ....util.crypto import hash_user_password

class SqliteUserDAO(UserDAO):

    def __init__(self, conn):
        super().__init__(conn)
        self._conn = conn

    def login_user(self, username, password):
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('SELECT password_hash FROM ps_user_account WHERE username = ?', (username,))
                res = cur.fetchone()
                self._conn.commit()
            except Exception as e:
                logging.error('Query error {}'.format(str(e)))
                try:
                    self._conn.rollback()
                    logging.debug('Rolled back')
                except Exception as e1:
                    logging.error('Error rolling back {}'.format(str(e1)))
                return

            if res:
                password_hash, = res
                if password_hash != hash_user_password(password):
                    raise AuthenticationError('Incorrect username or password!')
            else:
                raise AuthenticationError('User not found!')

        finally:
            try:
                cur.close()
            except:
                pass
import logging
from ....error import KeyError, FileServerErrorCode
from ..key_dao import KeyDAO, Key

class SqliteKeyDAO(KeyDAO):

    def __init__(self, conn):
        super().__init__(conn)

    def get_key(self, key_id):
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('SELECT key_bytes, algorithm, is_system FROM ps_key WHERE name = ?', (key_id,))
                res = cur.fetchone()
                if res is None:
                    raise KeyError('Key [{}] not found!'.format(key_id,), FileServerErrorCode.KEY_NOT_FOUND)
                self._conn.commit()
                key_bytes, algorithm, is_system = res
                return Key(key_id, bytes(key_bytes), algorithm, bool(is_system))
            except Exception as e:
                logging.error('Query error {}'.format(str(e)))
                self.rollback_nothrow()
                raise e
        finally:
            try:
                cur.close()
            except:
                pass
import sqlite3
from ....error import FileServerErrorCode, KeyError

def get_key_id(cur: sqlite3.Cursor, key_name: str) -> int:
    cur.execute('SELECT id FROM ps_key WHERE name = ?', (key_name,))
    res = cur.fetchone()
    if res is None:
        raise KeyError('Key [{}] not found!'.format(key_name), FileServerErrorCode.KEY_NOT_FOUND)
    key_id, = res
    return key_id
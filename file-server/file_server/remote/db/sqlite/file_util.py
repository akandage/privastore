from ....error import RemoteFileError
import sqlite3
from typing import Optional

def is_file_committed(cur: sqlite3.Cursor, remote_id: str) -> bool:
    cur.execute(
        '''
        SELECT created_epoch 
        FROM ps_remote_file 
        WHERE remote_id = ? AND removed_epoch IS NULL
        '''
    , (remote_id,))
    res = cur.fetchone()
    if res is None:
        raise RemoteFileError('Remote file [{}] not found'.format(remote_id))
    return bool(res[0] is not None)

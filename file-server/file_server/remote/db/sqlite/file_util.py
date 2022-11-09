from ....error import RemoteFileError
import sqlite3
from typing import Optional

def is_file_committed(cur: sqlite3.Cursor, remote_id: str, epoch_no: Optional[int]=None) -> bool:
    if epoch_no is not None:
        cur.execute(
            '''
            SELECT is_committed 
            FROM ps_remote_file 
            WHERE remote_id = ? AND created_epoch = ?
            '''
        , (remote_id, epoch_no))
    else:
        cur.execute(
            '''
            SELECT is_committed 
            FROM ps_remote_file 
            WHERE remote_id = ?
            '''
        , (remote_id,))
    res = cur.fetchone()
    if res is None:
        raise RemoteFileError('Remote file [{}] not found'.format(remote_id))
    return bool(res[0])

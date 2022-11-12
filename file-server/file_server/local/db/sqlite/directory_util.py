from ....error import DirectoryError, FileServerErrorCode
from ...file_transfer_status import FileTransferStatus
import sqlite3
from typing import Optional

def query_directory_id(cur: sqlite3.Cursor, parent_directory_id: int, directory_name: str, show_hidden: bool=False) -> Optional[int]:
    cur.execute('''
        SELECT L.child_id FROM ps_directory AS D INNER JOIN ps_link AS L ON L.child_id = D.id 
        WHERE L.parent_id = ? AND D.name = ? AND (D.is_hidden <> 1 OR D.is_hidden = ?)
            AND D.is_removed <> 1
    ''', (parent_directory_id, directory_name, show_hidden))
    res = cur.fetchone()
    if res is None:
        return None
    return res[0]

def query_file_id(cur: sqlite3.Cursor, parent_directory_id: int, file_name: str, show_hidden: bool=False) -> Optional[int]:
    cur.execute('''
        SELECT F.id FROM ps_file AS F INNER JOIN ps_file_version AS V ON F.id = V.file_id 
        WHERE F.parent_id = ? AND F.name = ? AND V.local_transfer_status <> ? AND (F.is_hidden <> 1 OR F.is_hidden = ?) 
            AND F.is_removed <> 1
    ''', (parent_directory_id, file_name, FileTransferStatus.TRANSFER_DATA_FAILED.value, show_hidden))
    res = cur.fetchone()
    if res is None:
        return None
    return res[0]

def traverse_path(cur: sqlite3.Cursor, path: list[str]) -> int:
    # Start from root directory and iterate to the last directory in the path.
    #
    directory_id = 1
    for directory_name in path:
        directory_id = query_directory_id(cur, directory_id, directory_name)
        if directory_id is None:
            raise DirectoryError('Invalid path to directory [{}]'.format('/' + '/'.join(path)), FileServerErrorCode.INVALID_PATH)
    return directory_id
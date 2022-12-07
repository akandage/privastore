import sqlite3
from ....error import FileServerErrorCode, FileError

def get_file_data_id(cur: sqlite3.Cursor, file_id: int, version: int) -> int:
    cur.execute('SELECT file_data_id FROM ps_file_version WHERE file_id = ? AND version = ?', (file_id, version))
    res = cur.fetchone()
    if res is None:
        raise FileError('File data not found!', FileServerErrorCode.FILE_VERSION_NOT_FOUND)
    file_data_id, = res
    return file_data_id
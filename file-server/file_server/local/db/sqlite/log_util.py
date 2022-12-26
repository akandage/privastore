import json
import sqlite3
from ....error import FileServerErrorCode, LogError
from ...log_entry_type import LogEntryType

def encode_log_entry(entry) -> bytes:
    return json.dumps(entry).encode('utf-8')

def decode_log_entry(entry: bytes):
    return json.loads(str(entry, 'utf-8'))

def get_first_epoch(cur: sqlite3.Cursor) -> int:
    cur.execute('SELECT min(epoch_no) FROM ps_log')
    res = cur.fetchone()
    if res is None or res[0] is None:
        raise LogError('Log has no entries!')
    return res[0]

def get_current_epoch(cur: sqlite3.Cursor) -> int:
    cur.execute('SELECT max(epoch_no) FROM ps_log')
    res = cur.fetchone()
    if res is None or res[0] is None:
        raise LogError('Log has no entries!')
    return res[0]

def add_log_entry(cur: sqlite3.Cursor, type: LogEntryType, entry: bytes) -> int:
    epoch_no = get_current_epoch(cur)
    cur.execute('INSERT INTO ps_log (epoch_no, entry_type, entry) VALUES (?, ?, ?)', (epoch_no, type.value, entry))
    if cur.rowcount != 1:
        raise LogError('Could not add log entry [{}]!'.format(type.name))
    seq_no = cur.lastrowid
    return seq_no

def get_log_entry(cur: sqlite3.Cursor, seq_no: int) -> bytes:
    cur.execute('SELECT entry FROM ps_log WHERE seq_no = ?', (seq_no,))
    res = cur.fetchone()
    if res is None:
        raise LogError('Log entry [{}] not found!'.format(seq_no))
    return res[0]
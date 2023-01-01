import json
import sqlite3
from ....error import EpochError, FileServerErrorCode, LogError
from ...log_entry_type import LogEntryType

def encode_log_entry(entry) -> bytes:
    return json.dumps(entry).encode('utf-8')

def decode_log_entry(entry: bytes):
    return json.loads(str(entry, 'utf-8'))

def get_first_epoch(cur: sqlite3.Cursor) -> int:
    cur.execute('SELECT min(epoch_no) FROM ps_log')
    res = cur.fetchone()
    if res is None or res[0] is None:
        raise LogError('Log has no entries!', FileServerErrorCode.LOG_IS_EMPTY)
    return res[0]

def get_current_epoch(cur: sqlite3.Cursor) -> int:
    cur.execute('SELECT max(epoch_no) FROM ps_log')
    res = cur.fetchone()
    if res is None or res[0] is None:
        raise LogError('Log has no entries!', FileServerErrorCode.LOG_IS_EMPTY)
    return res[0]

def end_current_epoch(cur: sqlite3.Cursor) -> int:
    epoch_no = get_current_epoch(cur)
    epoch_no += 1
    cur.execute('INSERT INTO ps_log (epoch_no, entry_type) VALUES (?, ?)'.format((epoch_no, LogEntryType.START_EPOCH)))
    if cur.rowcount != 1:
        raise LogError('Could not end current epoch in log!')
    return epoch_no

def add_log_entry(cur: sqlite3.Cursor, type: LogEntryType, entry: bytes) -> int:
    if type == LogEntryType.START_EPOCH:
        raise LogError('Invalid log entry type [{}]!'.format(type.name))
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
        raise LogError('Log entry [{}] not found!'.format(seq_no), FileServerErrorCode.LOG_ENTRY_NOT_FOUND)
    return res[0]

def get_epoch_log_entries(cur: sqlite3.Cursor, epoch_no: int) -> list[bytes]:
    entries = list()
    num_entries = 0
    cur.execute('SELECT entry, entry_type FROM ps_log WHERE epoch_no = ?', (epoch_no,))
    for entry, entry_type in cur.fetchall():
        entry_type = LogEntryType(entry_type)
        if entry_type != LogEntryType.START_EPOCH:
            entries.append(entry)
        num_entries += 1
    if num_entries == 0:
        raise EpochError('Epoch [{}] not found in log!'.format(epoch_no), FileServerErrorCode.INVALID_EPOCH_NO)
    return entries

def truncate_log(cur: sqlite3.Cursor, epoch_no: int):
    curr_epoch = get_current_epoch(cur)
    if epoch_no == curr_epoch:
        return
    elif epoch_no > curr_epoch:
        raise EpochError('Cannot truncate log to epoch [{}]. Epoch number is higher than current epoch'.format(epoch_no), FileServerErrorCode.INVALID_EPOCH_NO)
    cur.execute('DELETE FROM ps_log WHERE epoch_no < ?', (epoch_no,))
    if cur.rowcount != 1:
        raise LogError('Could not truncate log to epoch [{}]'.format(epoch_no))

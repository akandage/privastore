from ....error import EpochError, FileServerErrorCode, RemoteFileError
import sqlite3

def get_current_epoch(cur: sqlite3.Cursor) -> int:
    cur.execute(
        '''
        SELECT max(epoch_no)
        FROM ps_epoch
        '''
    )
    res = cur.fetchone()
    if res is not None:
        if res[0] is not None:
            return int(res[0]) + 1
    #
    # Default to first epoch.
    #
    return 1

def check_valid_epoch(cur: sqlite3.Cursor, epoch_no: int) -> bool:
    '''
        Check that the provided epoch number is not a previous one.
    '''
    if epoch_no < 1:
        raise RemoteFileError('Invalid epoch no!', FileServerErrorCode.INVALID_EPOCH_NO)
    curr_epoch = get_current_epoch(cur)
    if epoch_no < curr_epoch:
        raise EpochError('Cannot modify file in previous epoch [{}]. Current epoch is [{}]'.format(epoch_no, curr_epoch), FileServerErrorCode.EPOCH_IS_OVER)
    return True
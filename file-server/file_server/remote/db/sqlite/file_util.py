from ....error import RemoteFileError

def is_file_committed(cur, remote_id, epoch_no=None):
    if epoch_no is not None:
        cur.execute(
            '''
            SELECT is_committed 
            FROM ps_remote_file 
            WHERE remote_id = ? AND created_epoch_no = ?
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

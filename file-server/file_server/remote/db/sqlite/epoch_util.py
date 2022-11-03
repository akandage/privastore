from ....error import EpochError

def get_current_epoch(cur):
    cur.execute(
        '''
        SELECT max(epoch_no)
        FROM ps_epoch
        '''
    )
    res = cur.fetchone()
    if res is not None:
        return int(res[0]) + 1
    #
    # Default to first epoch.
    #
    return 1

def check_current_epoch(cur, epoch_no):
    '''
        Check that the provided epoch number is the current one.
    '''
    curr_epoch = get_current_epoch(cur)
    if epoch_no != curr_epoch:
        raise EpochError('Cannot modify file in previous epoch [{}]. Current epoch is [{}]'.format(epoch_no, curr_epoch))
    return True
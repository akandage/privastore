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
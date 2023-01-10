from enum import Enum

class FileTransferStatus(Enum):
    '''
        Default transfer status (start state).
    '''
    NONE = 1
    '''
        Transferring file data.
    '''
    TRANSFERRING_DATA = 2
    '''
        Transferred all file data.
    '''
    TRANSFERRED_DATA = 3
    '''
        Failed to transfer all file data due to error, timeout etc.
    '''
    TRANSFER_DATA_FAILED = 4
    '''
        Syncing file (ensure it is durable).
    '''
    SYNCING_DATA = 5
    '''
        All file data is transferred and synced.
    '''
    SYNCED_DATA = 6
    '''
        Failed to sync file due to error, timeout etc.
    '''
    SYNC_DATA_FAILED = 7
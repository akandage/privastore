from enum import Enum

class FileTransferStatus(Enum):
    '''
        File is empty.
    '''
    EMPTY = 1
    '''
        Receiving file data.
    '''
    RECEIVING = 2
    '''
        Received all file data.
    '''
    RECEIVED = 3
    '''
        Failed to receive all file data due to error, timeout etc.
    '''
    RECEIVING_FAILED = 4
    '''
        Transferring file data to remote server.
    '''
    TRANSFERRING_TO_REMOTE = 5
    '''
        Transferred all file data to remote server.
    '''
    TRANSFERRED_TO_REMOTE = 6
    '''
        Failed to transfer all file data due to error, timeout etc.
    '''
    TRANSFER_TO_REMOTE_FAILED = 7
    '''
        Syncing file (ensure it is durable) at remote server.
    '''
    SYNCING_REMOTE = 8
    '''
        All file data is transferred and synced at remote server.
    '''
    SYNCED = 9
    '''
        Failed to sync file at remote server due to error, timeout etc.
    '''
    SYNC_FAILED = 10
from enum import Enum

class FileTransferStatus(Enum):
    '''
        Receiving file data.
    '''
    RECEIVING = 1
    '''
        Received all file data.
    '''
    RECEIVED = 2
    '''
        Failed to receive all file data due to error, timeout etc.
    '''
    RECEIVING_FAILED = 3
    '''
        Transferring file data to remote server.
    '''
    TRANSFERRING_TO_REMOTE = 4
    '''
        Transferred all file data to remote server.
    '''
    TRANSFERRED_TO_REMOTE = 5
    '''
        Failed to transfer all file data due to error, timeout etc.
    '''
    TRANSFER_TO_REMOTE_FAILED = 6
    '''
        Syncing file (ensure it is durable) at remote server.
    '''
    SYNCING_REMOTE = 7
    '''
        All file data is transferred and synced at remote server.
    '''
    SYNCED = 8
    '''
        Failed to sync file at remote server due to error, timeout etc.
    '''
    SYNC_FAILED = 9
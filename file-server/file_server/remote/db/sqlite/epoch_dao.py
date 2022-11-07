from ..epoch_dao import EpochDAO
from .epoch_util import check_valid_epoch
from ....error import EpochError, FileServerErrorCode, RemoteFileError
from ....file import File
from .file_util import is_file_committed
import logging
import time

class SqliteEpochDAO(EpochDAO):

    def __init__(self, conn):
        super().__init__(conn)
    
    def end_epoch(self, epoch_no, marker_id=None):
        if marker_id is not None:
            if not File.is_valid_file_id(marker_id):
                raise RemoteFileError('Invalid remote file id!', FileServerErrorCode.INVALID_FILE_ID)
        cur = self._conn.cursor()
        try:
            try:
                check_valid_epoch(cur)
                if marker_id is not None:
                    if not is_file_committed(cur, marker_id):
                        raise RemoteFileError('Epoch marker file must be committed remote file!', FileServerErrorCode.FILE_IS_COMMITTED)
                cur.execute(
                    '''
                    INSERT INTO ps_epoch (epoch_no, marker_id) 
                    VALUES (?, ?)
                    '''
                , (epoch_no, marker_id))
                self._conn.commit()
            except EpochError as e:
                logging.error('Query error {}'.format(str(e)))
                self.rollback_nothrow()
                raise e
            except RemoteFileError as e:
                logging.error('Remote file error {}'.format(str(e)))
                self.rollback_nothrow()
                raise e
            except Exception as e:
                logging.error('Query error {}'.format(str(e)))
                self.rollback_nothrow()
                raise e
        finally:
            try:
                cur.close()
            except:
                pass
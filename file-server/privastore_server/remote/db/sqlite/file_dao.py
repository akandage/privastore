from collections import namedtuple
from ....file import File
from ..file_dao import FileDAO, RemoteFileMetadata
from ....error import EpochError, FileServerErrorCode, RemoteFileError
from .epoch_util import check_valid_epoch
from .file_util import is_file_committed
import logging
import time

class SqliteFileDAO(FileDAO):

    def __init__(self, conn):
        super().__init__(conn)

    def create_file(self, remote_id):
        if not File.is_valid_file_id(remote_id):
            raise RemoteFileError('Invalid remote file id!', FileServerErrorCode.INVALID_FILE_ID)
        cur = self._conn.cursor()
        try:
            try:
                file_timestamp = round(time.time())
                cur.execute(
                    '''
                    INSERT INTO ps_remote_file (remote_id, created_timestamp, modified_timestamp) 
                    VALUES (?, ?, ?)
                    '''
                , (remote_id, file_timestamp, file_timestamp))
                self._conn.commit()
            except Exception as e:
                logging.error('Query error {}'.format(str(e)))
                self.rollback_nothrow()
                raise e
        finally:
            try:
                cur.close()
            except:
                pass
    
    def get_file_metadata(self, remote_id):
        if not File.is_valid_file_id(remote_id):
            raise RemoteFileError('Invalid remote file id!', FileServerErrorCode.INVALID_FILE_ID)
        cur = self._conn.cursor()
        try:
            try:
                cur.execute(
                    '''
                    SELECT created_timestamp, modified_timestamp, created_epoch, removed_epoch 
                    FROM ps_remote_file 
                    WHERE remote_id = ? AND removed_epoch IS NULL
                    '''
                , (remote_id,))
                res = cur.fetchone()
                if res is None:
                    raise RemoteFileError('Remote file [{}] not found'.format(remote_id), FileServerErrorCode.FILE_NOT_FOUND)
                self._conn.commit()
                return RemoteFileMetadata(
                    bool(res[2] is not None),
                    time.gmtime(res[0]),
                    time.gmtime(res[1]),
                    res[2],
                    res[3]
                )
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

    def file_modified(self, remote_id):
        if not File.is_valid_file_id(remote_id):
            raise RemoteFileError('Invalid remote file id!', FileServerErrorCode.INVALID_FILE_ID)
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('BEGIN')
                if is_file_committed(cur, remote_id):
                    raise RemoteFileError('Cannot modify committed remote file [{}]'.format(remote_id), FileServerErrorCode.FILE_IS_COMMITTED)
                modified_timestamp = round(time.time())
                cur.execute(
                    '''
                    UPDATE ps_remote_file 
                    SET modified_timestamp = ? 
                    WHERE remote_id = ? AND removed_epoch IS NULL
                    '''
                , (modified_timestamp, remote_id))
                if cur.rowcount != 1:
                    raise RemoteFileError('No rows updated!')
                self._conn.commit()
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

    def commit_file(self, epoch_no, remote_id):
        if not File.is_valid_file_id(remote_id):
            raise RemoteFileError('Invalid remote file id!', FileServerErrorCode.INVALID_FILE_ID)
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('BEGIN')
                check_valid_epoch(cur, epoch_no)
                if is_file_committed(cur, remote_id):
                    logging.debug('Remote file [{}] is already committed'.format(remote_id))
                    return
                cur.execute(
                    '''
                    UPDATE ps_remote_file 
                    SET created_epoch = ? 
                    WHERE remote_id = ? AND removed_epoch IS NULL
                    '''
                , (epoch_no, remote_id))
                if cur.rowcount != 1:
                    raise RemoteFileError('No rows updated!')
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

    def remove_file(self, epoch_no, remote_id, remove_file_cb=None):
        if not File.is_valid_file_id(remote_id):
            raise RemoteFileError('Invalid remote file id!', FileServerErrorCode.INVALID_FILE_ID)
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('BEGIN')
                check_valid_epoch(cur, epoch_no)
                if is_file_committed(cur, remote_id):
                    cur.execute(
                        '''
                        UPDATE ps_remote_file
                        SET removed_epoch = ?
                        WHERE remote_id = ? AND removed_epoch IS NULL
                        '''
                    , (epoch_no, remote_id))
                else:
                    cur.execute(
                        '''
                        DELETE 
                        FROM ps_remote_file 
                        WHERE remote_id = ?
                        '''
                        , (remote_id,)
                    )
                    
                    if remove_file_cb is not None:
                        logging.debug('Remove uncommitted file [{}]'.format(remote_id))
                        remove_file_cb(remote_id)
                if cur.rowcount != 1:
                    raise RemoteFileError('No rows updated or removed!')
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
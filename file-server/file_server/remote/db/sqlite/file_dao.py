from collections import namedtuple
from ....file import File
from ..file_dao import FileDAO
from ....error import EpochError, RemoteFileError
from .epoch_util import check_current_epoch
from .file_util import is_file_committed
import logging
import time

RemoteFileMetadata = namedtuple('RemoteFileMetadata', ['file_size', 'is_committed', 'created_timestamp', 'modified_timestamp', 'created_epoch', 'removed_epoch'])

class SqliteFileDAO(FileDAO):

    def __init__(self, conn):
        super().__init__(conn)

    def create_file(self, epoch_no, remote_id, file_size):
        if not File.is_valid_file_id(remote_id):
            raise RemoteFileError('Invalid remote file id!')
        cur = self._conn.cursor()
        try:
            try:
                file_timestamp = round(time.time())
                check_current_epoch(cur, epoch_no)
                cur.execute(
                    '''
                    INSERT INTO ps_remote_file (remote_id, file_size, created_timestamp, modified_timestamp, created_epoch) 
                    VALUES (?, ?, ?, ?, ?)
                    '''
                , (remote_id, file_size, file_timestamp, file_timestamp, epoch_no))
                self._conn.commit()
            except EpochError as e:
                logging.error('Epoch error {}'.format(str(e)))
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
    
    def get_file_metadata(self, remote_id):
        cur = self._conn.cursor()
        try:
            try:
                cur.execute(
                    '''
                    SELECT file_size, is_committed, created_timestamp, modified_timestamp, created_epoch, removed_epoch 
                    FROM ps_remote_file 
                    WHERE remote_id = ?
                    '''
                , (remote_id,))
                res = cur.fetchone()
                if res is None:
                    raise RemoteFileError('Remote file [{}] not found'.format(remote_id))
                self._conn.commit()
                return RemoteFileMetadata(
                    res[0],
                    bool(res[1]),
                    time.gmtime(res[2]),
                    time.gmtime(res[3]),
                    res[4],
                    res[5]
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

    def file_modified(self, epoch_no, remote_id):
        cur = self._conn.cursor()
        try:
            try:
                modified_timestamp = round(time.time())
                check_current_epoch(cur, epoch_no)
                if is_file_committed(cur, remote_id, epoch_no=epoch_no):
                    raise RemoteFileError('Cannot modify committed remote file [{}]'.format(remote_id))
                cur.execute(
                    '''
                    UPDATE ps_remote_file 
                    SET modified_timestamp = ? 
                    WHERE remote_id = ?
                    '''
                , (modified_timestamp, remote_id))
                if cur.rowcount != 1:
                    raise RemoteFileError('Remote file [{}] not found'.format(remote_id))
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

    def commit_file(self, epoch_no, remote_id):
        cur = self._conn.cursor()
        try:
            try:
                check_current_epoch(cur, epoch_no)
                if is_file_committed(cur, remote_id, epoch_no=epoch_no):
                    logging.debug('Remote file [{}] is already committed'.format(remote_id))
                    return
                cur.execute(
                    '''
                    SELECT is_committed 
                    FROM ps_remote_file 
                    WHERE remote_id = ? AND created_epoch_no = ?
                    '''
                , (remote_id, epoch_no))
                res = cur.fetchone()
                if res is None:
                    raise RemoteFileError('Remote file [{}] not found'.format(remote_id))
                if bool(res[0]):
                    raise RemoteFileError('Cannot modify committed file [{}]'.format(remote_id))
                cur.execute(
                    '''
                    UPDATE ps_remote_file 
                    SET is_committed = ? 
                    WHERE remote_id = ?
                    '''
                , (1, remote_id))
                if cur.rowcount != 1:
                    raise RemoteFileError('Remote file [{}] not found'.format(remote_id))
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

    def remove_file(self, epoch_no, remote_id):
        cur = self._conn.cursor()
        try:
            try:
                check_current_epoch(cur, epoch_no)
                cur.execute(
                    '''
                    UPDATE ps_remote_file
                    SET removed_epoch_no = ?
                    WHERE remote_id = ?
                    '''
                , (epoch_no, remote_id))
                if cur.rowcount != 1:
                    raise RemoteFileError('Remote file [{}] not found'.format(remote_id))
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
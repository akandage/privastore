from ..file_dao import FileDAO, FileVersionMetadata
from ....error import DirectoryError, FileError, FileServerErrorCode
from .directory_util import query_file_id, traverse_path
from ....file import File
from ...file_type import FileType
from ...file_transfer_status import FileTransferStatus
from ....util.file import str_path
import logging

class SqliteFileDAO(FileDAO):

    def __init__(self, conn):
        super().__init__(conn)

    def get_file_version_metadata(self, path=None, file_name=None, version=None, local_id=None):
        if file_name is not None and len(file_name) == 0:
            raise FileError('File name can\'t be empty!', FileServerErrorCode.FILE_NAME_EMPTY)
        if local_id is not None and not File.is_valid_file_id(local_id):
            raise FileError('Invalid local file id!', FileServerErrorCode.INVALID_FILE_ID)
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('BEGIN')
                if local_id is not None:
                    cur.execute('''
                        SELECT F.file_type, V.version, V.local_id, V.remote_id, 
                            V.file_size, V.size_on_disk, V.total_chunks, 
                            V.uploaded_chunks, V.downloaded_chunks, 
                            V.local_transfer_status, V.remote_transfer_status 
                        FROM ps_file AS F INNER JOIN ps_file_version AS V ON F.id = V.file_id 
                        WHERE V.local_id = ?
                    ''', (local_id,))
                    res = cur.fetchone()
                else:
                    directory_id = traverse_path(cur, path)
                    file_id = query_file_id(cur, directory_id, file_name)
                    if file_id is None:
                        raise FileError('File [{}] not found in path [{}]'.format(file_name, str_path(path)), FileServerErrorCode.FILE_NOT_FOUND)
                    if version is not None:
                        cur.execute('''
                            SELECT F.file_type, V.version, V.local_id, V.remote_id, 
                                V.file_size, V.size_on_disk, V.total_chunks, 
                                V.uploaded_chunks, V.downloaded_chunks, 
                                V.local_transfer_status, V.remote_transfer_status 
                            FROM ps_file AS F INNER JOIN ps_file_version AS V ON F.id = V.file_id 
                            WHERE F.id = ? AND V.version = ?
                        ''', (file_id, version))
                        res = cur.fetchone()
                    else:
                        cur.execute('''
                            SELECT F.file_type, V.version, V.local_id, V.remote_id, 
                                V.file_size, V.size_on_disk, V.total_chunks, 
                                V.uploaded_chunks, V.downloaded_chunks, 
                                V.local_transfer_status, V.remote_transfer_status 
                            FROM ps_file AS F INNER JOIN ps_file_version AS V ON F.id = V.file_id 
                            WHERE F.id = ? 
                            ORDER BY V.version DESC
                        ''', (file_id,))
                        res = cur.fetchone()
                if res is None:
                    raise FileError('File [{}] version [{}] not found!'.format(str_path(path + [file_name]), version), FileServerErrorCode.FILE_VERSION_NOT_FOUND)
                self._conn.commit()
                return FileVersionMetadata(
                    FileType(res[0]),
                    res[1],
                    res[2],
                    res[3],
                    res[4],
                    res[5],
                    res[6],
                    res[7],
                    res[8],
                    FileTransferStatus(res[9]),
                    FileTransferStatus(res[10])
                )
            except DirectoryError as e:
                logging.error('Directory error: {}'.format(str(e)))
                self.rollback_nothrow()
                raise e
            except FileError as e:
                logging.error('File error: {}'.format(str(e)))
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
    
    def update_file_local(self, path, file_name, version, local_id, file_size, size_on_disk, total_chunks, transfer_status = FileTransferStatus.NONE):
        if len(file_name) == 0:
            raise FileError('File name can\'t be empty!', FileServerErrorCode.FILE_NAME_EMPTY)
        if not File.is_valid_file_id(local_id):
            raise FileError('Invalid local file id!', FileServerErrorCode.INVALID_FILE_ID)
        if file_size <= 0:
            raise FileError('File size must be >= 0', FileServerErrorCode.INVALID_FILE_SIZE)
        if size_on_disk < 0:
            raise FileError('File size on disk must be >= 0', FileServerErrorCode.INTERNAL_ERROR)
        if total_chunks < 0:
            raise FileError('File total chunks must >= 0', FileServerErrorCode.INTERNAL_ERROR)
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('BEGIN')
                directory_id = traverse_path(cur, path)
                file_id = query_file_id(cur, directory_id, file_name)
                if file_id is None:
                    raise FileError('File [{}] not found in path [{}]'.format(file_name, str_path(path)), FileServerErrorCode.FILE_NOT_FOUND)
                cur.execute('''
                        UPDATE ps_file_version 
                        SET local_id = ?, file_size = ?, size_on_disk = ?, total_chunks = ?, local_transfer_status = ? 
                        WHERE file_id = ? AND version = ?
                    ''', (local_id, file_size, size_on_disk, total_chunks, transfer_status.value, file_id, version))
                if cur.rowcount != 1:
                    raise FileError('File [{}] version [{}] not found!'.format(str_path(path + [file_name]), version), FileServerErrorCode.FILE_VERSION_NOT_FOUND)
                self._conn.commit()
            except DirectoryError as e:
                logging.error('Directory error: {}'.format(str(e)))
                self.rollback_nothrow()
                raise e
            except FileError as e:
                logging.error('File error: {}'.format(str(e)))
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
    
    def update_file_remote(self, local_id, remote_id = None, transfer_status = FileTransferStatus.NONE, transferred_chunks=0):
        if not File.is_valid_file_id(local_id):
            raise FileError('Invalid local file id!', FileServerErrorCode.INVALID_FILE_ID)
        if remote_id is not None and not File.is_valid_file_id(remote_id):
            raise FileError('Invalid remote file id!', FileServerErrorCode.INVALID_FILE_ID)
        cur = self._conn.cursor()
        try:
            try:
                if remote_id is not None:
                    cur.execute('''
                            UPDATE ps_file_version 
                            SET remote_id = ?, remote_transfer_status = ? 
                            WHERE local_id = ?
                        ''', (remote_id, transfer_status.value, local_id))
                else:
                    cur.execute('''
                            UPDATE ps_file_version 
                            SET remote_transfer_status = ?, uploaded_chunks = ?  
                            WHERE local_id = ?
                        ''', (transfer_status.value, transferred_chunks, local_id))
                if cur.rowcount != 1:
                    raise FileError('File [{}] not found!'.format(local_id), FileServerErrorCode.FILE_VERSION_NOT_FOUND)
                self._conn.commit()
            except DirectoryError as e:
                logging.error('Directory error: {}'.format(str(e)))
                self.rollback_nothrow()
                raise e
            except FileError as e:
                logging.error('File error: {}'.format(str(e)))
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
    
    def update_file_download(self, local_id, transferred_chunks=0):
        if not File.is_valid_file_id(local_id):
            raise FileError('Invalid local file id!', FileServerErrorCode.INVALID_FILE_ID)
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('''
                        UPDATE ps_file_version 
                        SET downloaded_chunks = ?  
                        WHERE local_id = ?
                    ''', (transferred_chunks, local_id))
                if cur.rowcount != 1:
                    raise FileError('File [{}] not found!'.format(local_id), FileServerErrorCode.FILE_VERSION_NOT_FOUND)
                self._conn.commit()
            except DirectoryError as e:
                logging.error('Directory error: {}'.format(str(e)))
                self.rollback_nothrow()
                raise e
            except FileError as e:
                logging.error('File error: {}'.format(str(e)))
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

    def update_file_transfer_status(self, path, file_name, version, local_transfer_status=None, remote_transfer_status=None):
        if len(file_name) == 0:
            raise FileError('File name can\'t be empty!', FileServerErrorCode.FILE_NAME_EMPTY)
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('BEGIN')
                directory_id = traverse_path(cur, path)
                file_id = query_file_id(cur, directory_id, file_name)
                if file_id is None:
                    raise FileError('File [{}] not found in path [{}]'.format(file_name, str_path(path)), FileServerErrorCode.FILE_NOT_FOUND)
                if local_transfer_status is not None:
                    cur.execute('''
                            UPDATE ps_file_version 
                            SET local_transfer_status = ? 
                            WHERE file_id = ? AND version = ?
                        ''', (local_transfer_status.value, file_id, version))
                    if cur.rowcount != 1:
                        raise FileError('File [{}] version [{}] not found!'.format(str_path(path + [file_name]), version), FileServerErrorCode.FILE_VERSION_NOT_FOUND)
                if remote_transfer_status is not None:
                    cur.execute('''
                            UPDATE ps_file_version 
                            SET remote_transfer_status = ? 
                            WHERE file_id = ? AND version = ?
                        ''', (remote_transfer_status.value, file_id, version))
                    if cur.rowcount != 1:
                        raise FileError('File [{}] version [{}] not found!'.format(str_path(path + [file_name]), version), FileServerErrorCode.FILE_VERSION_NOT_FOUND)
                self._conn.commit()
            except DirectoryError as e:
                logging.error('Directory error: {}'.format(str(e)))
                self.rollback_nothrow()
                raise e
            except FileError as e:
                logging.error('File error: {}'.format(str(e)))
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
    
    def remove_file_version(self, local_id: str) -> None:
        if not File.is_valid_file_id(local_id):
            raise FileError('Invalid file id!')
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('BEGIN')
                cur.execute(
                    '''
                        SELECT file_id, version
                        FROM ps_file_version
                        WHERE local_id = ?
                    ''', (local_id,)
                )
                res = cur.fetchone()
                if res is None:
                    raise FileError('File [{}] not found!'.format(local_id), FileServerErrorCode.FILE_VERSION_NOT_FOUND)
                file_id, version = res
                cur.execute(
                    '''
                        SELECT count(*)
                        FROM ps_file_version
                        WHERE file_id = ?
                    ''', (file_id,)
                )
                count, = cur.fetchone()
                cur.execute(
                    '''
                        DELETE 
                        FROM ps_file_version 
                        WHERE file_id = ?, version = ?
                    ''', (file_id, version)
                )
                if cur.rowcount != 1:
                    raise FileError('Could not delete file [{}]'.format(local_id))
                if count == 1:
                    cur.execute(
                        '''
                            DELETE 
                            FROM ps_file
                            WHERE file_id = ?
                        ''', (file_id,)
                    )
                    if cur.rowcount != 1:
                        raise FileError('Could not delete file [{}]'.format(local_id))
                self._conn.commit()
            except DirectoryError as e:
                logging.error('Directory error: {}'.format(str(e)))
                self.rollback_nothrow()
                raise e
            except FileError as e:
                logging.error('File error: {}'.format(str(e)))
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
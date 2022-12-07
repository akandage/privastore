from ..file_dao import FileDAO, FileMetadata, FileVersionMetadata
from ....error import DirectoryError, FileError, FileServerErrorCode, KeyError
from .directory_util import query_file_id, traverse_path
from ....file import File
from ...file_type import FileType
from ...file_transfer_status import FileTransferStatus
from .file_util import get_file_data_id
from ....key import Key
from .key_util import get_key_id
from ....util.file import str_path
import logging

class SqliteFileDAO(FileDAO):

    def __init__(self, conn):
        super().__init__(conn)

    def get_file_version_metadata(self, path, file_name, version=None):
        if file_name is not None and len(file_name) == 0:
            raise FileError('File name can\'t be empty!', FileServerErrorCode.FILE_NAME_EMPTY)
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('BEGIN')
                query = '''
                    SELECT F.file_type, V.version, D.local_id, D.remote_id, 
                        K.name, D.file_size, D.size_on_disk, D.total_chunks, 
                        D.uploaded_chunks, D.downloaded_chunks, 
                        D.local_transfer_status, D.remote_transfer_status 
                    FROM ps_file AS F INNER JOIN ps_file_version AS V ON F.id = V.file_id 
                        INNER JOIN ps_file_data AS D ON V.file_data_id = D.id 
                        INNER JOIN ps_key AS K ON K.id = D.key_id
                '''
                directory_id = traverse_path(cur, path)
                file_id = query_file_id(cur, directory_id, file_name)
                if file_id is None:
                    raise FileError('File [{}] not found in path [{}]'.format(file_name, str_path(path)), FileServerErrorCode.FILE_NOT_FOUND)
                if version is not None:
                    query += ' WHERE F.id = ? AND V.version = ?'
                    cur.execute(query, (file_id, version))
                else:
                    query += ' WHERE F.id = ?'
                    query += ' ORDER BY V.version DESC'
                    cur.execute(query, (file_id,))
                res = cur.fetchone()
                if res is None:
                    raise FileError('File version not found!', FileServerErrorCode.FILE_VERSION_NOT_FOUND)
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
                    res[9],
                    FileTransferStatus(res[10]),
                    FileTransferStatus(res[11])
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

    def get_file_metadata(self, local_id: str) -> 'FileMetadata':
        if not File.is_valid_file_id(local_id):
            raise FileError('Invalid local file id!', FileServerErrorCode.INVALID_FILE_ID)
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('BEGIN')
                cur.execute('''
                    SELECT D.local_id, D.remote_id, 
                        K.name, D.file_size, D.size_on_disk, D.total_chunks, 
                        D.uploaded_chunks, D.downloaded_chunks, 
                        D.local_transfer_status, D.remote_transfer_status 
                    FROM ps_file_data AS D INNER JOIN ps_key AS K ON K.id = D.key_id 
                    WHERE local_id = ?
                ''', (local_id,))
                res = cur.fetchone()
                if res is None:
                    raise FileError('File [{}] not found!'.format(local_id), FileServerErrorCode.FILE_VERSION_NOT_FOUND)
                self._conn.commit()
                return FileMetadata(
                    res[0],
                    res[1],
                    res[2],
                    res[3],
                    res[4],
                    res[5],
                    res[6],
                    res[7],
                    FileTransferStatus(res[8]),
                    FileTransferStatus(res[9])
                )
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

    def list_orphaned_file_data(self) -> list['FileMetadata']:
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('BEGIN')
                cur.execute('''
                    SELECT D.local_id, D.remote_id, D.local_transfer_status, 
                        D.remote_transfer_status 
                    FROM ps_file_data AS D
                    WHERE id NOT IN (
                        SELECT V.file_data_id
                        FROM ps_file_version AS V
                    )
                ''')
                orphaned = list()
                for local_id, remote_id, local_transfer_status, remote_transfer_status in cur.fetchall():
                    # TODO: Return other fields if required.
                    orphaned.append(FileMetadata(
                        local_id,
                        remote_id,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        FileTransferStatus(local_transfer_status),
                        FileTransferStatus(remote_transfer_status)
                    ))
                self._conn.commit()
                return orphaned
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

    def update_file_local(self, path, file_name, version, local_id, key_id, file_size, size_on_disk, total_chunks, transfer_status = FileTransferStatus.NONE):
        if len(file_name) == 0:
            raise FileError('File name can\'t be empty!', FileServerErrorCode.FILE_NAME_EMPTY)
        if not File.is_valid_file_id(local_id):
            raise FileError('Invalid local file id!', FileServerErrorCode.INVALID_FILE_ID)
        if key_id != 'null' and not Key.is_valid_key_id(key_id):
            raise KeyError('Invalid key id!', FileServerErrorCode.INVALID_KEY_ID)
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
                key_id = get_key_id(cur, key_id)
                file_data_id = get_file_data_id(cur, file_id, version)
                cur.execute('''
                        UPDATE ps_file_data
                        SET local_id = ?, key_id = ?, file_size = ?, size_on_disk = ?, downloaded_chunks = ?, total_chunks = ?, local_transfer_status = ? 
                        WHERE id = ?
                    ''', (local_id, key_id, file_size, size_on_disk, total_chunks, total_chunks, transfer_status.value, file_data_id,))
                if cur.rowcount != 1:
                    raise FileError('File [{}] version [{}] metadata could not be updated!'.format(str_path(path + [file_name]), version))
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
                            UPDATE ps_file_data 
                            SET remote_id = ?, remote_transfer_status = ? 
                            WHERE local_id = ?
                        ''', (remote_id, transfer_status.value, local_id))
                else:
                    cur.execute('''
                            UPDATE ps_file_data 
                            SET remote_transfer_status = ?, uploaded_chunks = ?  
                            WHERE local_id = ?
                        ''', (transfer_status.value, transferred_chunks, local_id))
                if cur.rowcount != 1:
                    raise FileError('File [{}] not found!'.format(local_id), FileServerErrorCode.FILE_VERSION_NOT_FOUND)
                self._conn.commit()
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
                        UPDATE ps_file_data  
                        SET downloaded_chunks = ?  
                        WHERE local_id = ?
                    ''', (transferred_chunks, local_id))
                if cur.rowcount != 1:
                    raise FileError('File [{}] not found!'.format(local_id), FileServerErrorCode.FILE_VERSION_NOT_FOUND)
                self._conn.commit()
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
                file_data_id = get_file_data_id(cur, file_id, version)
                if local_transfer_status is not None:
                    cur.execute('''
                            UPDATE ps_file_data 
                            SET local_transfer_status = ? 
                            WHERE id = ?
                        ''', (local_transfer_status.value, file_data_id))
                    if cur.rowcount != 1:
                        raise FileError('File [{}] version [{}] not found!'.format(str_path(path + [file_name]), version), FileServerErrorCode.FILE_VERSION_NOT_FOUND)
                if remote_transfer_status is not None:
                    cur.execute('''
                            UPDATE ps_file_data  
                            SET remote_transfer_status = ? 
                            WHERE id = ?
                        ''', (remote_transfer_status.value, file_data_id))
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
    
    def remove_file_data(self, local_id: str) -> None:
        if not File.is_valid_file_id(local_id):
            raise FileError('Invalid file id!')
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('BEGIN')
                cur.execute(
                    '''
                        DELETE 
                        FROM ps_file_data
                        WHERE local_id = ?
                    ''', (local_id,)
                )
                if cur.rowcount != 1:
                    raise FileError('Could not delete file [{}] data!'.format(local_id))
                self._conn.commit()
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
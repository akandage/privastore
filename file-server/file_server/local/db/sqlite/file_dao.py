from ..file_dao import FileDAO
from ....error import DirectoryError, FileError
from .directory_util import query_directory_id, query_file_id, traverse_path
from ....file import File
from ...file_type import FileType
from ...file_transfer_status import FileTransferStatus
from ....util.file import str_path
import logging

class SqliteFileDAO(FileDAO):

    def __init__(self, conn):
        super().__init__(conn)

    def get_file_version_metadata(self, path, file_name, version=None):
        if len(file_name) == 0:
            raise FileError('File name can\'t be empty!')
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('BEGIN')
                directory_id = traverse_path(cur, path)
                file_id = query_file_id(cur, directory_id, file_name)
                if file_id is None:
                    raise FileError('File [{}] not found in path [{}]'.format(file_name, str_path(path)))
                if version is not None:
                    cur.execute('''
                        SELECT F.file_type, V.version, V.local_id, V.remote_id, V.size_bytes, V.transfer_status 
                        FROM ps_file AS F INNER JOIN ps_file_version AS V ON F.id = V.file_id 
                        WHERE F.id = ? AND V.version = ?
                    ''', (file_id, version))
                    res = cur.fetchone()
                else:
                    cur.execute('''
                        SELECT F.file_type, V.version, V.local_id, V.remote_id, V.size_bytes, V.transfer_status 
                        FROM ps_file AS F INNER JOIN ps_file_version AS V ON F.id = V.file_id 
                        WHERE F.id = ? 
                        ORDER BY V.version DESC
                    ''', (file_id,))
                    res = cur.fetchone()
                if res is None:
                    raise FileError('File [{}] version [{}] not found!'.format(str_path(path + [file_name]), version))
                self._conn.commit()
                return {
                    'file_type': FileType(res[0]),
                    'version': res[1],
                    'local_id': res[2],
                    'remote_id': res[3],
                    'size_bytes': res[4],
                    'transfer_status': FileTransferStatus(res[5])
                }
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
    
    def update_file_local(self, path, file_name, version, local_id, size_bytes):
        if len(file_name) == 0:
            raise FileError('File name can\'t be empty!')
        if not File.is_valid_file_id(local_id):
            raise FileError('Invalid local file id!')
        if size_bytes <= 0:
            raise FileError('File size in bytes must be >= 0')
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('BEGIN')
                directory_id = traverse_path(cur, path)
                file_id = query_file_id(cur, directory_id, file_name)
                if file_id is None:
                    raise FileError('File [{}] not found in path [{}]'.format(file_name, str_path(path)))
                cur.execute('''
                        UPDATE ps_file_version 
                        SET local_id = ?, size_bytes = ?, transfer_status = ? 
                        WHERE file_id = ? AND version = ?
                    ''', (local_id, size_bytes, FileTransferStatus.RECEIVED.value, file_id, version))
                if cur.rowcount != 1:
                    raise FileError('File [{}] version [{}] not found!'.format(str_path(path + [file_name]), version))
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
    
    def update_file_remote(self, path, file_name, version, remote_id, transfer_status):
        if len(file_name) == 0:
            raise FileError('File name can\'t be empty!')
        if not File.is_valid_file_id(remote_id):
            raise FileError('Invalid remote file id!')
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('BEGIN')
                directory_id = traverse_path(cur, path)
                file_id = query_file_id(cur, directory_id, file_name)
                if file_id is None:
                    raise FileError('File [{}] not found in path [{}]'.format(file_name, str_path(path)))
                cur.execute('''
                        UPDATE ps_file_version 
                        SET remote_id = ?, transfer_status = ? 
                        WHERE file_id = ? AND version = ?
                    ''', (remote_id, transfer_status.value, file_id, version))
                if cur.rowcount != 1:
                    raise FileError('File [{}] version [{}] not found!'.format(str_path(path + [file_name]), version))
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
    
    def update_file_transfer_status(self, path, file_name, version, transfer_status):
        if len(file_name) == 0:
            raise FileError('File name can\'t be empty!')
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('BEGIN')
                directory_id = traverse_path(cur, path)
                file_id = query_file_id(cur, directory_id, file_name)
                if file_id is None:
                    raise FileError('File [{}] not found in path [{}]'.format(file_name, str_path(path)))
                cur.execute('''
                        UPDATE ps_file_version 
                        SET transfer_status = ? 
                        WHERE file_id = ? AND version = ?
                    ''', (transfer_status.value, file_id, version))
                if cur.rowcount != 1:
                    raise FileError('File [{}] version [{}] not found!'.format(str_path(path + [file_name]), version))
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
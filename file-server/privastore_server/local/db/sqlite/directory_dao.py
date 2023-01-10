from ..directory_dao import DirectoryDAO
from .directory_util import query_directory_id, query_file_id, traverse_path
from ....error import DirectoryError, FileError, FileServerErrorCode
from ....file import File
from ...file_transfer_status import FileTransferStatus
from ...file_type import FileType
from ....util.file import str_path
import logging
import time

class SqliteDirectoryDAO(DirectoryDAO):

    def __init__(self, conn):
        super().__init__(conn)
    
    def create_directory(self, path, directory_name, is_hidden=False):
        if len(directory_name) == 0:
            raise DirectoryError('Directory name can\'t be empty!', FileServerErrorCode.DIRECTORY_NAME_EMPTY)
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('BEGIN')
                directory_id = traverse_path(cur, path)
                if query_directory_id(cur, directory_id, directory_name, is_hidden) is not None:
                    raise DirectoryError('Directory [{}] exists in path [{}]'.format(directory_name, str_path(path)), FileServerErrorCode.DIRECTORY_EXISTS)
                elif query_file_id(cur, directory_id, directory_name, is_hidden) is not None:
                    raise FileError('File [{}] exists in path [{}]'.format(directory_name, str_path(path)), FileServerErrorCode.FILE_EXISTS)
                cur.execute('INSERT INTO ps_directory (name, is_hidden) VALUES (?, ?)', (directory_name, is_hidden))
                created_directory_id = cur.lastrowid
                cur.execute('INSERT INTO ps_link (parent_id, child_id) VALUES (?, ?)', (directory_id, created_directory_id))
                self._conn.commit()
                return created_directory_id
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
    
    def create_file(self, path, file_name, file_type=FileType.BINARY_DATA, is_hidden=False):
        if len(file_name) == 0:
            raise FileError('File name can\'t be empty!', FileServerErrorCode.FILE_NAME_EMPTY)
        cur = self._conn.cursor()
        try:
            try:
                now = round(time.time())
                cur.execute('BEGIN')
                directory_id = traverse_path(cur, path)
                if query_directory_id(cur, directory_id, file_name, is_hidden) is not None:
                    raise DirectoryError('Directory [{}] exists in path [{}]'.format(file_name, str_path(path)), FileServerErrorCode.DIRECTORY_EXISTS)
                elif query_file_id(cur, directory_id, file_name, is_hidden) is not None:
                    raise FileError('File [{}] exists in path [{}]'.format(file_name, str_path(path)), FileServerErrorCode.FILE_EXISTS)
                cur.execute('INSERT INTO ps_file (name, file_type, parent_id, is_hidden) VALUES (?, ?, ?, ?)', (file_name, file_type.value, directory_id, is_hidden))
                created_file_id = cur.lastrowid
                cur.execute('INSERT INTO ps_file_data (created_timestamp, local_transfer_status, remote_transfer_status) VALUES (?, ?, ?)', 
                    (now, FileTransferStatus.NONE.value, FileTransferStatus.NONE.value))
                file_data_id = cur.lastrowid
                cur.execute('INSERT INTO ps_file_version (file_id, version, created_timestamp, file_data_id) VALUES (?, ?, ?, ?)', 
                    (created_file_id, 1, now, file_data_id))
                self._conn.commit()
                return created_file_id
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

    def remove_file(self, path, file_name, remove_file_cb=None, is_hidden=False):
        if file_name is not None and len(file_name) == 0:
            raise FileError('File name can\'t be empty!', FileServerErrorCode.FILE_NAME_EMPTY)
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('BEGIN')
                directory_id = traverse_path(cur, path)
                if query_directory_id(cur, directory_id, file_name, is_hidden) is not None:
                    raise DirectoryError('[{}] in path [{}] is a directory'.format(file_name, str_path(path)), FileServerErrorCode.FILE_IS_DIRECTORY)
                query_params = (file_name, directory_id, is_hidden)

                if remove_file_cb is not None:
                    cur.execute('''
                        SELECT V.version, D.local_id, D.remote_id 
                        FROM ps_file_version AS V INNER JOIN ps_file AS F ON V.file_id = F.id
                            INNER JOIN ps_file_data AS D ON V.file_data_id = D.id
                        WHERE F.name = ? AND F.parent_id = ? AND (is_hidden <> 1 OR is_hidden = ?)
                    ''', query_params)
                    remove_file_cb_args = cur.fetchall()

                cur.execute('''
                    DELETE FROM ps_file
                    WHERE name = ? AND parent_id = ? AND (is_hidden <> 1 OR is_hidden = ?)
                ''', query_params)
                logging.debug('Remove file [{}] affected {} rows'.format(str_path(path + [file_name]), cur.rowcount))

                if cur.rowcount == 0:
                    raise FileError('File [{}] not found!'.format(str_path(path + [file_name])), FileServerErrorCode.FILE_NOT_FOUND)

                if remove_file_cb is not None:
                    for version, local_id, remote_id in remove_file_cb_args:
                        remove_file_cb(path, file_name, version, local_id, remote_id)
                
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

    def list_directory(self, path, show_hidden=False):
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('BEGIN')
                directory_id = traverse_path(cur, path)
                entries = []
                cur.execute('''SELECT D.name 
                    FROM ps_directory AS D INNER JOIN ps_link AS L ON D.id = L.child_id 
                    WHERE L.parent_id = ? AND (D.is_hidden <> 1 OR D.is_hidden = ?) 
                    ORDER BY D.name ASC''', (directory_id, show_hidden,))
                for directory_name in cur.fetchall():
                    entries.append(('d', directory_name[0]))
                cur.execute('''SELECT F.name 
                    FROM ps_file AS F INNER JOIN ps_file_version AS V ON F.id = V.file_id
                        INNER JOIN ps_file_data AS D on V.file_data_id = D.id
                    WHERE F.parent_id = ? AND (F.is_hidden <> 1 OR F.is_hidden = ?) AND D.local_transfer_status <> ? 
                    ORDER BY F.name ASC''', (directory_id, show_hidden, FileTransferStatus.TRANSFERRING_DATA.value))
                for file_name in cur.fetchall():
                    entries.append(('f', file_name[0]))
                self._conn.commit()
                return entries
            except DirectoryError as e:
                logging.error('Directory error: {}'.format(str(e)))
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
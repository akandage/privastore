from ..directory_dao import DirectoryDAO
from .directory_util import query_directory_id, query_file_id, traverse_path
from ....error import DirectoryError, FileError
from ...file_transfer_status import FileTransferStatus
from ....util.file import str_path
import logging

class SqliteDirectoryDAO(DirectoryDAO):

    def __init__(self, conn):
        super().__init__(conn)
    
    def create_directory(self, path, directory_name, is_hidden=False):
        if len(directory_name) == 0:
            raise DirectoryError('Directory name can\'t be empty!')
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('BEGIN')
                directory_id = traverse_path(cur, path)
                if query_directory_id(cur, directory_id, directory_name) is not None:
                    raise DirectoryError('Directory [{}] exists in path [{}]'.format(directory_name, str_path(path)))
                elif query_file_id(cur, directory_id, directory_name) is not None:
                    raise FileError('File [{}] exists in path [{}]'.format(directory_name, str_path(path)))
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
    
    def create_file(self, path, file_name, is_hidden=False):
        if len(file_name) == 0:
            raise FileError('File name can\'t be empty!')
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('BEGIN')
                directory_id = traverse_path(cur, path)
                if query_directory_id(cur, directory_id, file_name) is not None:
                    raise DirectoryError('Directory [{}] exists in path [{}]'.format(file_name, str_path(path)))
                elif query_file_id(cur, directory_id, file_name) is not None:
                    raise FileError('File [{}] exists in path [{}]'.format(file_name, str_path(path)))
                cur.execute('INSERT INTO ps_file (name, parent_id, is_hidden) VALUES (?, ?, ?)', (file_name, directory_id, is_hidden))
                created_file_id = cur.lastrowid
                cur.execute('INSERT INTO ps_file_version (file_id, version, size_bytes, transfer_status) VALUES (?, ?, ?, ?)', 
                    (created_file_id, 1, 0, FileTransferStatus.EMPTY.value))
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

    def list_directory(self, path, show_hidden=False):
        cur = self._conn.cursor()
        try:
            try:
                cur.execute('BEGIN')
                directory_id = traverse_path(cur, path)
                entries = []
                cur.execute('''SELECT name 
                    FROM ps_directory AS D INNER JOIN ps_link AS L ON D.id = L.child_id 
                    WHERE L.parent_id = ? AND (D.is_hidden <> 1 OR D.is_hidden = ?) 
                    ORDER BY D.name ASC''', (directory_id, show_hidden,))
                for directory_name in cur.fetchall():
                    entries.append(('d', directory_name[0]))
                cur.execute('''SELECT F.name 
                    FROM ps_file AS F INNER JOIN ps_file_version AS V ON F.id = V.file_id
                    WHERE F.parent_id = ? AND (F.is_hidden <> 1 OR F.is_hidden = ?) AND V.transfer_status <> ?
                    ORDER BY name ASC''', (directory_id, show_hidden, FileTransferStatus.RECEIVING_FAILED.value))
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
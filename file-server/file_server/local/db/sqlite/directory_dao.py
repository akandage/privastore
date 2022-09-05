from ..directory_dao import DirectoryDAO
from ....error import DirectoryError
import logging

class SqliteDirectoryDAO(DirectoryDAO):

    def __init__(self, conn):
        super().__init__(conn)
    
    def query_directory_id(self, cur, parent_directory_id, directory_name):
        cur.execute('''
            SELECT L.child_id FROM ps_directory AS D INNER JOIN ps_link AS L ON L.child_id = D.id 
            WHERE L.parent_id = ? AND D.name = ?
        ''', (parent_directory_id, directory_name))
        res = cur.fetchone()
        if res is None:
            return None
        return res[0]

    def traverse_path(self, cur, path):
        # Start from root directory and iterate to the last directory in the path.
        #
        directory_id = 1
        for directory_name in path:
            directory_id = self.query_directory_id(cur, directory_id, directory_name)
            if directory_id is None:
                raise DirectoryError('Invalid path to directory [{}]'.format('/' + '/'.join(path)))
        return directory_id

    def create_directory(self, path, directory_name, is_hidden=False):
        if len(directory_name) == 0:
            raise DirectoryError('Directory name can\'t be empty!')
        cur = self._conn.cursor()
        try:
            try:
                directory_id = self.traverse_path(cur, path)
                if self.query_directory_id(cur, directory_id, directory_name) is not None:
                    raise DirectoryError('Directory [{}] exists in path [{}]'.format(directory_name, '/' + '/'.join(path)))
                cur.execute('INSERT INTO ps_directory (name, is_hidden) VALUES (?, ?)', (directory_name, is_hidden))
                cur.execute('INSERT INTO ps_link (parent_id, child_id) VALUES (?, last_insert_rowid())', (directory_id,))
                self._conn.commit()
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
    
    def list_directory(self, path, show_hidden=False):
        cur = self._conn.cursor()
        try:
            try:
                directory_id = self.traverse_path(cur, path)
                entries = []
                cur.execute('''SELECT name 
                    FROM ps_directory AS D INNER JOIN ps_link AS L ON D.id = L.child_id 
                    WHERE L.parent_id = ? AND (D.is_hidden <> 1 OR D.is_hidden = ?) 
                    ORDER BY D.name ASC''', (directory_id, show_hidden,))
                for directory_name in cur.fetchall():
                    entries.append(('d', directory_name[0]))
                cur.execute('''SELECT name FROM ps_file 
                    WHERE parent_id = ? AND (is_hidden <> 1 OR is_hidden = ?)
                    ORDER BY name ASC''', (directory_id, show_hidden))
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
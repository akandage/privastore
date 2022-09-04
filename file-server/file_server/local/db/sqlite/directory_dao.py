from ..directory_dao import DirectoryDAO
from ....error import DirectoryError
import logging

class SqliteDirectoryDAO(DirectoryDAO):

    def __init__(self, conn):
        super().__init__(conn)
    
    def create_directory(self, path, directory_name, is_hidden=False):
        cur = self._conn.cursor()
        try:
            try:
                # Start from root directory and iterate to the last directory in the path.
                #
                directory_id = 1
                for directory in path:
                    cur.execute('''
                        SELECT L.child_id FROM ps_directory AS D INNER JOIN ps_link AS L ON L.child_id = D.directory_id 
                        WHERE L.parent_id = ? AND D.name = ?
                    ''', (directory_id, directory))
                    res = cur.fetchone()
                    if res is None:
                        raise DirectoryError('Invalid path to directory [{}]'.format('/' + path.join('/')))
                    directory_id = res,

                cur.execute('INSERT INTO ps_directory (name, is_hidden) VALUES (?, ?)', (directory_name, is_hidden))
                cur.execute('INSERT INTO ps_link (parent_id, child_id) VALUES (?, last_insert_rowid())', (directory_id,))
                self._conn.commit()
            except DirectoryError as e:
                raise e
            except Exception as e:
                logging.error('Query error {}'.format(str(e)))
                try:
                    self._conn.rollback()
                    logging.debug('Rolled back')
                except Exception as e1:
                    logging.error('Error rolling back {}'.format(str(e1)))
                raise e
        finally:
            try:
                cur.close()
            except:
                pass
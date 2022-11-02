from collections import namedtuple
from ..file_dao import FileDAO
from ....error import RemoteFileError
import logging
import time

class SqliteFileDAO(FileDAO):

    def __init__(self, conn):
        super().__init__(conn)
    
    def create_file(self, remote_id, file_size):
        cur = self._conn.cursor()
        try:
            try:
                file_timestamp = round(time.time())
                cur.execute(
                    '''
                    INSERT INTO ps_remote_file (remote_id, file_size, created_timestamp, modified_timestamp)
                    VALUES (?, ?, ?, ?)
                    '''
                , (remote_id, file_size, file_timestamp, file_timestamp))
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
        raise Exception('Not implemented!')

    def file_modified(self, remote_id):
        raise Exception('Not implemented!')

    def commit_file(self, remote_id):
        raise Exception('Not implemented!')

    def remove_file(self, remote_id):
        raise Exception('Not implemented!')
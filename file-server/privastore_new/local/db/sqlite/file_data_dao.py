import logging
import time

from ....db.conn import SqliteConnection
from ....db.dao import DataAccessObject
from ....db.file_data_dao import FileDataDAO
from ....error import FileError
from ....file import FileData

class SqliteFileDataDAO(DataAccessObject, FileDataDAO):

    def __init__(self, conn: SqliteConnection):
        super().__init__(conn)
    
    def conn(self) -> SqliteConnection:
        return super().conn()

    def create_file_data(self, uid: str) -> int:
        self.begin_transaction()
        cur = self.conn().cursor()
        try:
            now = round(time.time())
            cur.execute('''
                INSERT INTO ps_file_data (uid, created_timestamp, modified_timestamp) VALUES (?, ?, ?)
            ''', (uid, now, now))
            fd_id = cur.lastrowid
            self.commit()
            return fd_id
        except FileError as e:
            logging.error('File error: {}'.format(str(e)))
            self.rollback_nothrow()
            raise e
        except Exception as e:
            logging.error('Query error: {}'.format(str(e)))
            self.rollback_nothrow()
            raise e
        finally:
            try:
                cur.close()
            except:
                pass

    def get_file_data(self, uid: str) -> FileData:
        self.begin_transaction()
        cur = self.conn().cursor()
        try:
            cur.execute('''
                SELECT id, size, total_chunks, created_timestamp, modified_timestamp, is_writable, is_synced
                FROM ps_file_data
                WHERE uid = ?
            ''', (uid,))
            res = cur.fetchone()
            if res is None:
                raise FileError('File data [{}] not found'.format(uid), FileError.FILE_DATA_NOT_FOUND)
            id, size, total_chunks, created_timestamp, modified_timestamp, is_writable, is_synced = res
            self.commit()
            return FileData(
                id,
                size,
                total_chunks,
                created_timestamp,
                modified_timestamp,
                is_writable,
                is_synced
            )
        except FileError as e:
            logging.error('File error: {}'.format(str(e)))
            self.rollback_nothrow()
            raise e
        except Exception as e:
            logging.error('Query error: {}'.format(str(e)))
            self.rollback_nothrow()
            raise e
        finally:
            try:
                cur.close()
            except:
                pass
    
    def append_chunk(self, fd_id: int, chunk_data: bytes) -> int:
        if len(chunk_data) == 0:
            raise FileError('Chunk cannot be empty')

        self.begin_transaction()
        cur = self.conn().cursor()
        try:
            cur.execute('''
                SELECT is_writable
                FROM ps_file_data
                WHERE id = ?
            ''', (fd_id,))
            res = cur.fetchone()
            if res is None:
                raise FileError('File data id [{}] not found', FileError.FILE_DATA_NOT_FOUND)
            elif not res[0]:
                raise FileError('File data id [{}] not writable', FileError.FILE_DATA_NOT_WRITABLE)
            cur.execute('''
                SELECT max(chunk_id)
                FROM ps_file_chunk
                WHERE fd_id = ?
            ''', (fd_id,))
            res = cur.fetchone()
            if res is not None and res[0] is not None:
                chunk_id = res[0]+1
            else:
                chunk_id = 1
            cur.execute('''
                INSERT INTO ps_file_chunk (fd_id, chunk_id, chunk_data) VALUES (?, ?, ?)
            ''', (fd_id, chunk_id, chunk_data))
            now = round(time.time())
            cur.execute('''
                UPDATE ps_file_data
                SET size = size + ?, total_chunks = total_chunks + 1, created_timestamp = ?, modified_timestamp = ?
                WHERE id = ?
            ''', (len(chunk_data), now, now, fd_id))
            self.commit()
            return chunk_id
        except FileError as e:
            logging.error('File error: {}'.format(str(e)))
            self.rollback_nothrow()
            raise e
        except Exception as e:
            logging.error('Query error: {}'.format(str(e)))
            self.rollback_nothrow()
            raise e
        finally:
            try:
                cur.close()
            except:
                pass
    
    def read_chunk(self, fd_id: int, chunk_id: int) -> bytes:
        self.begin_transaction()
        cur = self.conn().cursor()
        try:
            cur.execute('''
                SELECT chunk_data
                FROM ps_file_chunk
                WHERE fd_id = ? AND chunk_id = ?
            ''', (fd_id, chunk_id))
            res = cur.fetchone()
            if res is None:
                return b''
            chunk_data, = res
            self.commit()
            return chunk_data
        except FileError as e:
            logging.error('File error: {}'.format(str(e)))
            self.rollback_nothrow()
            raise e
        except Exception as e:
            logging.error('Query error: {}'.format(str(e)))
            self.rollback_nothrow()
            raise e
        finally:
            try:
                cur.close()
            except:
                pass
    
    def set_file_data_synced(self, fd_id: int, is_synced: bool) -> None:
        self.begin_transaction()
        cur = self.conn().cursor()
        try:
            cur.execute('''
                UPDATE ps_file_data
                SET is_synced = ?
                WHERE id = ?
            ''', (is_synced, fd_id))
            if cur.rowcount != 1:
                raise FileError('File data id [{}] not found'.format(fd_id), FileError.FILE_DATA_NOT_FOUND)
            self.commit()
        except FileError as e:
            logging.error('File error: {}'.format(str(e)))
            self.rollback_nothrow()
            raise e
        except Exception as e:
            logging.error('Query error: {}'.format(str(e)))
            self.rollback_nothrow()
            raise e
        finally:
            try:
                cur.close()
            except:
                pass
    
    def set_file_data_writable(self, fd_id: int, is_writable: bool) -> None:
        self.begin_transaction()
        cur = self.conn().cursor()
        try:
            cur.execute('''
                UPDATE ps_file_data
                SET is_writable = ?
                WHERE id = ?
            ''', (is_writable, fd_id))
            if cur.rowcount != 1:
                raise FileError('File data id [{}] not found'.format(fd_id), FileError.FILE_DATA_NOT_FOUND)
            self.commit()
        except FileError as e:
            logging.error('File error: {}'.format(str(e)))
            self.rollback_nothrow()
            raise e
        except Exception as e:
            logging.error('Query error: {}'.format(str(e)))
            self.rollback_nothrow()
            raise e
        finally:
            try:
                cur.close()
            except:
                pass
    
    def remove_file_data(self, uid: str) -> None:
        self.begin_transaction()
        cur = self.conn().cursor()
        try:
            cur.execute('''
                DELETE FROM ps_file_data
                WHERE uid = ?
            ''', (uid,))
            if cur.rowcount != 1:
                raise FileError('Could not delete file data [{}]. File data not found'.format(uid))
            self.commit()
        except FileError as e:
            logging.error('File error: {}'.format(str(e)))
            self.rollback_nothrow()
            raise e
        except Exception as e:
            logging.error('Query error: {}'.format(str(e)))
            self.rollback_nothrow()
            raise e
        finally:
            try:
                cur.close()
            except:
                pass
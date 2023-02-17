import json
import logging

from ....db.conn import SqliteConnection
from ....db.dao import DataAccessObject
from ....error import LogError
from ..log_dao import LogDAO
from ....log.log_entry import LogEntry

class SqliteLogDAO(DataAccessObject, LogDAO):

    CHUNK_SIZE = 1024

    def __init__(self, conn: SqliteConnection):
        super().__init__(conn)
    
    def conn(self) -> SqliteConnection:
        return super().conn()

    def get_first_seq_no(self) -> int:
        self.begin_transaction()
        cur = self.conn().cursor()
        try:
            res = cur.execute('''
                SELECT min(seq_no)
                FROM ps_log
            ''')
            seq_no = 0
            if res is not None:
                seq_no, = res
            self.commit()
            return seq_no
        except LogError as e:
            logging.error('Log error: {}'.format(str(e)))
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
    
    def get_last_seq_no(self) -> int:
        self.begin_transaction()
        cur = self.conn().cursor()
        try:
            res = cur.execute('''
                SELECT max(seq_no)
                FROM ps_log
            ''')
            seq_no = 0
            if res is not None:
                seq_no, = res
            self.commit()
            return seq_no
        except LogError as e:
            logging.error('Log error: {}'.format(str(e)))
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
    
    def create_log_entry(self, entry: LogEntry) -> int:
        self.begin_transaction()
        cur = self.conn().cursor()
        try:
            cur.execute('''
                INSERT INTO ps_log (entry_type) VALUES (?)
            ''', (entry.type().value,))
            seq_no = cur.lastrowid
            entry_bytes = json.dumps(entry.entry()).encode('utf-8')
            chunk_id = 1
            for offset in range(0, len(entry_bytes), self.CHUNK_SIZE):
                cur.execute('''
                    INSERT INTO ps_log_entry (seq_no, chunk_id, chunk_data) VALUES (?, ?, ?)
                ''', (seq_no, chunk_id, entry_bytes[offset:offset+self.CHUNK_SIZE]))
                chunk_id += 1
            self.commit()
            return seq_no
        except LogError as e:
            logging.error('Log error: {}'.format(str(e)))
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
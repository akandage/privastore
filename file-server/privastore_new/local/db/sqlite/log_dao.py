import json
import logging

from ....db.conn import SqliteConnection
from ....db.dao import DataAccessObject
from ....error import LogError
from ....db.log_dao import LogDAO
from ....log.log_entry import LogEntry
from ....log.log_entry_type import LogEntryType

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
            if res is not None and res[0] is not None:
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
            if res is not None and res[0] is not None:
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

    def get_log_entries(self, min_seq_no: int) -> list[LogEntry]:
        self.begin_transaction()
        cur = self.conn().cursor()
        try:
            log_entries = list()
            cur.execute('''
                SELECT L.seq_no, LE.chunk_id, L.entry_type, LE.chunk_data
                FROM ps_log AS L INNER JOIN ps_log_entry AS LE ON L.seq_no = LE.seq_no
                WHERE L.seq_no >= ?
                ORDER BY L.seq_no ASC, LE.chunk_id ASC
            ''', (min_seq_no,))

            curr_seq_no = -1
            curr_entry_type = None
            chunk = None

            while True:
                res = cur.fetchone()

                if res is None:
                    if curr_seq_no != -1:
                        log_entries.append(LogEntry(curr_entry_type, json.loads(str(chunk, 'utf-8')), curr_seq_no))
                    break

                seq_no, _, entry_type, chunk_data = res
                if curr_seq_no == -1 or seq_no != curr_seq_no:
                    if curr_seq_no != -1:
                        log_entries.append(LogEntry(curr_entry_type, json.loads(str(chunk, 'utf-8')), curr_seq_no))
                    curr_seq_no = seq_no
                    curr_entry_type = LogEntryType(entry_type)
                    chunk = bytearray()
                chunk += chunk_data

            self.commit()
            return log_entries
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
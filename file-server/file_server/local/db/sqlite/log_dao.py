import logging
from ....error import EpochError, FileServerErrorCode, LogError
from ..log_dao import LogDAO
import log_util

class SqliteLogDAO(LogDAO):

    def __init__(self, conn):
        super().__init__(conn)
    
    def get_first_epoch(self):
        cur = self._conn.cursor()
        try:
            try:
                return log_util.get_first_epoch(cur)
            except EpochError as e:
                logging.error('Epoch error {}'.format(str(e)))
                self.rollback_nothrow()
                raise e
            except LogError as e:
                logging.error('Log error {}'.format(str(e)))
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

    def get_current_epoch(self):
        cur = self._conn.cursor()
        try:
            try:
                return log_util.get_current_epoch(cur)
            except EpochError as e:
                logging.error('Epoch error {}'.format(str(e)))
                self.rollback_nothrow()
                raise e
            except LogError as e:
                logging.error('Log error {}'.format(str(e)))
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
    
    def end_current_epoch(self):
        cur = self._conn.cursor()
        try:
            try:
                log_util.end_current_epoch(cur)
            except EpochError as e:
                logging.error('Epoch error {}'.format(str(e)))
                self.rollback_nothrow()
                raise e
            except LogError as e:
                logging.error('Log error {}'.format(str(e)))
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

    def add_log_entry(self, type, entry):
        cur = self._conn.cursor()
        try:
            try:
                return log_util.add_log_entry(cur, type, entry)
            except EpochError as e:
                logging.error('Epoch error {}'.format(str(e)))
                self.rollback_nothrow()
                raise e
            except LogError as e:
                logging.error('Log error {}'.format(str(e)))
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
    
    def get_log_entry(self, seq_no):
        cur = self._conn.cursor()
        try:
            try:
                return log_util.get_log_entry(cur, seq_no)
            except EpochError as e:
                logging.error('Epoch error {}'.format(str(e)))
                self.rollback_nothrow()
                raise e
            except LogError as e:
                logging.error('Log error {}'.format(str(e)))
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
    
    def get_epoch_log_entries(self, epoch_no):
        pass
    
    def truncate_log(self, epoch_no):
        pass
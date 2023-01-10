import logging
import sqlite3

class DataAccessObject(object):

    def __init__(self, conn: sqlite3.Connection):
        super().__init__()
        self._conn = conn
    
    def commit(self):
        logging.debug('Committing transaction')
        self._conn.commit()
        logging.debug('Committed transaction')
    
    def rollback(self):
        logging.debug('Rolling back transaction')
        self._conn.rollback()
        logging.debug('Rolled back transaction')
    
    def rollback_nothrow(self):
        try:
            self.rollback()
        except Exception as e:
            logging.error('Error rolling back transaction: {}'.format(str(e)))

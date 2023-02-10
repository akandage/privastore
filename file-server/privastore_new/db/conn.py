import logging
import sqlite3

from ..error import NotImplementedError

class DbConnection(object):

    def __init__(self):
        super().__init__()
        self._autocommit = True
    
    def begin_transaction(self):
        raise NotImplementedError()
    
    def set_autocommit(self, autocommit: bool=True):
        self._autocommit = autocommit
    
    def is_autocommit(self) -> bool:
        return self._autocommit
    
    def commit(self):
        raise NotImplementedError()
    
    def rollback(self):
        raise NotImplementedError()
    
    def rollback_nothrow(self):
        raise NotImplementedError()

class SqliteConnection(DbConnection):

    def __init__(self, conn: sqlite3.Connection):
        super().__init__()
        self._conn = conn
    
    def begin_transaction(self):
        self._conn.execute('BEGIN')
    
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
            logging.debug('Rolling back transaction')
            self.rollback()
            logging.debug('Rolled back transaction')
        except Exception as e:
            logging.error('Error rolling back transaction: {}'.format(str(e)))
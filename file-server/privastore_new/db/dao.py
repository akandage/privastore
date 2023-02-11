import logging

from .conn import DbConnection

class DataAccessObject(object):

    def __init__(self, conn: DbConnection):
        super().__init__()
        self._conn = conn
    
    def conn(self) -> DbConnection:
        return self._conn

    def begin_transaction(self):
        autocommit = self.conn().is_autocommit()

        if autocommit:
            logging.debug('Begin DAO transaction, auto-commit is enabled')
        else:
            logging.debug('Begin DAO transaction, auto-commit is disabled')

        if self.conn().is_autocommit():
            self.conn().begin_transaction()
    
    def commit(self):
        if self.conn().is_autocommit():
            self.conn().commit()

    def rollback(self):
        if self.conn().is_autocommit():
            self.conn().rollback()
    
    def rollback_nothrow(self):
        if self.conn().is_autocommit():
            self.conn().rollback_nothrow()

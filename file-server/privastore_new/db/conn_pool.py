from .conn import DbConnection
from ..error import DatabaseError
import logging
from ..pool import Pool
from typing import Callable

class DbConnectionPool(Pool):

    def __init__(self, factory: Callable[[], DbConnection], capacity: int=1, timeout: float=1):
        super().__init__(factory, capacity)
        self._timeout = timeout
        logging.debug('Database connection pool size: {}'.format(capacity))
        logging.debug('Database connection pool timeout: {}s'.format(timeout))
    
    def acquire(self, timeout=None) -> DbConnection:
        if timeout is None:
            timeout = self._timeout
        conn: DbConnection = super().acquire(timeout)
        if conn is None:
            raise DatabaseError('Timed out acquiring database connection from pool', DatabaseError.CONNECTION_POOL_TIMEOUT)
        conn.set_autocommit(True)
        return conn
    
    def try_acquire(self) -> DbConnection:
        return super().try_acquire()
    
    def release(self, conn: DbConnection):
        return super().release(conn)
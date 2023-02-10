from .conn import DbConnection
from ..error import DatabaseError
from ..pool import Pool
from typing import Callable

class DbConnectionPool(Pool):

    def __init__(self, factory: Callable[[], DbConnection], capacity: int=1):
        super().__init__(factory, capacity)
    
    def acquire(self, timeout=None) -> DbConnection:
        conn = super().acquire(timeout)
        if conn is None:
            raise DatabaseError('Timed out acquiring database connection from pool', DatabaseError.CONNECTION_POOL_TIMEOUT)
        return conn
    
    def try_acquire(self) -> DbConnection:
        return super().try_acquire()
    
    def release(self, conn: DbConnection):
        return super().release(conn)
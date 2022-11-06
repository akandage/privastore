from collections.abc import Callable
import logging
from ..pool import Pool
import sqlite3
from typing import Optional

class DbConnectionManager(object):

    def __init__(self, db_config, conn_factory: Callable[[], sqlite3.Connection]=None):
        self._conn_pool_size = conn_pool_size = int(db_config.get('connection-pool-size', 5))
        self._connection_pool_timeout = int(db_config.get('connection-pool-timeout', 30))
        self._conn_factory = conn_factory
        if conn_pool_size > 0:
            logging.debug('Initializing database connection pool with {} connections'.format(conn_pool_size))
            self._conn_pool = Pool(conn_factory, conn_pool_size)
            logging.debug('Initialized database connection pool')
        else:
            self._conn_pool = None
        
    def conn_factory(self) -> Callable[[], sqlite3.Connection]:
        return self._conn_factory
    
    def conn_pool(self) -> Optional[Pool]:
        return self._conn_pool

    def db_connect(self) -> sqlite3.Connection:
        if self._conn_pool:
            conn = self._conn_pool.acquire(timeout=self._connection_pool_timeout)
            if conn is None:
                raise Exception('Database connection pool timeout')
        else:
            conn = self._conn_factory()
        return conn

    def db_close(self, conn: sqlite3.Connection):
        if self._conn_pool:
            self._conn_pool.release(conn)
        else:
            conn.close()
import logging
from queue import Empty, LifoQueue

class DbConnectionPool(object):

    '''
        Database connection pool, thread-safe.

        maxconns - maximum number of concurrent connections.
        factory - connection factory method.
    '''
    def __init__(self, factory, maxconns=1):
        super().__init__()

        self._factory = factory
        self._maxconns = maxconns
        self._conns = LifoQueue(maxconns)

        for _ in range(maxconns):
            self._conns.put(factory())
    
    def acquire_conn(self, timeout=None):
        return self._conns.get(timeout=timeout)

    '''
        Try to acquire connection without blocking.
        If can't grab a connection from the pool immediately, return None.
    '''
    def try_acquire_conn(self):
        try:
            return self._conns.get(block=False)
        except Empty:
            return None

    def release_conn(self, conn):
        self._conns.put(conn, block=False)
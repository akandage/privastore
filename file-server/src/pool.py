import logging
from queue import Empty, LifoQueue

class Pool(object):

    '''
        Thread-safe pool for objects.

        capacity - maximum number of objects in pool.
        factory - connection factory method.
    '''
    def __init__(self, factory, capacity=1):
        super().__init__()

        self._factory = factory
        self._capacity = capacity
        self._pool = LifoQueue(capacity)

        for _ in range(capacity):
            self._pool.put(factory())
    
    def acquire(self, timeout=None):
        try:
            return self._pool.get(timeout=timeout)
        except Empty:
            return None

    '''
        Try to acquire object without blocking.
        If can't grab a object from the pool immediately, return None.
    '''
    def try_acquire(self):
        try:
            return self._pool.get(block=False)
        except Empty:
            return None

    def release(self, obj):
        self._pool.put(obj, block=False)
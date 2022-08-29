import unittest
from pool import Pool

class TestPool(unittest.TestCase):
    
    def setUp(self):
        self.factory = lambda: object()

    def tearDown(self):
        pass

    def test_acquire(self):
        pool = Pool(self.factory, 3)
        obj1 = pool.acquire(timeout=0.001)
        obj2 = pool.acquire(timeout=0.001)
        obj3 = pool.acquire(timeout=0.001)
        
        self.assertIsNotNone(obj1)
        self.assertIsNotNone(obj2)
        self.assertIsNotNone(obj3)
        self.assertTrue(obj1 != obj2 and obj1 != obj3 and obj2 != obj3, 'Pool objects not unique')
        self.assertIsNone(pool.acquire(timeout=0.001))

    def test_try_acquire(self):
        pool = Pool(self.factory, 3)
        obj1 = pool.try_acquire()
        obj2 = pool.try_acquire()
        obj3 = pool.try_acquire()
        
        self.assertIsNotNone(obj1)
        self.assertIsNotNone(obj2)
        self.assertIsNotNone(obj3)
        self.assertTrue(obj1 != obj2 and obj1 != obj3 and obj2 != obj3, 'Pool objects not unique')
        self.assertIsNone(pool.try_acquire())

    def test_release(self):
        pool = Pool(self.factory, 3)
        obj1 = pool.try_acquire()
        obj2 = pool.try_acquire()
        obj3 = pool.try_acquire()

        self.assertIsNone(pool.try_acquire())
        pool.release(obj3)
        obj4 = pool.try_acquire()
        self.assertIsNotNone(obj4)
        self.assertEqual(obj3, obj4)
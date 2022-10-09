import os
import random
import shutil
import unittest
from .file_cache import FileCache

class TestFileCache(unittest.TestCase):
    
    def setUp(self):
        cache_config = {
            'cache-path': 'test_file_cache'
        }
        self.cache = FileCache(cache_config)

    def tearDown(self):
        try:
            shutil.rmtree(self.cache.cache_path())
        except:
            pass
    
    def test_write_file(self):
        f1 = self.cache.open_file(mode='w')
        f2 = self.cache.open_file(mode='w')

        self.assertNotEqual(f1.file_id(), f2.file_id())
        chunk1 = random.randbytes(100)
        chunk2 = random.randbytes(50)
        chunk3 = random.randbytes(50)
        f1.append_chunk(chunk1)
        f1.append_chunk(chunk2)
        f2.append_chunk(chunk3)

        self.cache.close_file(f1)
        self.cache.close_file(f2)
        self.assertEqual(self.cache.cache_used(), 200)

        res = self.cache.open_file(file_id=f1.file_id(), mode='r')
        self.assertTrue(res['file'] is not None)
        f3 = res['file']
        self.assertEqual(f3.read_chunk(), chunk1)
        self.assertEqual(f3.read_chunk(), chunk2)
        self.assertTrue(f3.read_chunk() is None)
        self.cache.close_file(f3)

        res = self.cache.open_file(file_id=f2.file_id(), mode='r')
        self.assertTrue(res['file'] is not None)
        f4 = res['file']
        self.assertEqual(f4.read_chunk(), chunk3)
        self.assertTrue(f4.read_chunk() is None)
        self.cache.close_file(f4)

    def test_append_file(self):
        f1 = self.cache.open_file(mode='w')
        self.cache.close_file(f1, readable=False, writable=True, removable=False)

        self.assertEqual(f1.file_size(), 0)
        self.assertEqual(f1.size_on_disk(), 0)

        res = self.cache.open_file(file_id=f1.file_id(), mode='a')
        self.assertTrue(res['file'] is not None)
        f2 = res['file']
        chunk1 = random.randbytes(100)
        chunk2 = random.randbytes(50)
        f2.append_chunk(chunk1)
        f2.append_chunk(chunk2)
        self.cache.close_file(f2)

        res = self.cache.open_file(file_id=f1.file_id(), mode='r')
        self.assertTrue(res['file'] is not None)
        f3 = res['file']
        self.assertEqual(f3.read_chunk(), chunk1)
        self.assertEqual(f3.read_chunk(), chunk2)
        self.assertTrue(f3.read_chunk() is None)
        self.cache.close_file(f3)
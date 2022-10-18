import os
import random
import shutil
import unittest

from .error import FileCacheError
from .file import File
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

        f3 = self.cache.read_file(f1.file_id())
        self.assertTrue(f3 is not None)
        self.assertEqual(f3.read_chunk(), chunk1)
        self.assertEqual(f3.read_chunk(), chunk2)
        self.assertTrue(f3.read_chunk() is None)
        self.cache.close_file(f3)

        f4 = self.cache.read_file(f2.file_id())
        self.assertTrue(f4 is not None)
        self.assertEqual(f4.read_chunk(), chunk3)
        self.assertTrue(f4.read_chunk() is None)
        self.cache.close_file(f4)

        f5_id = File.generate_file_id()
        f5 = self.cache.open_file(file_id=f5_id, mode='w')
        try:
            self.cache.open_file(file_id=f5_id, mode='w')
            self.fail('Expected file already exists in cache error')
        except FileCacheError as e:
            self.assertEqual('File [{}] already exists in cache'.format(f5_id), str(e))
        self.cache.close_file(f5)

    def test_append_file(self):
        f1 = self.cache.open_file(mode='w')
        self.cache.close_file(f1, readable=False, writable=True, removable=False)

        self.assertEqual(f1.file_size(), 0)
        self.assertEqual(f1.size_on_disk(), 0)

        f2 = self.cache.open_file(file_id=f1.file_id(), mode='a')
        self.assertTrue(f2 is not None)
        chunk1 = random.randbytes(100)
        chunk2 = random.randbytes(50)
        f2.append_chunk(chunk1)
        f2.append_chunk(chunk2)
        self.cache.close_file(f2)

        f3 = self.cache.read_file(f1.file_id())
        self.assertTrue(f3 is not None)
        self.assertEqual(f3.read_chunk(), chunk1)
        self.assertEqual(f3.read_chunk(), chunk2)
        self.assertTrue(f3.read_chunk() is None)
        self.cache.close_file(f3)
    
    def test_remove_file(self):
        f1 = self.cache.open_file(mode='w')
        f2 = self.cache.open_file(mode='w')

        chunk1 = random.randbytes(100)
        chunk2 = random.randbytes(50)
        chunk3 = random.randbytes(50)
        f1.append_chunk(chunk1)
        f1.append_chunk(chunk2)
        f2.append_chunk(chunk3)

        try:
            self.cache.remove_file(f1)
            self.fail('Expected file is not removable error')
        except FileCacheError as e:
            self.assertEqual('File [{}] is not removable'.format(f1.file_id()), str(e))
        try:
            self.cache.remove_file(f2)
            self.fail('Expected file is not removable error')
        except FileCacheError as e:
            self.assertEqual('File [{}] is not removable'.format(f2.file_id()), str(e))

        self.cache.close_file(f1)
        self.cache.close_file(f2)
        self.assertEqual(self.cache.cache_used(), 200)
        self.cache.remove_file(f1)
        self.assertEqual(self.cache.cache_used(), 50)
        self.cache.remove_file(f2)
        self.assertEqual(self.cache.cache_used(), 0)

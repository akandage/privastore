import random
import shutil
from threading import Event, Thread
import unittest

from .error import FileCacheError
from .file import File
from .file_cache import FileCache

class TestFileCache(unittest.TestCase):

    def cleanup(self):
        try:
            shutil.rmtree(self.cache.cache_path())
        except:
            pass
    
    def setUp(self):
        self.cleanup()

    def tearDown(self):
        self.cleanup()
    
    def test_write_file(self):
        cache_config = {
            'store-path': 'test_file_cache',
        }
        self.cache = FileCache(cache_config)

        f1 = self.cache.write_file()
        f2 = self.cache.write_file()

        self.assertNotEqual(f1.file_id(), f2.file_id())
        chunk1 = random.randbytes(100)
        chunk2 = random.randbytes(50)
        chunk3 = random.randbytes(50)
        chunk4 = random.randbytes(25)
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
        self.assertEqual(f3.read_chunk(), b'')
        self.cache.close_file(f3)

        f4 = self.cache.read_file(f2.file_id())
        self.assertTrue(f4 is not None)
        self.assertEqual(f4.read_chunk(), chunk3)
        self.assertEqual(f4.read_chunk(), b'')
        self.cache.close_file(f4)

        f5_id = File.generate_file_id()
        f5 = self.cache.write_file(file_id=f5_id)
        try:
            self.cache.write_file(file_id=f5_id)
            self.fail('Expected file already exists in cache error')
        except FileCacheError as e:
            self.assertEqual('File [{}] already exists in cache'.format(f5_id), str(e))
        self.cache.close_file(f5)
       
    def test_append_file(self):
        cache_config = {
            'store-path': 'test_file_cache',
        }
        self.cache = FileCache(cache_config)

        f1 = self.cache.write_file()
        self.cache.close_file(f1, removable=False, writable=True)

        self.assertEqual(f1.file_size(), 0)
        self.assertEqual(f1.size_on_disk(), 0)

        f2 = self.cache.append_file(f1.file_id())
        self.assertTrue(f2 is not None)
        chunk1 = random.randbytes(100)
        chunk2 = random.randbytes(50)
        f2.append_chunk(chunk1)
        f2.append_chunk(chunk2)
        self.cache.close_file(f2)

        self.assertEqual(f2.file_size(), 150)
        self.assertEqual(f2.size_on_disk(), 150)

        f3 = self.cache.read_file(f1.file_id())
        self.assertTrue(f3 is not None)
        self.assertEqual(f3.read_chunk(), chunk1)
        self.assertEqual(f3.read_chunk(), chunk2)
        self.assertEqual(f3.read_chunk(), b'')
        self.cache.close_file(f3)
    
    def test_remove_file(self):
        cache_config = {
            'store-path': 'test_file_cache',
        }
        self.cache = FileCache(cache_config)

        f1 = self.cache.write_file()
        f2 = self.cache.write_file()

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
    
    def test_ensure_cache_space(self):
        cache_config = {
            'store-path': 'test_file_cache',
            'store-size': '4KB',
            'max-file-size': '4KB'
        }
        self.cache = FileCache(cache_config)

        chunk = random.randbytes(1024)
        f1 = self.cache.write_file(alloc_space=1024)
        f1.append_chunk(chunk)
        self.cache.close_file(f1)
        f2 = self.cache.write_file(alloc_space=1024)
        f2.append_chunk(chunk)
        self.cache.close_file(f2)
        f3 = self.cache.write_file(alloc_space=1024)
        f3.append_chunk(chunk)
        self.cache.close_file(f3)
        f4 = self.cache.write_file(alloc_space=1024)
        f4.append_chunk(chunk)
        self.cache.close_file(f4, removable=False)
        self.assertEqual(self.cache.cache_used(), 4096)

        self.assertTrue(self.cache.has_file(f1.file_id()))
        self.assertTrue(self.cache.has_file(f2.file_id()))
        self.assertTrue(self.cache.has_file(f3.file_id()))
        self.assertTrue(self.cache.has_file(f4.file_id()))

        try:
            self.cache.write_file(alloc_space=4096)
            self.fail('Expected insufficient space in cache error')
        except FileCacheError as e:
            self.assertEqual('Insufficient space in cache', str(e))

        self.assertTrue(self.cache.has_file(f1.file_id()))
        self.assertTrue(self.cache.has_file(f2.file_id()))
        self.assertTrue(self.cache.has_file(f3.file_id()))
        self.assertTrue(self.cache.has_file(f4.file_id()))

        f = self.cache.read_file(f3.file_id())
        self.cache.close_file(f)
        f = self.cache.read_file(f1.file_id())
        self.cache.close_file(f)

        # File access order (LRU -> MRU) F2 -> F4 -> F3 -> F1
        f = self.cache.write_file(alloc_space=2048)
        f.append_chunk(chunk)
        f.append_chunk(chunk)
        self.cache.close_file(f)
        self.assertEqual(self.cache.cache_used(), 4096)

        self.assertTrue(self.cache.has_file(f1.file_id()))
        self.assertFalse(self.cache.has_file(f2.file_id()))
        self.assertFalse(self.cache.has_file(f3.file_id()))
        self.assertTrue(self.cache.has_file(f4.file_id()))
    
    def test_concurrent_read_write(self):
        data_size = 4*1024 + 50
        data = random.randbytes(data_size)
        for _ in range(100):
            cache_config = {
                'store-path': 'test_file_cache',
                'chunk-size': '1KB'
            }
            self.cache = FileCache(cache_config)
            reader1_ok = Event()
            reader2_ok = Event()
            reader3_ok = Event()
            writer_ok = Event()
            file_id = File.generate_file_id()
            self.cache.create_empty_file(file_id, 5*1024)
            def reader1(ok: Event):
                file = self.cache.read_file(file_id)
                all_data = file.read()
                self.cache.close_file(file)
                if all_data == data:
                    ok.set()
            def reader2(ok: Event):
                file = self.cache.read_file(file_id)
                all_data = file.read()
                self.cache.close_file(file)
                if all_data == data:
                    ok.set()
            def reader3(ok: Event):
                file = self.cache.read_file(file_id)
                file.seek(data_size//2)
                half_data = file.read()
                self.cache.close_file(file)
                if half_data == data[data_size//2:]:
                    ok.set()
            def writer(ok):
                file = self.cache.append_file(file_id)
                file.write(data)
                file.flush()
                self.cache.close_file(file)
                ok.set()
                
            t1 = Thread(target=reader1, args=(reader1_ok,))
            t2 = Thread(target=reader2, args=(reader2_ok,))
            # t3 = Thread(target=reader3, args=(reader3_ok,))
            t4 = Thread(target=writer, args=(writer_ok,))
            t1.start()
            t2.start()
            # t3.start()
            t4.start()
            t1.join()
            t2.join()
            # t3.join()
            t4.join()
            self.assertTrue(reader1_ok.is_set())
            self.assertTrue(reader2_ok.is_set())
            # self.assertTrue(reader3_ok.is_set())
            self.assertTrue(writer_ok.is_set())
            self.cache.remove_file_by_id(file_id)
        

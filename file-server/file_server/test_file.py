import os
import random
import shutil
import unittest
from .error import FileError
from .file import File
from .file_chunk import get_encrypted_chunk_encoder, get_encrypted_chunk_decoder
from .util.crypto import get_encryptor_factory, get_decryptor_factory

class TestFile(unittest.TestCase):
    
    def cleanup(self):
        try:
            shutil.rmtree('test_file')
        except:
            pass

    def setUp(self):
        self.cleanup()
        os.mkdir('test_file')

    def tearDown(self):
        self.cleanup()
    
    def test_write_file(self):
        chunk1 = random.randbytes(1024)
        chunk2 = random.randbytes(1024)
        chunk3 = random.randbytes(100)

        f = File('test_file', mode='w')
        f.append_chunk(chunk1)
        f.close()

        self.assertEqual(len(chunk1), f.file_size())
        self.assertTrue(f.size_on_disk() >= len(chunk1))

        try:
            f.append_chunk(chunk2)
            self.fail('Expected file closed error')
        except FileError as e:
            self.assertEqual(str(e), 'File closed')
        
        try:
            File('test_file', file_id=f.file_id(), mode='w')
            self.fail('Expected error')
        except:
            pass

        f2 = File('test_file', mode='w')
        f2.append_chunk(chunk1)
        f2.append_chunk(chunk2)
        f2.append_chunk(chunk3)
        f2.close()

        self.assertEqual(len(chunk1)+len(chunk2)+len(chunk3), f2.file_size())
        self.assertTrue(f2.size_on_disk() >= len(chunk1)+len(chunk2)+len(chunk3))
        
    def test_read_write_file(self):
        chunk1 = random.randbytes(1024)
        chunk2 = random.randbytes(1024)
        chunk3 = random.randbytes(100)

        f = File('test_file', mode='w')
        f.append_chunk(chunk1)
        f.append_chunk(chunk2)
        f.append_chunk(chunk3)
        f.close()

        file_id = f.file_id()
        file_size = f.file_size()
        size_on_disk = f.size_on_disk()

        f2 = File('test_file', file_id=file_id, mode='r')
        self.assertEqual(f2.file_size(), file_size)
        self.assertEqual(f2.size_on_disk(), size_on_disk)
        self.assertEqual(chunk1, f2.read_chunk())
        self.assertEqual(chunk2, f2.read_chunk())
        self.assertEqual(chunk3, f2.read_chunk())
        self.assertTrue(f2.read_chunk() is None)
        f2.close()
    
    def test_append_file(self):
        chunk1 = random.randbytes(1024)
        chunk2 = random.randbytes(1024)

        f = File('test_file', mode='w')
        f.append_chunk(chunk1)
        f.close()

        self.assertEqual(f.total_chunks(), 1)
        self.assertEqual(f.file_size(), 1024)

        f2 = File('test_file', file_id=f.file_id(), mode='a')
        f2.append_chunk(chunk2)
        f2.close()

        f3 = File('test_file', file_id=f.file_id(), mode='r')
        self.assertEqual(f3.total_chunks(), 2)
        self.assertEqual(f3.file_size(), 2048)
        self.assertEqual(f3.read_chunk(), chunk1)
        self.assertEqual(f3.read_chunk(), chunk2)
        self.assertTrue(f3.read_chunk() is None)
        f3.close()

    def test_read_write_encrypted_file(self):
        key = os.urandom(16)
        enc_factory = get_encryptor_factory('aes-128-cbc', key)
        dec_factory = get_decryptor_factory('aes-128-cbc', key)
        chunk_enc = get_encrypted_chunk_encoder(enc_factory)
        chunk_dec = get_encrypted_chunk_decoder(dec_factory)

        chunk1 = random.randbytes(1024)
        chunk2 = random.randbytes(1024)
        chunk3 = random.randbytes(100)

        f = File('test_file', mode='w', encode_chunk=chunk_enc, decode_chunk=chunk_dec)
        f.append_chunk(chunk1)
        f.append_chunk(chunk2)
        f.append_chunk(chunk3)
        f.close()

        f2 = File('test_file', file_id=f.file_id(), mode='r', encode_chunk=chunk_enc, decode_chunk=chunk_dec)
        self.assertEqual(f2.read_chunk(), chunk1)
        self.assertEqual(f2.read_chunk(), chunk2)
        self.assertEqual(f2.read_chunk(), chunk3)
        self.assertTrue(f2.read_chunk() is None)
        f2.close()
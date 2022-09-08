import os
import unittest
from .file_chunk import get_encrypted_chunk_encoder, get_encrypted_chunk_decoder
from .util.crypto import get_encryptor_factory, get_decryptor_factory

class TestFileChunk(unittest.TestCase):
    
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_file_chunk(self):
        try:
            key = os.urandom(16)
            enc_factory = get_encryptor_factory('aes-128-cbc', key)
            dec_factory = get_decryptor_factory('aes-128-cbc', key)
            chunk_enc = get_encrypted_chunk_encoder(enc_factory)
            chunk_dec = get_encrypted_chunk_decoder(dec_factory)
            chunk_bytes = os.urandom(1024)
            with open('test_file_chunk.dat', 'wb') as f:
                chunk_enc(chunk_bytes, f)
                f.flush()
            with open('test_file_chunk.dat', 'rb') as f:
                x = chunk_dec(chunk_file=f)
                self.assertEqual(chunk_bytes, x, 'Expected decoded chunk to match original chunk bytes')
        finally:
            try:
                os.remove('test_file_chunk.dat')
            except:
                pass
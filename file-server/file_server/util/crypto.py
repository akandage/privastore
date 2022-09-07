from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers.algorithms import AES
from cryptography.hazmat.primitives.ciphers.modes import CBC
import hashlib
import os

'''
    Hash a password.
'''
def hash_user_password(password):
    if type(password) is not bytes:
        password = bytes(password, 'utf-8')

    return sha256(password)

def sha256(bytes):
    return hashlib.sha256(bytes).digest()

'''
    Cipher encryptor factory method.
'''
def get_encryptor_factory(key_algorithm, key_bytes):
    if key_algorithm == 'aes-128-cbc' or key_algorithm == 'aes-256-cbc':
        if key_algorithm == 'aes-128-cbc' and len(key_bytes) != 16:
            raise Exception('AES-128 key must be 16 bytes')
        if key_algorithm == 'aes-256-cbc' and len(key_bytes) != 32:
            raise Exception('AES-256 key must be 32 bytes')
        
        def factory():
            # IV (initialization vector) is 128-bit. Generate it securely randomly
            # and include in the encoded chunk.
            iv = os.urandom(16)
            cipher = Cipher(AES(key_bytes), CBC(iv))
            return cipher.encryptor(), iv

    return factory

'''
    Cipher decryptor factory method.
'''
def get_decryptor_factory(key_algorithm, key_bytes):
    if key_algorithm == 'aes-128-cbc' or key_algorithm == 'aes-256-cbc':
        if key_algorithm == 'aes-128-cbc' and len(key_bytes) != 16:
            raise Exception('AES-128 key must be 16 bytes')
        if key_algorithm == 'aes-256-cbc' and len(key_bytes) != 32:
            raise Exception('AES-256 key must be 32 bytes')
        
        def factory(iv):
            if len(iv) != 16:
                raise Exception('AES IV must be 16 bytes')
            cipher = Cipher(AES(key_bytes), CBC(iv))
            return cipher.decryptor()

    return factory

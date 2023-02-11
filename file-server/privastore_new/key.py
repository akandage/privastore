from .error import KeyError
import os
import uuid

class Key(object):

    def __init__(self, key_id: str, key_bytes: bytes, algorithm: str, is_system: bool):
        if type(key_bytes) != bytes or len(key_bytes) == 0:
            raise KeyError('Invalid key bytes!')
        if not self.is_valid_key_id(key_id):
            raise KeyError('Invalid key id!')

        self._key_id = key_id
        self._key_bytes = key_bytes
        self._algorithm = algorithm
        self._is_system = is_system

    def key_id(self) -> str:
        return self._key_id

    def key_bytes(self) -> bytes:
        return self._key_bytes

    def algorithm(self) -> str:
        return self._algorithm

    def is_system(self):
        return self._is_system

    @staticmethod
    def generate_key_id() -> str:
        return 'K-{}'.format(str(uuid.uuid4()))

    @staticmethod
    def is_valid_key_id(key_id: str) -> bool:
        if type(key_id) is not str:
            return False
        if key_id == 'system':
            return True
        if len(key_id) < 38 or not key_id.startswith('K-'):
            return False
        try:
            uuid.UUID(key_id[2:])
        except:
            return False
        return True

    @staticmethod
    def generate_key_bytes(algorithm: str) -> bytes:
        if algorithm == 'aes-128' or algorithm == 'aes-128-cbc':
            return os.urandom(16)
        elif algorithm == 'aes-256' or algorithm == 'aes-256-cbc':
            return os.urandom(32)
        else:
            raise KeyError('Unsupported algorithm [{}]'.format(algorithm), KeyError.INVALID_ALGORITHM)
    
    def __hash__(self):
        return hash(self.key_id())

    def __eq__(self, o):
        return isinstance(o, Key) and o.key_id() == self.key_id()
    
    def __str__(self):
        return 'Key id=[{}] algorithm=[{}]'.format(self.key_id(), self.algorithm())
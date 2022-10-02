from curses import meta
import os
import uuid
from .error import FileError

METADATA_FILE = '.metadata'

class File(object):

    def __init__(self, path, file_id=None, mode='r', encode_chunk=None, decode_chunk=None):
        self._file_id = file_id or self.generate_file_id()
        self._file_path = os.path.join(path, self._file_id)
        self._mode = mode
        self._chunks_read = 0
        self._chunks_written = 0
        self._total_chunks = 0
        self._file_size = 0
        self._size_on_disk = 0
        self._encode_chunk = encode_chunk
        self._decode_chunk = decode_chunk

        if mode == 'r':
            metadata_file_path = self.metadata_file_path()
            if not os.path.exists(metadata_file_path):
                raise FileError('Metadata file not found')
            with open(metadata_file_path, 'rb') as metadata_file:
                self._total_chunks = int.from_bytes(metadata_file.read(4), 'big', signed=False)
                self._file_size = int.from_bytes(metadata_file.read(4), 'big', signed=False)
                self._size_on_disk = int.from_bytes(metadata_file.read(4), 'big', signed=False)
        elif mode == 'w':
            os.mkdir(self._file_path)
        else:
            raise FileError('Invalid mode')
    
    def generate_file_id(self):
        return 'F-{}'.format(str(uuid.uuid4()))
    
    def metadata_file_path(self):
        return os.path.join(self._file_path, METADATA_FILE)
    
    def append_chunk(self, chunk_bytes):
        if self._mode != 'w':
            raise FileError('File not opened for writing')
        file_path = os.path.join(self._file_path, str(self._total_chunks+1))
        if os.path.exists(file_path):
            raise FileError('File chunk exists')
        with open(file_path, 'w') as chunk_file:
            self._size_on_disk += self._encode_chunk(chunk_bytes, chunk_file)
        self._chunks_written += 1
        self._file_size += len(chunk_bytes)
        self._total_chunks += 1
    
    def read_chunk(self):
        if self._mode != 'r':
            raise FileError('File not opened for reading')
        if self._chunks_read < self._total_chunks:
            file_path = os.path.join(self._file_path, str(self._chunks_read+1))
            if not os.path.exists(file_path):
                raise FileError('File chunk not found')
            with open(file_path, 'w') as chunk_file:
                chunk_bytes = self._decode_chunk(chunk_file)
            self._chunks_read += 1
            return chunk_bytes

    def close(self):
        with open(METADATA_FILE, 'wb') as metadata_file:
            metadata_file.write(self._total_chunks.to_bytes(4, 'big', signed=False))
            metadata_file.write(self._file_size.to_bytes(4, 'big', signed=False))
            metadata_file.write(self._size_on_disk.to_bytes(4, 'big', signed=False))
            metadata_file.flush()
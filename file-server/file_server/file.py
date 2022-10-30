import os
import shutil
import uuid
from .error import FileError
from .file_chunk import default_chunk_encoder, default_chunk_decoder
from .util.crypto import sha256

METADATA_FILE = '.metadata'

class File(object):

    def __init__(self, path, file_id=None, mode='r', encode_chunk=default_chunk_encoder, decode_chunk=default_chunk_decoder, skip_metadata=False):
        self._file_id = file_id or self.generate_file_id()
        self._file_path = os.path.join(path, self._file_id)
        self._mode = mode
        self._closed = False
        self._chunks_read = 0
        self._chunks_written = 0
        self._total_chunks = 0
        self._file_size = 0
        self._size_on_disk = 0
        self._encode_chunk = encode_chunk
        self._decode_chunk = decode_chunk

        if mode == 'r' or mode == 'a':
            if not skip_metadata:
                metadata_file_path = self.metadata_file_path()
                if not os.path.exists(metadata_file_path):
                    raise FileError('Metadata file not found')
                with open(metadata_file_path, 'rb') as metadata_file:
                    checksum = metadata_file.read(32)
                    if len(checksum) < 32:
                        raise FileError('Metadata file invalid checksum')
                    total_chunks = metadata_file.read(4)
                    if len(total_chunks) < 4:
                        raise FileError('Metadata file invalid total chunks')
                    file_size = metadata_file.read(4)
                    if len(file_size) < 4:
                        raise FileError('Metadata file invalid file size')
                    size_on_disk = metadata_file.read(4)
                    if len(size_on_disk) < 4:
                        raise FileError('Metadata file invalid file size on disk')
                    if sha256(total_chunks + file_size + size_on_disk) != checksum:
                        raise FileError('Metadata file checksum mismatch')
                    self._total_chunks = int.from_bytes(total_chunks, 'big', signed=False)
                    self._file_size = int.from_bytes(file_size, 'big', signed=False)
                    self._size_on_disk = int.from_bytes(size_on_disk, 'big', signed=False)
        elif mode == 'w':
            os.mkdir(self._file_path)
        else:
            raise FileError('Invalid mode')
    
    @staticmethod
    def generate_file_id():
        return 'F-{}'.format(str(uuid.uuid4()))

    @staticmethod
    def is_valid_file_id(file_id):
        if len(file_id) < 38 or not file_id.startswith('F-'):
            return False
        try:
            uuid.UUID(file_id[2:])
        except:
            return False
        return True

    def metadata_file_path(self):
        return os.path.join(self._file_path, METADATA_FILE)
    
    def file_id(self):
        return self._file_id
    
    def mode(self):
        return self._mode

    def chunks_read(self):
        return self._chunks_read
    
    def chunks_written(self):
        return self._chunks_written

    def total_chunks(self):
        return self._total_chunks

    def file_size(self):
        return self._file_size
    
    def size_on_disk(self):
        return self._size_on_disk

    def closed(self):
        return self._closed

    def set_closed(self):
        self._closed = True

    def write(self, data):
        '''
            Implement this so it behaves like file-like object.
        '''
        self.append_chunk(data)
        return len(data)

    def append_chunk(self, chunk_bytes):
        if self.closed():
            raise FileError('File closed')
        if self._mode != 'w' and self._mode != 'a':
            raise FileError('File not opened for writing')
        file_path = os.path.join(self._file_path, str(self._total_chunks+1))
        if os.path.exists(file_path):
            raise FileError('File chunk exists')
        with open(file_path, 'wb') as chunk_file:
            self._size_on_disk += self._encode_chunk(chunk_bytes, chunk_file)
        self._chunks_written += 1
        self._file_size += len(chunk_bytes)
        self._total_chunks += 1
    
    def read(self, size):
        '''
            Implement this so it behaves like file-like object.
        '''
        chunk = self.read_chunk()
        if chunk is None:
            return b''
        return chunk

    def read_chunk(self):
        if self.closed():
            raise FileError('File closed')
        if self._mode != 'r':
            raise FileError('File not opened for reading')
        if self._chunks_read < self._total_chunks:
            file_path = os.path.join(self._file_path, str(self._chunks_read+1))
            if not os.path.exists(file_path):
                raise FileError('File chunk not found')
            with open(file_path, 'rb') as chunk_file:
                chunk_bytes = self._decode_chunk(chunk_file=chunk_file)
            self._chunks_read += 1
            return chunk_bytes

    def remove(self):
        shutil.rmtree(self._file_path)
    
    def flush(self):
        '''
            Implement this so it behaves like file-like object (no-op).
        '''
        return

    def close(self):
        if not self.closed() and (self._mode == 'w' or self._mode == 'a'):
            metadata_file_path = self.metadata_file_path()
            total_chunks = self._total_chunks.to_bytes(4, 'big', signed=False)
            file_size = self._file_size.to_bytes(4, 'big', signed=False)
            size_on_disk = self._size_on_disk.to_bytes(4, 'big', signed=False)
            checksum = sha256(total_chunks + file_size + size_on_disk)
            with open(metadata_file_path, 'wb') as metadata_file:
                metadata_file.write(checksum)
                metadata_file.write(total_chunks)
                metadata_file.write(file_size)
                metadata_file.write(size_on_disk)
                metadata_file.flush()
        self.set_closed()
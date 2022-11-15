import logging
import os
import shutil
import uuid
from .error import FileError, FileServerErrorCode
from .file_chunk import default_chunk_encoder, default_chunk_decoder
from .util.crypto import sha256
from .util.file import KILOBYTE
from typing import Optional

METADATA_FILE = '.metadata'
FILE_ID_LENGTH = 38

class File(object):

    def __init__(self, path, file_id=None, mode='r', chunk_size=KILOBYTE, encode_chunk=default_chunk_encoder, decode_chunk=default_chunk_decoder):
        self._file_id = file_id or self.generate_file_id()
        self._file_path = os.path.join(path, self._file_id)
        self._mode = mode
        self._modified = False
        self._closed = False
        self._chunk_size = chunk_size
        self._chunks_read = 0
        self._chunks_written = 0
        self._total_chunks = 0
        self._file_size = 0
        self._size_on_disk = 0
        self._read_buffer = bytes()
        self._read_offset = 0
        self._write_buffer = bytes()
        self._encode_chunk = encode_chunk
        self._decode_chunk = decode_chunk

        if mode == 'r' or mode == 'a':
            self.read_metadata_file()
            if mode == 'r':
                logging.debug('File [{}] opened for reading'.format(file_id))
            else:
                logging.debug('File [{}] opened for appending'.format(file_id))
        elif mode == 'w':
            if os.path.exists(self._file_path):
                raise FileError('File [{}] exists'.format(self.file_id()), FileServerErrorCode.FILE_EXISTS)
            os.mkdir(self._file_path)
            self.write_metadata_file()
            logging.debug('File [{}] opened for writing'.format(file_id))
        else:
            raise FileError('Invalid mode')
    
    @staticmethod
    def generate_file_id() -> str:
        return 'F-{}'.format(str(uuid.uuid4()))

    @staticmethod
    def is_valid_file_id(file_id: str) -> bool:
        if len(file_id) < 38 or not file_id.startswith('F-'):
            return False
        try:
            uuid.UUID(file_id[2:])
        except:
            return False
        return True

    @staticmethod
    def create_empty(file_path: str, file_id: str):
        File(file_path, file_id, mode='w').close()

    def read_metadata_file(self):
        if not os.path.exists(self._file_path):
            raise FileError('File [{}] not found'.format(self.file_id()), FileServerErrorCode.FILE_NOT_FOUND)
        metadata_file_path = self.metadata_file_path()
        if not os.path.exists(metadata_file_path):
            raise FileError('Metadata file not found', FileServerErrorCode.FILE_IS_CORRUPT)
        with open(metadata_file_path, 'rb') as metadata_file:
            checksum = metadata_file.read(32)
            if len(checksum) < 32:
                raise FileError('Metadata file invalid checksum', FileServerErrorCode.FILE_IS_CORRUPT)
            total_chunks = metadata_file.read(4)
            if len(total_chunks) < 4:
                raise FileError('Metadata file invalid total chunks', FileServerErrorCode.FILE_IS_CORRUPT)
            file_size = metadata_file.read(4)
            if len(file_size) < 4:
                raise FileError('Metadata file invalid file size', FileServerErrorCode.FILE_IS_CORRUPT)
            size_on_disk = metadata_file.read(4)
            if len(size_on_disk) < 4:
                raise FileError('Metadata file invalid file size on disk', FileServerErrorCode.FILE_IS_CORRUPT)
            if sha256(total_chunks, file_size, size_on_disk) != checksum:
                raise FileError('Metadata file checksum mismatch', FileServerErrorCode.FILE_IS_CORRUPT)
            self._total_chunks = int.from_bytes(total_chunks, 'big', signed=False)
            self._file_size = int.from_bytes(file_size, 'big', signed=False)
            self._size_on_disk = int.from_bytes(size_on_disk, 'big', signed=False)

    def write_metadata_file(self):
        metadata_file_path = self.metadata_file_path()
        total_chunks = self._total_chunks.to_bytes(4, 'big', signed=False)
        file_size = self._file_size.to_bytes(4, 'big', signed=False)
        size_on_disk = self._size_on_disk.to_bytes(4, 'big', signed=False)
        checksum = sha256(total_chunks, file_size, size_on_disk)
        with open(metadata_file_path, 'wb') as metadata_file:
            metadata_file.write(checksum)
            metadata_file.write(total_chunks)
            metadata_file.write(file_size)
            metadata_file.write(size_on_disk)
            metadata_file.flush()

    def metadata_file_path(self) -> str:
        return os.path.join(self._file_path, METADATA_FILE)
    
    def file_id(self) -> str:
        return self._file_id
    
    def mode(self) -> str:
        return self._mode

    def chunk_size(self) -> int:
        return self._chunk_size

    def chunks_read(self) -> int:
        return self._chunks_read
    
    def chunks_written(self) -> int:
        return self._chunks_written

    def total_chunks(self) -> int:
        return self._total_chunks

    def file_size(self) -> int:
        return self._file_size
    
    def size_on_disk(self) -> int:
        return self._size_on_disk

    def modified(self) -> bool:
        return self._modified

    def closed(self) -> bool:
        return self._closed

    def set_closed(self) -> None:
        self._closed = True

    def seek(self, offset: int) -> None:
        if offset < 0 or offset > self.file_size():
            raise FileError('Cannot seek to offset [{}]'.format(offset), FileServerErrorCode.INVALID_SEEK_OFFSET)
        chunk_offset = 0
        chunk_start = 0
        while True:
            self.seek_chunk(chunk_offset)
            chunk = self.read_chunk()
            chunk_len = len(chunk)
            next_chunk = chunk_start + chunk_len
            if chunk_len > 0:
                if offset >= chunk_start and offset < next_chunk:
                    self._read_buffer = chunk
                    self._read_offset = offset - chunk_start
                    return
            else:
                raise FileError('Cannot seek to offset [{}]'.format(offset))
            chunk_offset += 1
            chunk_start = next_chunk
    
    def seek_chunk(self, offset: int) -> None:
        if offset < 0 or offset > self.total_chunks():
            raise FileError('Cannot seek to chunk [{}]'.format(offset), FileServerErrorCode.INVALID_CHUNK_NUM)
        self._chunks_read = offset
        self._read_buffer = bytes()
        self._read_offset = 0

    def tell(self) -> int:
        # TODO
        raise FileError('Not implemented!')

    def write(self, data: bytes) -> int:
        '''
            Implement this so it behaves like file-like object.
        '''
        data_len = len(data)
        if data_len == 0:
            return 0
        self._write_buffer += data
        self._modified = True
        wbuf_len = len(self._write_buffer)
        chunk_size = self.chunk_size()
        if wbuf_len >= chunk_size:
            num_chunks = wbuf_len // chunk_size
            for offset in range(0, num_chunks*chunk_size, chunk_size):
                end = offset + chunk_size
                self.append_chunk(self._write_buffer[offset:end])
            if end < wbuf_len:
                self._write_buffer = self._write_buffer[end:]
            else:
                self._write_buffer = bytes()
        return data_len

    def append_chunk(self, chunk_bytes: bytes) -> None:
        if self.closed():
            raise FileError('File closed')
        if self._mode != 'w' and self._mode != 'a':
            raise FileError('File not opened for writing')
        if len(chunk_bytes) == 0:
            raise FileError('Cannot append empty chunk')
        file_path = os.path.join(self._file_path, str(self._total_chunks+1))
        if os.path.exists(file_path):
            raise FileError('File chunk exists', FileServerErrorCode.FILE_IS_CORRUPT)
        with open(file_path, 'wb') as chunk_file:
            self._size_on_disk += self._encode_chunk(chunk_bytes, chunk_file)
        self._chunks_written += 1
        self._file_size += len(chunk_bytes)
        self._total_chunks += 1
        self._modified = True
    
    def read(self, size: Optional[int] = None) -> bytes:
        '''
            Implement this so it behaves like file-like object.
        '''
        if size is None:
            size = self.file_size()
        if size < 0:
            raise FileError('Invalid read size!')
        if size == 0:
            return b''
        
        buf = bytes()
        buf_len = 0

        while buf_len < size:
            read_buf_len = len(self._read_buffer)

            if read_buf_len - self._read_offset == 0:
                self._read_buffer = self.read_chunk()
                self._read_offset = 0
                read_buf_len = len(self._read_buffer)
            
            if read_buf_len == 0:
                break
            
            read_size = min(read_buf_len - self._read_offset, size - buf_len)
            read_end = self._read_offset + read_size
            buf += self._read_buffer[self._read_offset:read_end]
            buf_len += read_size
            self._read_offset = read_end

        return buf

    def read_chunk(self) -> bytes:
        if self.closed():
            raise FileError('File closed')
        if self._mode != 'r':
            raise FileError('File not opened for reading')
        if self._chunks_read < self._total_chunks:
            file_path = os.path.join(self._file_path, str(self._chunks_read+1))
            if not os.path.exists(file_path):
                raise FileError('File chunk not found', FileServerErrorCode.FILE_IS_CORRUPT)
            with open(file_path, 'rb') as chunk_file:
                chunk_bytes = self._decode_chunk(chunk_file=chunk_file)
            self._chunks_read += 1
            return chunk_bytes
        return b''

    def remove(self) -> None:
        shutil.rmtree(self._file_path)
    
    def flush(self) -> None:
        '''
            Implement this so it behaves like file-like object (no-op).
        '''
        if len(self._write_buffer) > 0:
            self.append_chunk(self._write_buffer)
            self._write_buffer = bytes()

    def close(self) -> None:
        if not self.closed() and self.modified() and (self._mode == 'w' or self._mode == 'a'):
            self.flush()
            self.write_metadata_file()
        self.set_closed()
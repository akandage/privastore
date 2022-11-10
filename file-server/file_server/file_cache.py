from collections import namedtuple
from .error import FileCacheError, FileError, FileServerErrorCode
from .file import File
from .file_chunk import chunk_encoder, chunk_decoder, default_chunk_encoder, default_chunk_decoder
from .util.file import config_bool, parse_mem_size, str_mem_size
import logging
import os
import shutil
from threading import Condition, Lock, RLock
import time
from typing import Optional

class IndexNode(object):

    def __init__(self, file_id, file_size, file_chunks):
        self._file_id: str = file_id
        # Size of the file on disk. Used for cache space management.
        self.file_size: int = file_size
        # Number of file chunks *currently* available to read.
        self.file_chunks: int = file_chunks
        self.num_readers: int = 0
        self.num_writers: int = 0
        self.error: bool = False
        self.removable: bool = False
        self.writable: bool = False
        self._prev: IndexNode = None
        self._next: IndexNode = None
        self.lock = Lock()
        # Used to signal readers that the file has changed.
        self.reader_cv = Condition(self.lock)
    
    def file_id(self) -> str:
        return self._file_id

class FileCache(object):

    class CacheFileReader(File):

        def __init__(self, cache_path: str, file_id: str, node: IndexNode, encode_chunk: chunk_encoder=default_chunk_encoder, decode_chunk: chunk_encoder=default_chunk_decoder, read_timeout: int=90):
            super().__init__(cache_path, file_id, mode='r', encode_chunk=encode_chunk, decode_chunk=decode_chunk, skip_metadata=True)
            self._node = node
            self._read_timeout = read_timeout

            with self._node.lock:
                self._node.num_readers += 1
                self._node.removable = False
            
            logging.debug('File [{}] opened for reading'.format(file_id))

        def file_size(self):
            #
            # This object is essentially a stream. Cannot be relied upon as we
            # are not updating this field in the reader object.
            #
            raise Exception('Not implemented!')
        
        def size_on_disk(self):
            #
            # This object is essentially a stream. Cannot be relied upon as we
            # are not updating this field in the reader object.
            #
            raise Exception('Not implemented!')

        def seek_chunk(self, offset):
            with self._node.lock:
                self._total_chunks = self._node.file_chunks
            return super().seek_chunk(offset)

        def read_chunk(self):
            start_t = time.time()
            end_t = start_t + self._read_timeout

            while True:
                now = time.time()
                if now >= end_t:
                    raise FileCacheError('Timed out reading file [{}]!'.format(self.file_id()), FileServerErrorCode.IO_TIMEOUT)
                with self._node.lock:
                    if self._node.error:
                        raise FileCacheError('File [{}] could not be read!'.format(self.file_id()), FileServerErrorCode.IO_ERROR)

                    self._total_chunks = self._node.file_chunks

                    if not self._node.writable:
                        # File cannot be appended to.
                        break
                    if self._chunks_read < self._total_chunks:
                        # We have chunks to read.
                        break

                    timeout = end_t - now
                    self._node.reader_cv.wait(timeout=timeout)

            return super().read_chunk()
        
        def close(self):
            if not self.closed():
                with self._node.lock:
                    if self._node.num_readers <= 0:
                        raise FileCacheError('Invalid file [{}] state!'.format(self.file_id()), FileServerErrorCode.INTERNAL_ERROR)
                    self._node.num_readers -= 1
                
                super().close()

    class CacheFileWriter(File):

        def __init__(self, cache_path: str, file_id: str, node: IndexNode, mode: str='w', encode_chunk: chunk_encoder=default_chunk_encoder, decode_chunk: chunk_decoder=default_chunk_decoder,):
            super().__init__(cache_path, file_id, mode=mode, encode_chunk=encode_chunk, decode_chunk=decode_chunk, skip_metadata=True)
            self._node = node

            if mode != 'w' and mode != 'a':
                raise FileError('Invalid mode!', FileServerErrorCode.INTERNAL_ERROR)

            with self._node.lock:
                if self._node.num_writers != 0:
                    raise FileCacheError('File [{}] already has writer!'.format(self.file_id()), FileServerErrorCode.FILE_NOT_WRITABLE)
                if not self._node.writable:
                    raise FileCacheError('File [{}] is not writable!'.format(self.file_id()), FileServerErrorCode.FILE_NOT_WRITABLE)
                self._node.num_writers = 1
                self._node.removable = False
            
            #
            # Read the metadata now that we have the lock on the file.
            #
            if mode == 'a':
                self.read_metadata_file()
                logging.debug('File [{}] opened for appending'.format(file_id))
            else:
                logging.debug('File [{}] opened for writing'.format(file_id))

        def write(self, data):
            lock = self._node.lock
            try:
                prev_chunks = self.total_chunks()
                written = super().write(data)
                with lock:
                    self._node.file_chunks += self.total_chunks() - prev_chunks
                    self._node.reader_cv.notify_all()
                return written
            except Exception as e:
                with lock:
                    self._node.error = True
                    self._node.reader_cv.notify_all()
                raise e

        def append_chunk(self, chunk_bytes):
            lock = self._node.lock
            try:
                super().append_chunk(chunk_bytes)
                with lock:
                    self._node.file_chunks += 1
                    self._node.reader_cv.notify_all()
            except Exception as e:
                with lock:
                    self._node.error = True
                    self._node.reader_cv.notify_all()
                raise e

        def close(self):
            lock = self._node.lock
            try:
                super().close()
                with lock:
                    if self._node.num_writers != 1:
                        raise FileCacheError('Invalid file [{}] state!'.format(self.file_id()), FileServerErrorCode.INTERNAL_ERROR)
                    self._node.num_writers = 0
                    self._node.reader_cv.notify_all()
            except Exception as e:
                with lock:
                    self._node.error = True
                    self._node.reader_cv.notify_all()
                raise e

    IndexNodeMetadata = namedtuple('IndexNodeMetadata', ['file_size', 'file_chunks'])

    class Index(object):

        def __init__(self):
            self._head = None
            self._tail = None
            self._files = dict()
        
        def head(self) -> IndexNode:
            return self._head
        
        def tail(self) -> IndexNode:
            return self._tail

        def __len__(self):
            return len(self._files)

        def has_node(self, file_id: str) -> bool:
            if file_id is None or not File.is_valid_file_id(file_id):
                raise FileError('Invalid file id!', FileServerErrorCode.INVALID_FILE_ID)

            return file_id in self._files

        def get_node(self, file_id: str) -> IndexNode:
            if file_id is None or not File.is_valid_file_id(file_id):
                raise FileError('Invalid file id!', FileServerErrorCode.INVALID_FILE_ID)

            return self._files.get(file_id)

        def add_node(self, node: IndexNode) -> None:
            file_id = node.file_id()
            if file_id is None or not File.is_valid_file_id(file_id):
                raise FileCacheError('Node missing or invalid file id!', FileServerErrorCode.INTERNAL_ERROR)
            if node._prev is not None or node._next is not None:
                raise FileCacheError('Node prev or next pointers already set!', FileServerErrorCode.INTERNAL_ERROR)
            if self.has_node(file_id):
                raise FileCacheError('Node already in index!', FileServerErrorCode.INTERNAL_ERROR)
            
            self._files[file_id] = node
            if self._tail is not None:
                node._prev = self._tail
                self._tail._next = node
                self._tail = node
            else:
                self._head = self._tail = node

        def pop_node(self, file_id: str) -> IndexNode:
            if not self.has_node(file_id):
                raise FileCacheError('Node is not in index!', FileServerErrorCode.INTERNAL_ERROR)
            node = self.get_node(file_id)
            node_prev = node._prev
            node_next = node._next

            if node_prev is not None:
                node_prev._next = node._next
            if node_next is not None:
                node_next._prev = node._prev
            node._prev = None
            node._next = None
            if node == self._head:
                self._head = node_next
            if node == self._tail:
                self._tail = node_prev

            self._files.pop(file_id)
            return node
        
        def move_to_back(self, file_id: str):
            node = self.pop_node(file_id)
            self.add_node(node)


    def __init__(self, cache_config):
        self._cache_path: str = cache_config.get('store-path', './cache')
        self._cache_used = 0
        self._cache_size = parse_mem_size(cache_config.get('store-size', '1GB'))
        self._chunk_size = parse_mem_size(cache_config.get('chunk-size', '1MB'))
        self._max_file_size = parse_mem_size(cache_config.get('max-file-size', '500MB'))
        self._file_eviction = config_bool(cache_config.get('enable-file-eviction', '1'))

        self._index = FileCache.Index()
        self._index_lock = RLock()

        if not os.path.exists(self._cache_path):
            os.mkdir(self._cache_path)
            logging.info('File cache created in path [{}]'.format(self._cache_path))
        else:
            for file_id in os.listdir(self._cache_path):
                try:
                    f = File(self._cache_path, file_id, mode='r')
                    node = self.create_cache_entry(file_id, f.size_on_disk(), f.total_chunks())
                    node.removable = True
                    f.close()
                except Exception as e:
                    logging.error('Error initializing cache file [{}]: {}'.format(file_id, str(e)))
        
        logging.debug('File store used [{}]'.format(str_mem_size(self._cache_used)))
        logging.debug('File store size [{}]'.format(str_mem_size(self._cache_size)))
        logging.debug('File chunk size [{}]'.format(str_mem_size(self._chunk_size)))
        logging.debug('Max file size [{}]'.format(str_mem_size(self._max_file_size)))
        logging.debug('File eviction enabled: [{}]'.format(self._file_eviction))

    def cache_path(self) -> str:
        return self._cache_path

    def cache_size(self) -> int:
        return self._cache_size
    
    def cache_used(self) -> int:
        cache_used = 0
        with self._index_lock:
            cache_used = self._cache_used
        return cache_used
    
    def cache_free_space(self) -> int:
        free_space = 0
        with self._index_lock:
            free_space = max(0, self._cache_size - self._cache_used)
        return free_space
    
    def file_chunk_size(self) -> int:
        return self._chunk_size
    
    def max_file_size(self) -> int:
        return self._max_file_size

    def has_file(self, file_id: str) -> bool:
        with self._index_lock:
            return self._index.has_node(file_id)

    def create_cache_entry(self, file_id: str, file_size: int, file_chunks: int=0) -> IndexNode:
        with self._index_lock:
            if self._index.has_node(file_id):
                raise FileCacheError('File [{}] already exists in cache'.format(file_id), FileServerErrorCode.FILE_EXISTS)
            self.ensure_cache_space(file_size)
            node = IndexNode(file_id, file_size, file_chunks)
            self._index.add_node(node)
            self._cache_used += file_size
            return node

    '''
        Open file in cache for reading.
        While file is opened, it will be locked preventing its removal from the cache.
        Allow multiple readers and single writer.

        If file is not found, return None.
        Otherwise, return a file like object for reading.
    '''
    def read_file(self, file_id: str, encode_chunk: chunk_encoder=default_chunk_encoder, decode_chunk: chunk_decoder=default_chunk_decoder) -> Optional[File]:
        with self._index_lock:
            if self._index.has_node(file_id):
                node = self._index.get_node(file_id)

                #
                # This is now the MRU (most recently used) file.
                #
                self._index.move_to_back(file_id)

                return FileCache.CacheFileReader(self._cache_path, file_id, node, encode_chunk=encode_chunk, decode_chunk=decode_chunk)

    '''
        Open file in cache for writing.
        While file is opened, it will be locked preventing its removal from the cache.
        Allow multiple readers and single writer.

        Return a file like object for writing.
    '''
    def write_file(self, file_id: Optional[str]=None, file_size: int=0, encode_chunk: chunk_encoder=default_chunk_encoder, decode_chunk: chunk_decoder=default_chunk_decoder) -> File:
        with self._index_lock:
            if file_id is None:
                file_id = File.generate_file_id()

            node = self.create_cache_entry(file_id, file_size)
            node.writable = True
            node.removable = False

            return FileCache.CacheFileWriter(self._cache_path, file_id, node, encode_chunk=encode_chunk, decode_chunk=decode_chunk)

    '''
        Open file in cache for appending.
        File must already exist in the cache.
        While file is opened, it will be locked preventing its removal from the cache.
        Allow multiple readers and single writer.

        Return a file like object for appending.
    '''
    def append_file(self, file_id: str, encode_chunk: chunk_encoder=default_chunk_encoder, decode_chunk: chunk_decoder=default_chunk_decoder) -> File:
        with self._index_lock:
            if not self._index.has_node(file_id):
                raise FileCacheError('File [{}] not found in cache'.format(file_id), FileServerErrorCode.FILE_NOT_FOUND)

            node = self._index.get_node(file_id)

            return FileCache.CacheFileWriter(self._cache_path, file_id, node, mode='a', encode_chunk=encode_chunk, decode_chunk=decode_chunk)

    '''
        Create empty file in the cache with file_size bytes reserved.
        File will be writable and appendable until closed.

        Throws error if file with given id already exists in cache or if there
        is insufficient space in the cache.

    '''
    def touch_file(self, file_id: str, file_size: int) -> None:
        with self._index_lock:
            node = self.create_cache_entry(file_id, file_size)
            node.writable = True
            node.removable = False
            File.touch_file(self._cache_path, file_id)

    '''
        Retrieve file metadata:
            - file_size - the allocated space for the file in the cache
            - file_chunks - number of file chunks
        
        Throws error if file does not exist in cache.
    '''
    def file_metadata(self, file_id: str) -> tuple:
        with self._index_lock:
            if not self._index.has_node(file_id):
                raise FileCacheError('File [{}] not found in cache'.format(file_id), FileServerErrorCode.FILE_NOT_FOUND)
            
            node = self._index.get_node(file_id)
            with node.lock:
                return FileCache.IndexNodeMetadata(node.file_size, node.file_chunks)

    '''
        Close file in cache.

    '''
    def close_file(self, file: File, removable: bool=True, writable: bool=False) -> None:
        if file.closed():
            raise FileCacheError('Already closed!', FileServerErrorCode.INTERNAL_ERROR)

        with self._index_lock:
            file.close()
            file_id = file.file_id()
            mode = file.mode()

            if mode == 'r':
                logging.debug('File [{}] reader closed'.format(file_id))
            elif mode == 'w':
                logging.debug('File [{}] writer closed'.format(file_id))
            elif mode == 'a':
                logging.debug('File [{}] appender closed'.format(file_id))

            if self._index.has_node(file_id):
                node = self._index.get_node(file_id)
                with node.lock:
                    if mode == 'w' or mode == 'a':
                        if not writable:
                            node.writable = False
                            node.reader_cv.notify_all()
                            logging.debug('File [{}] no longer writable'.format(file_id))

                        prev_size = node.file_size
                        curr_size = file.size_on_disk()
                        if curr_size > prev_size:
                            node.file_size = curr_size
                            self._cache_used += curr_size-prev_size

                        if self._cache_used > self._cache_size:
                            node.error = True
                            node.reader_cv.notify_all()
                            raise FileCacheError('File cache is full!', FileServerErrorCode.FILE_STORE_FULL)
                    if node.num_readers == 0 and node.num_writers == 0 and not node.writable:
                        node.removable = removable
                        if removable:
                            logging.debug('File [{}] now removable'.format(file_id))
            else:
                raise FileCacheError('File [{}] not found in cache'.format(file_id))

    '''
        Remove file from cache.

    '''
    def remove_file(self, file: File) -> None:
        file_id = file.file_id()
        self.remove_file_by_id(file_id)

    def remove_file_by_id(self, file_id: str, ignore_removable: bool = False) -> None:
        with self._index_lock:
            if self._index.has_node(file_id):
                node = self._index.get_node(file_id)
                with node.lock:
                    if not node.removable:
                        if not ignore_removable:
                            raise FileCacheError('File [{}] is not removable'.format(file_id), FileServerErrorCode.FILE_NOT_REMOVABLE)
                    if node.num_readers > 0:
                        raise FileCacheError('File has [{}] readers but is removable'.format(node.num_readers), FileServerErrorCode.INTERNAL_ERROR)
                    if node.num_writers > 0:
                        raise FileCacheError('File has [{}] writers but is removable'.format(node.num_writers), FileServerErrorCode.INTERNAL_ERROR)
                self._index.pop_node(file_id)
                try:
                    shutil.rmtree(os.path.join(self._cache_path, file_id))
                except Exception as e:
                    logging.warn('Could not remove file [{}] from cache: {}'.format(file_id, str(e)))
                logging.debug('Removed file [{}] from cache'.format(file_id))
                self._cache_used -= node.file_size
                logging.debug('Reclaimed [{}] space in cache'.format(str_mem_size(node.file_size)))
            else:
                raise FileCacheError('File [{}] not found in cache'.format(file_id), FileServerErrorCode.FILE_NOT_FOUND)

    '''
        Remove the LRU (least recently used file) from the cache to free up space.

    '''
    def remove_lru_file(self):
        # TODO
        pass

    '''
        Ensure at least the given amount of space is available in the cache.
        If there is not enough space in the cache then remove files starting
        from the LRU (least recently used) file until there is.
        Will only remove files with the removable flag set.

        size - amount of cache space required in bytes.

        Throws FileCacheError if given amount of space is not available.
    '''
    def ensure_cache_space(self, size: int) -> None:
        if size == 0:
            return
        if size < 0:
            raise FileCacheError('Invalid size!', FileServerErrorCode.INTERNAL_ERROR)
        if size > self._max_file_size:
            raise FileCacheError('File size too large!', FileServerErrorCode.FILE_TOO_LARGE)
        
        with self._index_lock:
            if self.cache_free_space() >= size:
                return
            elif not self._file_eviction:
                raise FileCacheError('Insufficient space in cache', FileServerErrorCode.INSUFFICIENT_SPACE)
            
            #
            # First check if we can make enough space available before
            # removing any files.
            #

            total_size = 0
            # Head node is LRU file.
            curr_node = self._index.head()
            while total_size < size and curr_node is not None:
                if curr_node.file_size > 0 and curr_node.removable:
                    total_size += curr_node.file_size
                curr_node = curr_node._next

            if total_size < size:
                raise FileCacheError('Insufficient space in cache', FileServerErrorCode.INSUFFICIENT_SPACE)

            total_size = 0
            curr_node = self._index.head()
            while total_size < size and curr_node is not None:
                next_node = curr_node._next

                if curr_node.removable:
                    file_id = curr_node.file_id()
                    self.remove_file_by_id(file_id)
                    total_size += curr_node.file_size
                    logging.debug('Evicted file [{}] from cache, recovered [{}] space'.format(file_id, str_mem_size(curr_node.file_size)))

                curr_node = next_node
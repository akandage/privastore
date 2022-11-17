from collections import namedtuple
import configparser
from .error import FileCacheError, FileError, FileServerErrorCode
from .file import File
from .file_chunk import chunk_encoder, chunk_decoder, default_chunk_encoder, default_chunk_decoder
from .util.file import config_bool, parse_mem_size, str_mem_size, KILOBYTE
import logging
import os
import shutil
from threading import Condition, RLock
import time
from typing import Optional, Union

class FileCache(object):

    class IndexNode(object):

        def __init__(self, index: 'FileCache.Index', file_id: str, alloc_space: int, writable: bool = True, removable: bool = False):
            self._index = index
            self._file_id = file_id
            self._alloc_space = alloc_space
            self._used_space = 0
            self._available_chunks = 0
            self._available_bytes = 0
            self._num_readers: int = 0
            self._num_writers: int = 0
            self._error: bool = False
            self._removable: bool = removable
            self._removed: bool = False
            self._writable: bool = writable
            self._prev: 'FileCache.IndexNode' = None
            self._next: 'FileCache.IndexNode' = None
            self.lock = RLock()
            self._readers = Condition(self.lock)
        
        def index(self) -> 'FileCache.Index':
            return self._index

        def file_id(self) -> str:
            return self._file_id
        
        def alloc_space(self) -> int:
            '''
                Allocated space for this file in the cache.
            '''
            with self.lock:
                return self._alloc_space

        def set_alloc_space(self, alloc_space: int):
            with self.lock:
                self._alloc_space = alloc_space

        def used_space(self) -> int:
            '''
                Amount of space used by this file.
            '''
            with self.lock:
                return self._used_space

        def set_used_space(self, used_space: int):
            with self.lock:
                if used_space == self._used_space:
                    return
                elif used_space < self._used_space:
                    raise FileCacheError('Cannot reduce file [{}] used space!'.format(self.file_id()))

                self._used_space = used_space

        def available_chunks(self):
            '''
                Number of chunks which can currently be read from the file.
            '''
            with self.lock:
                return self._available_chunks

        def set_available_chunks(self, avail_chunks: int):
            with self.lock:
                if avail_chunks == self._available_chunks:
                    return
                elif avail_chunks < self._available_chunks:
                    raise FileCacheError('Cannot reduce file [{}] available chunks!'.format(self.file_id()))

                self._available_chunks = avail_chunks
                self._readers.notify_all()

        def available_bytes(self):
            '''
                Number of bytes which can currently be read from the file.
            '''
            with self.lock:
                return self._available_bytes

        def set_available_bytes(self, avail_bytes: int):
            with self.lock:
                if avail_bytes == self._available_bytes:
                    return
                elif avail_bytes < self._available_bytes:
                    raise FileCacheError('Cannot reduce file [{}] available bytes!'.format(self.file_id()))

                self._available_bytes = avail_bytes

        def num_readers(self) -> int:
            with self.lock:
                return self._num_readers
        
        def num_writers(self) -> int:
            with self.lock:
                return self._num_writers
        
        def error(self) -> bool:
            with self.lock:
                return self._error
        
        def removable(self) -> bool:
            with self.lock:
                return self._removable

        def removed(self) -> bool:
            with self.lock:
                return self._removed
        
        def writable(self) -> bool:
            with self.lock:
                return self._writable

        def add_reader(self):
            with self.lock:
                if self._removed:
                    raise FileCacheError('Cannot add reader. File [{}] is removed'.format(self.file_id()))
                if self._error:
                    raise FileCacheError('Cannot add reader. File [{}] in error state'.format(self.file_id()))

                self._num_readers += 1
                self.set_removable(False)

        def remove_reader(self):
            with self.lock:
                if self._num_readers == 0:
                    raise FileCacheError('Cannot remove reader. File [{}] has no readers!'.format(self.file_id()))

                self._num_readers -= 1
                self.set_removable(True)

        def reader_wait(self, timeout: float = None):
            with self.lock:
                self._readers.wait(timeout)

        def add_writer(self):
            with self.lock:
                if self._removed:
                    raise FileCacheError('Cannot add writer. File [{}] is removed'.format(self.file_id()))
                if self._error:
                    raise FileCacheError('Cannot add writer. File [{}] in error state'.format(self.file_id()))
                if self._num_writers != 0:
                    raise FileCacheError('Cannot add writer. File [{}] has writers!'.format(self.file_id()))
                if not self._writable:
                    raise FileCacheError('Cannot add writer. File [{}] is not writable'.format(self.file_id()))

                self._num_writers = 1
                self.set_removable(False)

        def remove_writer(self):
            with self.lock:
                if self._num_writers == 0:
                    raise FileCacheError('Cannot remove writer. File [{}] has no writers!'.format(self.file_id()))

                self._num_writers = 0
                self.set_removable(True)
                self._readers.notify_all()

        def set_error(self):
            with self.lock:
                if not self._error:
                    self._error = True
                    self._readers.notify_all()
                    logging.debug('File [{}] in error state'.format(self.file_id()))

        def set_removable(self, removable: bool):
            with self.lock:
                if removable:
                    if self._num_readers == 0 and self._num_writers == 0:
                        self._removable = True
                        logging.debug('File [{}] now removable'.format(self.file_id()))
                else:
                    self._removable = removable
                    logging.debug('File [{}] is not removable'.format(self.file_id()))

        def set_removed(self):
            with self.lock:
                if self._num_readers != 0:
                    raise FileCacheError('Cannot remove file [{}] with [{}] readers'.format(self.file_id(), self._num_readers))
                if self._num_writers != 0:
                    raise FileCacheError('Cannot remove file [{}] with [{}] writers'.format(self.file_id(), self._num_writers))

                if not self._removed:
                    self._removed = True
                    logging.debug('File [{}] removed'.format(self.file_id()))

        def unset_writable(self):
            with self.lock:
                if self._num_writers != 0:
                    raise FileCacheError('Cannot make file [{}] with [{}] writers not writable'.format(self.file_id(), self._num_writers))

                if self._writable:
                    self._writable = False
                    self._readers.notify_all()
                    logging.debug('File [{}] no longer writable'.format(self.file_id()))

    class CacheFileReader(File):

        def __init__(self, file_id: str, node: 'FileCache.IndexNode', chunk_size: int = KILOBYTE, encode_chunk: chunk_encoder=default_chunk_encoder, decode_chunk: chunk_encoder=default_chunk_decoder, skip_metadata=False):
            super().__init__(node.index().cache().cache_path(), file_id, mode='r', chunk_size=chunk_size, encode_chunk=encode_chunk, decode_chunk=decode_chunk, skip_metadata=skip_metadata)
            self._node = node

        def node(self):
            return self._node

        def append_chunk(self, chunk_bytes):
            raise FileCacheError('Operation not supported!', FileServerErrorCode.INTERNAL_ERROR)

        def write(self, data):
            raise FileCacheError('Operation not supported!', FileServerErrorCode.INTERNAL_ERROR)

        def close(self):
            if not self.closed():
                error = None

                try:
                    super().close()
                except Exception as e:
                    error = e
                
                self._node.remove_reader()
                if error is not None:
                    raise error

    class ConcurrentCacheFileReader(CacheFileReader):

        def __init__(self, file_id: str, node: 'FileCache.IndexNode', chunk_size: int = KILOBYTE, encode_chunk: chunk_encoder=default_chunk_encoder, decode_chunk: chunk_encoder=default_chunk_decoder, read_timeout=90):
            super().__init__(file_id, node, chunk_size, encode_chunk, decode_chunk, skip_metadata=True)
            # TODO: Configure from config value.
            self._read_timeout = read_timeout
            self._total_chunks = self._node.available_chunks()

        def file_size(self):
            return self.node().available_bytes()

        def size_on_disk(self):
            return self.node().used_space()

        def seek_chunk(self, offset):
            if offset > self._total_chunks:
                self.wait_for_chunks(offset)
            
            return super().seek_chunk(offset)

        def read_chunk(self):
            if self._chunks_read == self._total_chunks:
                self.wait_for_chunks(self._chunks_read+1)
            
            return super().read_chunk()

        def wait_for_chunks(self, num_chunks):
            now = start_t = time.time()
            end_t = start_t + self._read_timeout
            node = self.node()

            while True:
                with node.lock:
                    if node.error():
                        raise FileCacheError('Cannot read file [{}]. File in error state!'.format(self.file_id()))
                    
                    self._total_chunks = node.available_chunks()
                    if not node.writable() or self._total_chunks >= num_chunks:
                        break

                    now = time.time()
                    if now < end_t:
                        node.reader_wait(end_t - now)
                    else:
                        raise FileCacheError('Timed out waiting for file [{}] chunks'.format(self.file_id()), FileServerErrorCode.IO_TIMEOUT)

    class CacheFileWriter(File):

        def __init__(self, file_id: str, node: 'FileCache.IndexNode', mode: str='w', chunk_size: int = KILOBYTE, encode_chunk: chunk_encoder=default_chunk_encoder, decode_chunk: chunk_decoder=default_chunk_decoder,):
            super().__init__(node.index().cache().cache_path(), file_id, mode=mode, chunk_size=chunk_size, encode_chunk=encode_chunk, decode_chunk=decode_chunk)
            self._node = node

        def node(self):
            return self._node

        def append_chunk(self, chunk_bytes):
            node = self._node
            if node.error():
                raise FileCacheError('Cannot append chunk to file [{}] in error state'.format(self.file_id()))

            prev_size = self.size_on_disk()
            try:
                super().append_chunk(chunk_bytes)
            except Exception as e:
                node.set_error()
                raise e
            curr_size = self.size_on_disk()
            
            if curr_size > prev_size:
                file_size = self.file_size()
                total_chunks = self.total_chunks()
                with node.lock:
                    if curr_size > node.alloc_space():
                        node.index().cache().resize_node(node, curr_size)
                    
                    node.set_available_bytes(file_size)
                    node.set_used_space(curr_size)
                    node.set_available_chunks(total_chunks)

        def close(self):
            if not self.closed():
                error = None
                
                try:
                    super().close()
                except Exception as e:
                    error = e

                self._node.remove_writer()
                if error is not None:
                    raise error
                    

    IndexNodeMetadata = namedtuple('IndexNodeMetadata', ['alloc_space', 'file_size', 'size_on_disk', 'file_chunks'])

    class Index(object):

        def __init__(self, cache: 'FileCache'):
            self._cache = cache
            self._head = None
            self._tail = None
            self._files = dict()
        
        def cache(self) -> 'FileCache':
            return self._cache

        def head(self) -> 'FileCache.IndexNode':
            return self._head
        
        def tail(self) -> 'FileCache.IndexNode':
            return self._tail

        def __len__(self):
            return len(self._files)

        def has_node(self, file_id: str) -> bool:
            if file_id is None or not File.is_valid_file_id(file_id):
                raise FileError('Invalid file id!', FileServerErrorCode.INVALID_FILE_ID)

            return file_id in self._files

        def get_node(self, file_id: str) -> 'FileCache.IndexNode':
            if file_id is None or not File.is_valid_file_id(file_id):
                raise FileError('Invalid file id!', FileServerErrorCode.INVALID_FILE_ID)

            return self._files.get(file_id)

        def add_node(self, node: 'FileCache.IndexNode') -> None:
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

        def pop_node(self, file_id: str) -> 'FileCache.IndexNode':
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


    def __init__(self, cache_config: Union[dict, configparser.ConfigParser]):
        self._cache_path: str = cache_config.get('store-path', './cache')
        self._cache_used = 0
        self._cache_size = parse_mem_size(cache_config.get('store-size', '1GB'))
        self._chunk_size = parse_mem_size(cache_config.get('chunk-size', '1MB'))
        self._max_file_size = parse_mem_size(cache_config.get('max-file-size', '500MB'))
        self._file_eviction = config_bool(cache_config.get('enable-file-eviction', '1'))

        self._index = FileCache.Index(cache=self)
        self._index_lock = RLock()

        if not os.path.exists(self._cache_path):
            os.mkdir(self._cache_path)
            logging.info('File cache created in path [{}]'.format(self._cache_path))
        else:
            for file_id in os.listdir(self._cache_path):
                try:
                    f = File(self._cache_path, file_id, mode='r')
                    self.create_cache_entry(file_id, f.size_on_disk(), writable=False, removable=True)
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

    def create_cache_entry(self, file_id: str, alloc_space: int, writable: bool, removable: bool) -> 'FileCache.IndexNode':
        with self._index_lock:
            if self._index.has_node(file_id):
                raise FileCacheError('File [{}] already exists in cache'.format(file_id), FileServerErrorCode.FILE_EXISTS)
            logging.debug('Create cache entry for file [{}] using [{}] space'.format(file_id, str_mem_size(alloc_space)))
            self.ensure_cache_space(alloc_space)
            node = FileCache.IndexNode(self._index, file_id, alloc_space, writable, removable)
            self._index.add_node(node)
            self._cache_used += alloc_space
            logging.debug('Created cache entry for file [{}] using [{}] space'.format(file_id, str_mem_size(alloc_space)))
            return node

    '''
        Open file in cache for reading.
        While file is opened, it will be locked preventing its removal from the cache.
        Allow multiple readers and single writer.

        If file is not found, return None.
        Otherwise, return a file like object for reading.
    '''
    def read_file(self, file_id: str, encode_chunk: chunk_encoder=default_chunk_encoder, decode_chunk: chunk_decoder=default_chunk_decoder) -> Optional['FileCache.CacheFileReader']:
        with self._index_lock:
            if self._index.has_node(file_id):
                node = self._index.get_node(file_id)

                #
                # This is now the MRU (most recently used) file.
                #
                self._index.move_to_back(file_id)
            else:
                return

        with node.lock:
            node.add_reader()

            try:
                if node.writable():
                    reader = FileCache.ConcurrentCacheFileReader(file_id, node, chunk_size=self.file_chunk_size(), encode_chunk=encode_chunk, decode_chunk=decode_chunk)
                else:
                    reader = FileCache.CacheFileReader(file_id, node, chunk_size=self.file_chunk_size(), encode_chunk=encode_chunk, decode_chunk=decode_chunk)
            except Exception as e:
                logging.error('Error opening file [{}] reader: {}'.format(file_id, str(e)))
                node.remove_reader()
                raise e
            
            return reader

    '''
        Open file in cache for writing.
        While file is opened, it will be locked preventing its removal from the cache.
        Allow multiple readers and single writer.

        Return a file like object for writing.
    '''
    def write_file(self, file_id: Optional[str]=None, alloc_space: int=0, encode_chunk: chunk_encoder=default_chunk_encoder, decode_chunk: chunk_decoder=default_chunk_decoder) -> 'FileCache.CacheFileWriter':
        with self._index_lock:
            if file_id is None:
                file_id = File.generate_file_id()

            node = self.create_cache_entry(file_id, alloc_space, writable=True, removable=False)
            node.add_writer()

        with node.lock:
            try:
                writer = FileCache.CacheFileWriter(file_id, node, chunk_size=self.file_chunk_size(), encode_chunk=encode_chunk, decode_chunk=decode_chunk)
            except Exception as e:
                logging.error('Error opening file [{}] writer: {}'.format(file_id, str(e)))
                node.remove_writer()
                try:
                    self.remove_file_by_node(node)
                except Exception as e1:
                    logging.warn('Error cleaning up file [{}] writer: {}'.format(file_id, str(e1)))
                raise e
            
            return writer

    '''
        Open file in cache for appending.
        File must already exist in the cache.
        While file is opened, it will be locked preventing its removal from the cache.
        Allow multiple readers and single writer.

        Return a file like object for appending.
    '''
    def append_file(self, file_id: str, encode_chunk: chunk_encoder=default_chunk_encoder, decode_chunk: chunk_decoder=default_chunk_decoder) -> 'FileCache.CacheFileWriter':
        with self._index_lock:
            if not self._index.has_node(file_id):
                raise FileCacheError('File [{}] not found in cache'.format(file_id), FileServerErrorCode.FILE_NOT_FOUND)

            node = self._index.get_node(file_id)

        with node.lock:
            node.add_writer()

            try:
                writer = FileCache.CacheFileWriter(file_id, node, mode='a', chunk_size=self.file_chunk_size(), encode_chunk=encode_chunk, decode_chunk=decode_chunk)
            except Exception as e:
                node.remove_writer()
                node.set_error()
                raise e
            
            return writer

    '''
        Create empty file in the cache with file_size bytes reserved.
        File will be writable and appendable until closed.

        Throws error if file with given id already exists in cache or if there
        is insufficient space in the cache.

    '''
    def create_empty_file(self, file_id: str, alloc_space: int, writable: bool = True, removable: bool = False) -> None:
        with self._index_lock:
            if file_id is None:
                file_id = File.generate_file_id()

            node = self.create_cache_entry(file_id, alloc_space, writable=writable, removable=removable)
            node.add_writer()

        with node.lock:
            try:
                File.create_empty(self._cache_path, file_id)
                node.remove_writer()
                logging.debug('Empty file [{}] created with [{}] space allocated'.format(file_id, str_mem_size(alloc_space)))
            except Exception as e:
                logging.error('Error creating empty file [{}]: {}'.format(file_id, str(e)))
                node.remove_writer()
                try:
                    self.remove_file_by_node(node)
                except Exception as e1:
                    logging.error('Error cleaning up empty file [{}]: {}'.format(file_id, str(e1)))
                raise e

    '''
        Retrieve file metadata:
            - size_on_disk - the allocated space for the file in the cache
            - file_chunks - number of file chunks
        
        Throws error if file does not exist in cache.
    '''
    def file_metadata(self, file_id: str) -> 'FileCache.IndexNodeMetadata':
        reader = self.read_file(file_id)
        try:
            return FileCache.IndexNodeMetadata(reader.node().alloc_space(), reader.file_size(), reader.size_on_disk(), reader.total_chunks())
        finally:
            self.close_file(reader)

    '''
        Resize the amount of space allocated to the file.

    '''
    def resize_node(self, node: 'FileCache.IndexNode', size_on_disk: int) -> None:
        with node.lock:
            file_id = node.file_id()
            alloc_space = node.alloc_space()

            if node.error():
                raise FileCacheError('Cannot resize file [{}] in error state'.format(file_id))
            if node.removed():
                raise FileCacheError('Cannot resize removed file [{}]'.format(file_id))

            if size_on_disk > alloc_space:
                extra_space = size_on_disk - alloc_space
                logging.debug('Allocate [{}B] extra space for file [{}]'.format(extra_space, file_id))

                try:
                    self.ensure_cache_space(extra_space)
                except Exception as e:
                    logging.error('Error allocating [{}B] extra space for file [{}]'.format(extra_space, file_id))
                    node.set_error()
                    raise e

                node.set_alloc_space(size_on_disk)
                with self._index_lock:
                    self._cache_used += extra_space
                logging.debug('Allocated [{}B] extra space for file [{}]'.format(extra_space, file_id))
            elif not node.writable():
                extra_space = alloc_space - size_on_disk

                if extra_space > 0:
                    logging.debug('Free [{}B] extra space for file [{}]'.format(extra_space, file_id))
                    node.set_alloc_space(size_on_disk)
                    with self._index_lock:
                        self._cache_used -= extra_space
                    logging.debug('Free [{}B] extra space for file [{}]'.format(extra_space, file_id))

    '''
        Close file in cache.

    '''
    def close_file(self, file: File, removable: bool=True, writable: bool=False) -> None:
        if file.closed():
            raise FileCacheError('Already closed!', FileServerErrorCode.INTERNAL_ERROR)

        file_id = file.file_id()
        mode = file.mode()
        error = None

        if mode == 'r':
            logging.debug('Closing file [{}] reader'.format(file_id))
        elif mode == 'w':
            logging.debug('Closing file [{}] writer'.format(file_id))
        elif mode == 'a':
            logging.debug('Closing file [{}] appender'.format(file_id))

        try:
            file.close()
        except Exception as e:
            logging.error('Error closing file [{}]: {}'.format(file_id, str(e)))
            error = e

        with self._index_lock:
            if self._index.has_node(file_id):
                node = self._index.get_node(file_id)
            else:
                raise FileCacheError('File [{}] not found in cache'.format(file_id))

            with node.lock:
                node.set_removable(removable)
                if mode == 'w' or mode == 'a':
                    if not writable:
                        node.unset_writable()
                    
                    if not node.error():
                        self.resize_node(node, file.size_on_disk())

        if error is not None:
            raise error

        if mode == 'r':
            logging.debug('File [{}] reader closed'.format(file_id))
        elif mode == 'w':
            logging.debug('File [{}] writer closed'.format(file_id))
        elif mode == 'a':
            logging.debug('File [{}] appender closed'.format(file_id))

    def set_file_removable(self, file_id: str, removable: bool):
        with self._index_lock:
            if self._index.has_node(file_id):
                node = self._index.get_node(file_id)
            else:
                raise FileCacheError('File [{}] not found in cache'.format(file_id))
        
        node.set_removable(removable)

    '''
        Remove file from cache.

    '''
    def remove_file(self, file: File) -> None:
        file_id = file.file_id()
        self.remove_file_by_id(file_id)

    def remove_file_by_id(self, file_id: str) -> None:
        with self._index_lock:
            if self._index.has_node(file_id):
                node = self._index.get_node(file_id)
                self.remove_file_by_node(node)
            else:
                raise FileCacheError('File [{}] not found in cache'.format(file_id), FileServerErrorCode.FILE_NOT_FOUND)

    def remove_file_by_node(self, node: 'FileCache.IndexNode') -> None:
        file_id = node.file_id()
        with node.lock:
            if node.removed():
                raise FileCacheError('File [{}] already removed'.format(file_id), FileServerErrorCode.INTERNAL_ERROR)
            if not node.removable():
                raise FileCacheError('File [{}] is not removable'.format(file_id), FileServerErrorCode.FILE_NOT_REMOVABLE)
            if node.num_readers() > 0:
                raise FileCacheError('Removing file with [{}] readers'.format(node.num_readers), FileServerErrorCode.INTERNAL_ERROR)
            if node.num_writers() > 0:
                raise FileCacheError('Removing file with [{}] writers'.format(node.num_writers), FileServerErrorCode.INTERNAL_ERROR)
            
            alloc_space = node.alloc_space()
            node.set_removed()

        self._index.pop_node(file_id)

        try:
            shutil.rmtree(os.path.join(self._cache_path, file_id))
        except Exception as e:
            logging.warn('Could not remove file [{}] from cache: {}'.format(file_id, str(e)))

        logging.debug('Removed file [{}] from cache'.format(file_id))
        self._cache_used -= alloc_space
        logging.debug('Reclaimed [{}] space in cache'.format(str_mem_size(alloc_space)))

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
            remove_nodes: list['FileCache.IndexNode'] = []
            while total_size < size and curr_node is not None:
                curr_node.lock.acquire()
                next_node = curr_node._next
                alloc_space = curr_node.alloc_space()
                if alloc_space > 0 and curr_node.removable():
                    total_size += alloc_space
                    # Lock this node for removal.
                    remove_nodes.append(curr_node)
                else:
                    curr_node.lock.release()
                curr_node = next_node

            if total_size < size:
                for node in remove_nodes:
                    node.lock.release()

                raise FileCacheError('Insufficient space in cache', FileServerErrorCode.INSUFFICIENT_SPACE)

            #
            # We can free enough space, remove the files.
            #

            for node in remove_nodes:
                self.remove_file_by_node(node)
                node.lock.release()
                logging.debug('Evicted file [{}] from cache, recovered [{}] space'.format(node.file_id(), str_mem_size(node.alloc_space())))
            
            logging.debug('Freed [{}] space in cache'.format(str_mem_size(total_size)))
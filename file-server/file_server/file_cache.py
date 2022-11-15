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

        def __init__(self, index: 'FileCache.Index', file_id: str, alloc_space: int):
            self._index = index
            self._file_id = file_id
            self.alloc_space = alloc_space
            self.num_readers: int = 0
            self.num_writers: int = 0
            self.removable: bool = False
            self.removed: bool = False
            self.writable: bool = False
            self._prev: 'FileCache.IndexNode' = None
            self._next: 'FileCache.IndexNode' = None
            self.lock = RLock()
        
        def index(self) -> 'FileCache.Index':
            return self._index

        def file_id(self) -> str:
            return self._file_id

    class CacheFileReader(File):

        def __init__(self, file_id: str, node: 'FileCache.IndexNode', chunk_size: int = KILOBYTE, encode_chunk: chunk_encoder=default_chunk_encoder, decode_chunk: chunk_encoder=default_chunk_decoder):
            super().__init__(node.index().cache().cache_path(), file_id, mode='r', chunk_size=chunk_size, encode_chunk=encode_chunk, decode_chunk=decode_chunk)
            self._node = node
            logging.debug('File [{}] opened for reading'.format(file_id))

        def append_chunk(self, chunk_bytes):
            raise FileCacheError('Operation not supported!', FileServerErrorCode.INTERNAL_ERROR)

        def write(self, data):
            raise FileCacheError('Operation not supported!', FileServerErrorCode.INTERNAL_ERROR)

        def close(self):
            if not self.closed():
                super().close()
                with self._node.lock:
                    if self._node.num_readers <= 0:
                        raise FileCacheError('File [{}] in invalid state! File has [{}] readers'.format(self.file_id(), self._node.num_readers), FileServerErrorCode.INTERNAL_ERROR)
                    self._node.num_readers -= 1

    class CacheFileWriter(File):

        def __init__(self, file_id: str, node: 'FileCache.IndexNode', mode: str='w', chunk_size: int = KILOBYTE, encode_chunk: chunk_encoder=default_chunk_encoder, decode_chunk: chunk_decoder=default_chunk_decoder,):
            super().__init__(node.index().cache().cache_path(), file_id, mode=mode, chunk_size=chunk_size, encode_chunk=encode_chunk, decode_chunk=decode_chunk)
            self._node = node

        def append_chunk(self, chunk_bytes):
            prev_size = self.size_on_disk()
            super().append_chunk(chunk_bytes)
            curr_size = self.size_on_disk()
            if curr_size > prev_size:
                node = self._node
                with node.lock:
                    if curr_size > node.alloc_space:
                        node.index().cache().resize_node(node, curr_size)

        def close(self):
            if not self.closed():
                super().close()
                with self._node.lock:
                    if self._node.num_writers != 1:
                        raise FileCacheError('File [{}] in invalid state! File has [{}] writers'.format(self.file_id(), self._node.num_writers), FileServerErrorCode.INTERNAL_ERROR)
                    self._node.num_writers = 0

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
                    node = self.create_cache_entry(file_id, f.size_on_disk())
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

    def create_cache_entry(self, file_id: str, alloc_space: int) -> 'FileCache.IndexNode':
        with self._index_lock:
            if self._index.has_node(file_id):
                raise FileCacheError('File [{}] already exists in cache'.format(file_id), FileServerErrorCode.FILE_EXISTS)
            logging.debug('Create cache entry for file [{}] using [{}] space'.format(file_id, str_mem_size(alloc_space)))
            self.ensure_cache_space(alloc_space)
            node = FileCache.IndexNode(self._index, file_id, alloc_space)
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
    def read_file(self, file_id: str, encode_chunk: chunk_encoder=default_chunk_encoder, decode_chunk: chunk_decoder=default_chunk_decoder) -> Optional[File]:
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
            if node.removed:
                return
            if node.num_writers != 0:
                raise FileCacheError('File [{}] has [{}] writers!'.format(file_id, node.num_writers), FileServerErrorCode.FILE_NOT_READABLE)

            node.num_readers += 1
            node.removable = False

            return FileCache.CacheFileReader(file_id, node, chunk_size=self.file_chunk_size(), encode_chunk=encode_chunk, decode_chunk=decode_chunk)

    '''
        Open file in cache for writing.
        While file is opened, it will be locked preventing its removal from the cache.
        Allow multiple readers and single writer.

        Return a file like object for writing.
    '''
    def write_file(self, file_id: Optional[str]=None, alloc_space: int=0, encode_chunk: chunk_encoder=default_chunk_encoder, decode_chunk: chunk_decoder=default_chunk_decoder) -> File:
        with self._index_lock:
            if file_id is None:
                file_id = File.generate_file_id()

            node = self.create_cache_entry(file_id, alloc_space)
            node.num_writers = 1
            node.writable = True
            node.removable = False

        with node.lock:
            if node.removed:
                raise FileCacheError('Non-removable file [{}] was removed!'.format(file_id))

            return FileCache.CacheFileWriter(file_id, node, chunk_size=self.file_chunk_size(), encode_chunk=encode_chunk, decode_chunk=decode_chunk)

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

        with node.lock:
            if node.removed:
                raise FileCacheError('File [{}] not found in cache'.format(file_id), FileServerErrorCode.FILE_NOT_FOUND)
            if node.num_writers != 0:
                raise FileCacheError('File [{}] has [{}] writers!'.format(file_id, node.num_writers), FileServerErrorCode.FILE_NOT_WRITABLE)
            if not node.writable:
                raise FileCacheError('File [{}] is not writable!'.format(file_id), FileServerErrorCode.FILE_NOT_WRITABLE)
            node.num_writers = 1
            node.removable = False

            return FileCache.CacheFileWriter(file_id, node, mode='a', chunk_size=self.file_chunk_size(), encode_chunk=encode_chunk, decode_chunk=decode_chunk)

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

            node = self.create_cache_entry(file_id, alloc_space)
            node.writable = writable
            node.removable = removable

        with node.lock:
            if node.removed:
                if not removable:
                    raise FileCacheError('Non-removable empty file [{}] was removed!'.format(file_id))
                return

            File.create_empty(self._cache_path, file_id)
            logging.debug('Empty file [{}] created with [{}] space allocated'.format(file_id, str_mem_size(alloc_space)))

    '''
        Retrieve file metadata:
            - size_on_disk - the allocated space for the file in the cache
            - file_chunks - number of file chunks
        
        Throws error if file does not exist in cache.
    '''
    def file_metadata(self, file_id: str) -> 'FileCache.IndexNodeMetadata':
        with self._index_lock:
            if not self._index.has_node(file_id):
                raise FileCacheError('File [{}] not found in cache'.format(file_id), FileServerErrorCode.FILE_NOT_FOUND)
            
            node = self._index.get_node(file_id)

        with node.lock:
            if node.removed:
                raise FileCacheError('File [{}] not found in cache'.format(file_id), FileServerErrorCode.FILE_NOT_FOUND)
            node.num_readers += 1
            node.removable = False
            reader = FileCache.CacheFileReader(file_id, node, chunk_size=self.file_chunk_size())
            try:
                return FileCache.IndexNodeMetadata(node.alloc_space, reader.file_size(), reader.size_on_disk(), reader.total_chunks())
            finally:
                self.close_file(reader)

    '''
        Resize the amount of space allocated to the file.

    '''
    def resize_node(self, node: 'FileCache.IndexNode', size_on_disk: int) -> None:
        with node.lock:
            file_id = node.file_id()
            alloc_space = node.alloc_space
            if size_on_disk > alloc_space:
                extra_space = size_on_disk - alloc_space
                logging.debug('Allocate [{}] extra space for file [{}]'.format(str_mem_size(extra_space), file_id))
                self.ensure_cache_space(extra_space)
                node.alloc_space = size_on_disk
                with self._index_lock:
                    self._cache_used += extra_space
                logging.debug('Allocated [{}] extra space for file [{}]'.format(str_mem_size(extra_space), file_id))
            elif not node.writable:
                extra_space = alloc_space - size_on_disk
                if extra_space > 0:
                    logging.debug('Free [{}] extra space for file [{}]'.format(str_mem_size(extra_space), file_id))
                    node.alloc_space = size_on_disk
                    with self._index_lock:
                        self._cache_used -= extra_space
                    logging.debug('Free [{}] extra space for file [{}]'.format(str_mem_size(extra_space), file_id))

    '''
        Close file in cache.

    '''
    def close_file(self, file: File, removable: bool=True, writable: bool=False) -> None:
        if file.closed():
            raise FileCacheError('Already closed!', FileServerErrorCode.INTERNAL_ERROR)

        try:
            logging.debug('Closing file [{}] mode [{}] in cache'.format(file.file_id(), file.mode()))
            file.close()
        except Exception as e:
            mode = file.mode()
            logging.error('Error closing file [{}] mode [{}]: {}'.format(file.file_id(), mode, str(e)))
            if mode == 'w' or mode == 'a':
                # Allow the file to be removed if something goes wrong closing writer.
                node.writable = False
                node.removable = True
            raise e

        with self._index_lock:
            file_id = file.file_id()
            if self._index.has_node(file_id):
                node = self._index.get_node(file_id)
            else:
                raise FileCacheError('File [{}] not found in cache'.format(file_id))
            
            size_on_disk = file.size_on_disk()
            mode = file.mode()

            if mode == 'r':
                logging.debug('File [{}] reader closed'.format(file_id))
            elif mode == 'w':
                logging.debug('File [{}] writer closed'.format(file_id))
            elif mode == 'a':
                logging.debug('File [{}] appender closed'.format(file_id))

            with node.lock:
                self.set_file_removable(node, removable)
                if mode == 'w' or mode == 'a':
                    if node.writable:
                        if not writable:
                            node.writable = writable
                            logging.debug('File [{}] no longer writable'.format(file_id))
                    
                    self.resize_node(node, size_on_disk)

    def set_file_removable(self, node: 'FileCache.IndexNode', removable: bool):
        with node.lock:
            if removable:
                if node.num_readers == 0 and node.num_writers == 0:
                    node.removable = True
                    logging.debug('File [{}] now removable'.format(node.file_id()))
            else:
                node.removable = removable
                logging.debug('File [{}] is not removable'.format(node.file_id()))

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
            if node.removed:
                raise FileCacheError('File [{}] already removed'.format(file_id), FileServerErrorCode.INTERNAL_ERROR)
            if not node.removable:
                raise FileCacheError('File [{}] is not removable'.format(file_id), FileServerErrorCode.FILE_NOT_REMOVABLE)
            if node.num_readers > 0:
                raise FileCacheError('Removing file with [{}] readers'.format(node.num_readers), FileServerErrorCode.INTERNAL_ERROR)
            if node.num_writers > 0:
                raise FileCacheError('Removing file with [{}] writers'.format(node.num_writers), FileServerErrorCode.INTERNAL_ERROR)
            
            alloc_space = node.alloc_space
            node.removed = True

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
                if curr_node.alloc_space > 0 and curr_node.removable:
                    total_size += curr_node.alloc_space
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
                logging.debug('Evicted file [{}] from cache, recovered [{}] space'.format(node.file_id(), str_mem_size(node.alloc_space)))
            
            logging.debug('Freed [{}] space in cache'.format(str_mem_size(total_size)))
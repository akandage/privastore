from select import select
from .error import FileCacheError, FileError
from .file import File
from .util.file import parse_mem_size, str_mem_size
import logging
import os
import shutil
from threading import Event, RLock

READY = 'ready'
READABLE_FLAG = 'readable'
WRITABLE_FLAG = 'writable'
REMOVABLE_FLAG = 'removable'
FILE_PATH = 'file_path'
FILE_NAME = 'file_name'
FILE_SIZE = 'file_size'
FILE_VERSION = 'file_version'

def default_file_factory(cache_path, file_id=None, mode='r'):
    return File(cache_path, file_id=file_id, mode=mode)

class FileCache(object):

    class IndexNode(object):

        def __init__(self, file_id, file_size, readable, writable, removable):
            self._file_id = file_id
            self.file_size = file_size
            self.readable = readable
            self.writable = writable
            self.removable = removable
            self._prev = None
            self._next = None
            self._ready = Event()
        
        def file_id(self):
            return self._file_id
        
        def set_ready(self):
            self._ready.set()

        def wait_ready(self, timeout=None):
            self._ready.wait(timeout)

    class Index(object):

        def __init__(self):
            self._head = None
            self._tail = None
            self._files = dict()
        
        def head(self):
            return self._head
        
        def tail(self):
            return self._tail

        def __len__(self):
            return len(self._files)

        def has_node(self, file_id):
            return file_id in self._files

        def get_node(self, file_id):
            return self._files.get(file_id)

        def add_node(self, node):
            file_id = node.file_id()
            if file_id is None or not File.is_valid_file_id(file_id):
                raise FileCacheError('Node missing or invalid file id!')
            if node._prev is not None or node._next is not None:
                raise FileCacheError('Node prev or next pointers already set!')
            if self.has_node(file_id):
                raise FileCacheError('Node already in index!')
            
            self._files[file_id] = node
            if self._tail is not None:
                node._prev = self._tail
                self._tail._next = node
                self._tail = node
            else:
                self._head = self._tail = node

        def pop_node(self, file_id):
            if not self.has_node(file_id):
                raise FileCacheError('Node is not in index!')
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
        
        def move_to_back(self, file_id):
            node = self.pop_node(file_id)
            self.add_node(node)


    def __init__(self, cache_config, file_factory=default_file_factory):
        self._file_factory = file_factory
        self._cache_path = cache_config.get('cache-path', './cache')
        self._cache_used = 0
        self._cache_size = parse_mem_size(cache_config.get('cache-size', '1GB'))
        self._chunk_size = parse_mem_size(cache_config.get('chunk-size', '1MB'))

        self._index = FileCache.Index()
        self._index_lock = RLock()

        if not os.path.exists(self._cache_path):
            os.mkdir(self._cache_path)
            logging.info('File cache created in path [{}]'.format(self._cache_path))
        else:
            for file_id in os.listdir(self._cache_path):
                try:
                    f = File(self._cache_path, file_id, mode='r')
                    self.create_cache_entry(file_id, f.size_on_disk(), readable=True, writable=False, removable=True)
                    f.close()
                except Exception as e:
                    logging.error('Error initializing cache file [{}]: {}'.format(file_id, str(e)))
        
        logging.debug('File cache used [{}]'.format(str_mem_size(self._cache_used)))
        logging.debug('File cache size [{}]'.format(str_mem_size(self._cache_size)))
        logging.debug('File chunk size [{}]'.format(str_mem_size(self._chunk_size)))

    def cache_path(self):
        return self._cache_path

    def cache_size(self):
        return self._cache_size
    
    def cache_used(self):
        return self._cache_used
    
    def cache_free_space(self):
        free_space = 0
        with self._index_lock:
            free_space = max(0, self._cache_size - self._cache_used)
        return free_space
    
    def file_chunk_size(self):
        return self._chunk_size

    def create_cache_entry(self, file_id, file_size, readable=False, writable=True, removable=False):
        with self._index_lock:
            if self._index.has_node(file_id):
                raise FileCacheError('File [{}] already exists in cache'.format(file_id))
            if file_size > self.cache_free_space():
                raise FileCacheError('Not enough free space in cache')
            node = FileCache.IndexNode(file_id, file_size, readable, writable, removable)
            self._index.add_node(node)
            self._cache_used += file_size

    '''
        Open file in cache for reading.
        While file is opened, it will be locked preventing its removal from the cache.
        Allow multipled readers and single writer.

        If file is found (cache hit) return the index entry - the readable
        flag will indicate whether the file can be read from.
        Otherwise, the file is not found (cache miss) and None is returned.
        If a timeout (in seconds) is specified, the read will be blocking.
 
    '''
    def read_file(self, file_id, timeout=None):
        self._index_lock.acquire()
        try:
            if self._index.has_node(file_id):
                node = self._index.get_node(file_id)
                if timeout is not None:
                    #
                    # Release the lock and then wait for the file to
                    # be ready to read. Once the flag is set or we timeout
                    # reacquire the lock and check the index entry again.
                    #
                    self._index_lock.release()
                    node.wait_ready(timeout)
                    self._index_lock.acquire()
                    if not self._index.has_node(file_id):
                        #
                        # File was removed while we waited, cache miss.
                        #
                        return

                if node.readable:
                    if node.writable:
                        raise FileCacheError('File [{}] in invalid state'.format(file_id))
                    node.removable = False
                    #
                    # This is now the MRU (most recently used) file.
                    #
                    self._index.move_to_back(file_id)
                    return self._file_factory(self._cache_path, file_id, mode='r')

                # Cache miss.
                return
        finally:
            self._index_lock.release()


    '''
        Open file in cache for writing or appending.
        While file is opened, it will be locked preventing its removal from the cache.
        Allow multipled readers and single writer.

        New file is opened for writing in the cache and returned. The file
        must be closed in order for it be accessed by readers.
    '''
    def open_file(self, file_id=None, file_size=0, mode='w'):
        if mode == 'w':
            if file_id is not None:
                self.create_cache_entry(file_id, file_size)
                file = self._file_factory(self._cache_path, file_id=file_id, mode=mode)
            else:
                file = self._file_factory(self._cache_path, file_id=file_id, mode=mode)
                self.create_cache_entry(file.file_id(), file_size)
            return file
        elif mode == 'a':
            if file_id is not None:
                if File.is_valid_file_id(file_id):
                    with self._index_lock:
                        if self._index.has_node(file_id):
                            node = self._index.get_node(file_id)
                            if node.readable:
                                raise FileCacheError('Cannot append to readable file [{}]'.format(file_id))
                            if node.removable:
                                raise FileCacheError('Cannot append to removable file [{}]'.format(file_id))
                            if node.writable:
                                return self._file_factory(self._cache_path, file_id, mode)

                            raise FileCacheError('File [{}] in invalid state'.format(file_id))
                        else:
                            raise FileCacheError('File [{}] not found in cache'.format(file_id))
                else:
                    raise FileCacheError('Invalid file id')
            else:
                raise FileCacheError('No file id specified')
        else:
            raise FileCacheError('Invalid mode')

    '''
        Close file in cache.

    '''
    def close_file(self, file, readable=True, writable=False, removable=True):
        file.close()
        with self._index_lock:
            file_id = file.file_id()
            mode = file.mode()
            if self._index.has_node(file_id):
                node = self._index.get_node(file_id)
                node.readable = readable
                node.writable = writable
                node.removable = removable
                if mode == 'w' or mode == 'a':
                    prev_size = node.file_size
                    curr_size = file.size_on_disk()
                    node.file_size = curr_size
                    self._cache_used += curr_size-prev_size
                    if self._cache_used > self._cache_size:
                        raise FileCacheError('File cache is full!')
                if readable:
                    node.set_ready()
            else:
                raise FileCacheError('File [{}] not found in cache'.format(file_id))

    '''
        Remove file from cache.

    '''
    def remove_file(self, file):
        with self._index_lock:
            file_id = file.file_id()
            if self._index.has_node(file_id):
                node = self._index.pop_node(file_id)
                # Notify any readers waiting.
                node.set_ready()
                file.remove()
                self._cache_used -= node.file_size()
            else:
                raise FileCacheError('File [{}] not found in cache'.format(file_id))

    '''
        Remove the LRU (least recently used file) from the cache to free up space.

    '''
    def remove_lru_file(self):
        # TODO
        pass
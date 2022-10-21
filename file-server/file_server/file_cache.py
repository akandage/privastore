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

        def __init__(self, file_id, file_size):
            self._file_id = file_id
            self.file_size = file_size
            self.num_readers = 0
            self.num_writers = 0
            self.removable = False
            self._prev = None
            self._next = None
            self._ready = Event()
        
        def file_id(self):
            return self._file_id
        
        def set_ready(self):
            self._ready.set()

        def clear_ready(self):
            self._ready.clear()

        def wait_ready(self, timeout=None):
            return self._ready.wait(timeout)
        
        def __str__(self):
            return 'FileCache.IndexNode({{file_id={}, file_size={}, num_readers={}, num_writers={}}})'.format(self.file_id(), self.file_size, self.num_readers, self.num_writers)

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
        self._max_file_size = parse_mem_size(cache_config.get('max-file-size', '500MB'))

        self._index = FileCache.Index()
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
        
        logging.debug('File cache used [{}]'.format(str_mem_size(self._cache_used)))
        logging.debug('File cache size [{}]'.format(str_mem_size(self._cache_size)))
        logging.debug('File chunk size [{}]'.format(str_mem_size(self._chunk_size)))
        logging.debug('Max file size [{}]'.format(str_mem_size(self._max_file_size)))

    def cache_path(self):
        return self._cache_path

    def cache_size(self):
        return self._cache_size
    
    def cache_used(self):
        cache_used = 0
        with self._index_lock:
            cache_used = self._cache_used
        return cache_used
    
    def cache_free_space(self):
        free_space = 0
        with self._index_lock:
            free_space = max(0, self._cache_size - self._cache_used)
        return free_space
    
    def file_chunk_size(self):
        return self._chunk_size
    
    def max_file_size(self):
        return self._max_file_size

    def has_file(self, file_id):
        with self._index_lock:
            return self._index.has_node(file_id)

    def create_cache_entry(self, file_id, file_size):
        with self._index_lock:
            if self._index.has_node(file_id):
                raise FileCacheError('File [{}] already exists in cache'.format(file_id))
            self.ensure_cache_space(file_size)
            node = FileCache.IndexNode(file_id, file_size)
            self._index.add_node(node)
            self._cache_used += file_size
            return node

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
                    if not node.wait_ready(timeout):
                        logging.debug('Timed out waiting to read file [{}]'.format(file_id))
                        return
                    self._index_lock.acquire()
                    if not self._index.has_node(file_id):
                        #
                        # File was removed while we waited, cache miss.
                        #
                        return

                node = self._index.get_node(file_id)
                if node.num_writers != 0:
                    raise FileCacheError('File [{}] is being written')

                #
                # Disallow removing the file from the cache while it is being
                # read.
                #
                node.removable = False
                node.num_readers += 1
                #
                # This is now the MRU (most recently used) file.
                #
                self._index.move_to_back(file_id)
                return self._file_factory(self._cache_path, file_id, mode='r')
        finally:
            try:
                self._index_lock.release()
            except Exception as e:
                logging.warn('Error releasing cache index lock: {}'.format(str(e)))


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
                if File.is_valid_file_id(file_id):
                    node = self.create_cache_entry(file_id, file_size)
                    file = self._file_factory(self._cache_path, file_id=file_id, mode=mode)
                else:
                    raise FileCacheError('Invalid file id')
            else:
                file = self._file_factory(self._cache_path, file_id=file_id, mode=mode)
                node = self.create_cache_entry(file.file_id(), file_size)
            node.num_writers += 1
            return file
        elif mode == 'a':
            if file_id is not None:
                if File.is_valid_file_id(file_id):
                    with self._index_lock:
                        if self._index.has_node(file_id):
                            node = self._index.get_node(file_id)
                            if node.num_readers != 0:
                                raise FileCacheError('File [{}] is being read'.format(file_id))
                            if node.num_writers != 0:
                                raise FileCacheError('File [{}] is being written'.format(file_id))

                            #
                            # Disallow removing the file from the cache while it is being
                            # written.
                            #
                            node.removable = False
                            node.num_writers += 1
                            node.clear_ready()
                            #
                            # This is now the MRU (most recently used) file.
                            #
                            self._index.move_to_back(file_id)
                            return self._file_factory(self._cache_path, file_id, mode)
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
    def close_file(self, file, removable=True):
        if file.closed():
            raise FileCacheError('Already closed!')
        file.close()
        with self._index_lock:
            file_id = file.file_id()
            mode = file.mode()
            if self._index.has_node(file_id):
                node = self._index.get_node(file_id)
                if mode == 'r':
                    node.num_readers -= 1
                    if node.num_readers == 0:
                        node.removable = removable
                    elif node.num_readers < 0:
                        raise FileCacheError('Invalid state')
                elif mode == 'w' or mode == 'a':
                    node.num_writers -= 1
                    if node.num_writers == 0:
                        node.removable = removable
                    elif node.num_writers < 0:
                        raise FileCacheError('Invalid state')
                    prev_size = node.file_size
                    curr_size = file.size_on_disk()
                    node.file_size = curr_size
                    if curr_size >= prev_size:
                        self._cache_used += curr_size-prev_size
                    else:
                        self._cache_used -= prev_size-curr_size
                    if self._cache_used > self._cache_size:
                        raise FileCacheError('File cache is full!')
                    node.set_ready()
            else:
                raise FileCacheError('File [{}] not found in cache'.format(file_id))

    '''
        Remove file from cache.

    '''
    def remove_file(self, file):
        file_id = file.file_id()
        self.remove_file_by_id(file_id)

    def remove_file_by_id(self, file_id):
        with self._index_lock:
            if self._index.has_node(file_id):
                node = self._index.get_node(file_id)
                if not node.removable:
                    raise FileCacheError('File [{}] is not removable'.format(file_id))
                self._index.pop_node(file_id)
                if node.num_readers != 0:
                    # Notify any readers waiting.
                    node.set_ready()
                try:
                    shutil.rmtree(os.path.join(self._cache_path, file_id))
                except Exception as e:
                    logging.warn('Could not remove file [{}] from cache: {}'.format(file_id, str(e)))
                self._cache_used -= node.file_size
            else:
                raise FileCacheError('File [{}] not found in cache'.format(file_id))

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
    def ensure_cache_space(self, size):
        if size == 0:
            return
        if size < 0:
            raise FileCacheError('Invalid size!')
        if size > self._max_file_size:
            raise FileCacheError('File size too large!')
        
        with self._index_lock:
            if self.cache_free_space() >= size:
                return
            
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
                raise FileCacheError('Insufficient space in cache')

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
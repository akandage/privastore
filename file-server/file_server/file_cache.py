from .error import FileCacheError, FileError
from .file import File
from .file_chunk import default_chunk_encoder, default_chunk_decoder
from .util.file import parse_mem_size, str_mem_size
import logging
import os
import shutil
from threading import Condition, Lock, RLock
import time

class FileCache(object):

    class CacheFileReader(File):

        def __init__(self, cache_path, file_id, node, encode_chunk=default_chunk_encoder, decode_chunk=default_chunk_decoder, read_timeout=90):
            super().__init__(cache_path, file_id, mode='r', encode_chunk=encode_chunk, decode_chunk=decode_chunk, skip_metadata=True)
            self._node = node
            self._read_timeout = read_timeout

            with self._node.lock:
                self._node.num_readers += 1

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

        def read_chunk(self):
            start_t = time.time()
            end_t = start_t + self._read_timeout

            while True:
                now = time.time()
                if now >= end_t:
                    raise FileCacheError('Timed out reading file [{}]!'.format(self.file_id()))
                with self._node.lock:
                    if self._node.error:
                        raise FileCacheError('File [{}] could not be read!'.format(self.file_id()))

                    self._total_chunks = self._node.file_chunks

                    if self._node.num_writers == 0:
                        # Write end of the file closed.
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
                        raise FileCacheError('Invalid file [{}] state!'.format(self.file_id()))
                    self._node.num_readers -= 1
                
                super().close()

    class CacheFileWriter(File):

        def __init__(self, cache_path, file_id, node, mode='w', encode_chunk=default_chunk_encoder, decode_chunk=default_chunk_decoder,):
            super().__init__(cache_path, file_id, mode=mode, encode_chunk=encode_chunk, decode_chunk=decode_chunk, skip_metadata=True)
            self._node = node

            if mode != 'w' and mode != 'a':
                raise FileError('Invalid mode!')

            with self._node.lock:
                if self._node.num_writers != 0:
                    raise Exception('File [{}] already has writer!'.format(self.file_id()))
                self._node.num_writers = 1
            
            #
            # Read the metadata now that we have the lock on the file.
            #
            if mode == 'a':
                self.read_metadata_file()

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
                        raise FileCacheError('Invalid file [{}] state!'.format(self.file_id()))
                    self._node.num_writers = 0
                    self._node.reader_cv.notify_all()
            except Exception as e:
                with lock:
                    self._node.error = True
                    self._node.reader_cv.notify_all()
                raise e

    class IndexNode(object):

        def __init__(self, file_id, file_size, file_chunks):
            self._file_id = file_id
            # Size of the file on disk. Used for cache space management.
            self.file_size = file_size
            # Number of file chunks *currently* available to read.
            self.file_chunks = file_chunks
            self.num_readers = 0
            self.num_writers = 0
            self.error = False
            self.removable = False
            self._prev = None
            self._next = None
            self.lock = Lock()
            # Used to signal readers that the file has changed.
            self.reader_cv = Condition(self.lock)
        
        def file_id(self):
            return self._file_id

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
            if file_id is None or not File.is_valid_file_id(file_id):
                raise FileCacheError('Invalid file id!')

            return file_id in self._files

        def get_node(self, file_id):
            if file_id is None or not File.is_valid_file_id(file_id):
                raise FileCacheError('Invalid file id!')

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


    def __init__(self, cache_config):
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
                    node = self.create_cache_entry(file_id, f.size_on_disk(), f.total_chunks())
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

    def create_cache_entry(self, file_id, file_size, file_chunks=0):
        with self._index_lock:
            if self._index.has_node(file_id):
                raise FileCacheError('File [{}] already exists in cache'.format(file_id))
            self.ensure_cache_space(file_size)
            node = FileCache.IndexNode(file_id, file_size, file_chunks)
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
    def read_file(self, file_id, encode_chunk=default_chunk_encoder, decode_chunk=default_chunk_decoder):
        with self._index_lock:
            if self._index.has_node(file_id):
                node = self._index.get_node(file_id)

                #
                # Disallow removing the file from the cache while it is being
                # read.
                #
                node.removable = False
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
    def write_file(self, file_id=None, file_size=0, encode_chunk=default_chunk_encoder, decode_chunk=default_chunk_decoder):
        with self._index_lock:
            if file_id is None:
                file_id = File.generate_file_id()

            node = self.create_cache_entry(file_id, file_size)
            #
            # Disallow removing the file from the cache while it is being
            # written to.
            #
            node.removable = False

            return FileCache.CacheFileWriter(self._cache_path, file_id, node, encode_chunk=encode_chunk, decode_chunk=decode_chunk)

    '''
        Open file in cache for appending.
        File must already exist in the cache.
        While file is opened, it will be locked preventing its removal from the cache.
        Allow multiple readers and single writer.

        Return a file like object for appending.
    '''
    def append_file(self, file_id, encode_chunk=default_chunk_encoder, decode_chunk=default_chunk_decoder):
        with self._index_lock:
            if not self._index.has_node(file_id):
                raise FileCacheError('File [{}] not found in cache'.format(file_id))

            node = self._index.get_node(file_id)
            #
            # Disallow removing the file from the cache while it is being
            # written to.
            #
            node.removable = False

            return FileCache.CacheFileWriter(self._cache_path, file_id, node, mode='a', encode_chunk=encode_chunk, decode_chunk=decode_chunk)

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
                with node.lock:
                    if mode == 'w':
                        prev_size = node.file_size
                        curr_size = file.size_on_disk()
                        node.file_size = curr_size
                        if curr_size >= prev_size:
                            self._cache_used += curr_size-prev_size
                        else:
                            self._cache_used -= prev_size-curr_size
                        if self._cache_used > self._cache_size:
                            raise FileCacheError('File cache is full!')
                    if node.num_readers == 0 and node.num_writers == 0:
                        node.removable = removable
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
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

    def __init__(self, cache_config, file_factory=default_file_factory):
        self._file_factory = file_factory
        self._cache_path = cache_config.get('cache-path', './cache')
        self._cache_used = 0
        self._cache_size = parse_mem_size(cache_config.get('cache-size', '1GB'))
        self._chunk_size = parse_mem_size(cache_config.get('chunk-size', '1MB'))

        #
        # TODO: Refactor such that index is persisted in db.
        # Till then, we have to cleanup the cache on each startup.
        #
        self._index = dict()
        self._index_lock = RLock()

        if os.path.exists(self._cache_path):
            shutil.rmtree(self._cache_path)

        if not os.path.exists(self._cache_path):
            os.mkdir(self._cache_path)
            logging.info('File cache created in path [{}]'.format(self._cache_path))
        
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

    def create_cache_entry(self, file_id, file_path, file_name, file_version, file_size):
        if file_id is None or not File.is_valid_file_id(file_id):
            raise FileCacheError('Invalid file id!')
        if file_path is None:
            raise FileCacheError('No path specified')
        if file_name is None:
            raise FileCacheError('No file name specified')
        with self._index_lock:
            if file_id in self._index:
                raise FileCacheError('File [{}] already exists in cache'.format(file_id))
            if file_size > self.cache_free_space():
                raise FileCacheError('Not enough free space in cache')
            self._index[file_id] = {
                READY: Event(),
                READABLE_FLAG: False,
                WRITABLE_FLAG: True,
                REMOVABLE_FLAG: False,
                FILE_PATH: file_path,
                FILE_NAME: file_name,
                FILE_VERSION: file_version,
                FILE_SIZE: file_size
            }
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
        if file_id is not None and File.is_valid_file_id(file_id):
            self._index_lock.acquire()
            try:
                if file_id in self._index:
                    entry = self._index[file_id]
                    if timeout is not None:
                        #
                        # Release the lock and then wait for the file to
                        # be ready to read. Once the flag is set or we timeout
                        # reacquire the lock and check the index entry again.
                        #
                        self._index_lock.release()
                        entry[READY].wait(timeout)
                        self._index_lock.acquire()
                        if file_id not in self._index:
                            #
                            # File was removed while we waited, cache miss.
                            #
                            return
                        entry = self._index[file_id]
                    if entry[READABLE_FLAG]:
                        if entry[WRITABLE_FLAG]:
                            raise Exception('File [{}] in invalid state'.format(file_id))
                        entry[REMOVABLE_FLAG] = False
                        entry = dict(entry)
                        entry['file'] = self._file_factory(self._cache_path, file_id, mode='r')
                    else:
                        entry = dict(entry)
                        entry['file'] = None
                    return entry
            finally:
                self._index_lock.release()
        else:
            raise FileCacheError('Invalid file id')


    '''
        Open file in cache for writing or appending.
        While file is opened, it will be locked preventing its removal from the cache.
        Allow multipled readers and single writer.

        New file is opened for writing in the cache and returned. The file
        must be closed in order for it be accessed by readers.
    '''
    def open_file(self, file_id=None, file_path=None, file_name=None, file_version=1, file_size=0, mode='w'):
        if mode == 'w':
            file = self._file_factory(self._cache_path, file_id=file_id, mode=mode)
            self.create_cache_entry(file.file_id(), file_path, file_name, file_version, file_size)
            return file
        elif mode == 'a':
            if file_id is not None:
                if File.is_valid_file_id(file_id):
                    with self._index_lock:
                        if file_id in self._index:
                            entry = self._index[file_id]
                            if entry[READABLE_FLAG]:
                                raise FileCacheError('Cannot append to readable file [{}]'.format(file_id))
                            if entry[REMOVABLE_FLAG]:
                                raise FileCacheError('Cannot append to removable file [{}]'.format(file_id))
                            if entry[WRITABLE_FLAG]:
                                entry = dict(entry)
                                entry['file'] = self._file_factory(self._cache_path, file_id, mode)
                            else:
                                raise FileCacheError('File [{}] in invalid state'.format(file_id))
                            return entry
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
            if file_id in self._index:
                entry = self._index[file.file_id()]
                entry[READABLE_FLAG] = readable
                entry[WRITABLE_FLAG] = writable
                entry[REMOVABLE_FLAG] = removable
                if mode == 'w' or mode == 'a':
                    prev_size = entry[FILE_SIZE]
                    curr_size = file.size_on_disk()
                    entry[FILE_SIZE] = curr_size
                    self._cache_used += curr_size-prev_size
                    if self._cache_used > self._cache_size:
                        raise FileCacheError('File cache is full!')
                if readable:
                    entry[READY].set()
            else:
                raise FileCacheError('File [{}] not found in cache'.format(file_id))

    '''
        Remove file from cache.

    '''
    def remove_file(self, file):
        with self._index_lock:
            file_id = file.file_id()
            if file_id in self._index:
                entry = self._index[file_id]
                self._index.pop(file_id)
                # Notify any readers waiting.
                entry[READY].set()
                file.remove()
                self._cache_used -= entry[FILE_SIZE]
            else:
                raise FileCacheError('File [{}] not found in cache'.format(file_id))

    '''
        Remove the LRU (least recently used file) from the cache to free up space.

    '''
    def remove_lru_file(self):
        # TODO
        pass
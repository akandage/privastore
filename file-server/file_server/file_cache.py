from .error import FileCacheError, FileError
from .file import File
from .util.file import parse_mem_size, str_mem_size
import logging
import os
from threading import RLock

READABLE_FLAG = 'readable'
WRITABLE_FLAG = 'writable'
REMOVABLE_FLAG = 'removable'
FILE_PATH = 'file_path'
FILE_NAME = 'file_name'

def default_file_factory(cache_path, file_id=None, mode='r'):
    return File(cache_path, file_id=file_id, mode=mode)

class FileCache(object):

    def __init__(self, cache_config, file_factory=default_file_factory):
        self._file_factory = file_factory
        self._cache_path = cache_config.get('cache-path', './cache')
        self._cache_used = 0
        self._cache_size = parse_mem_size(cache_config.get('cache-size', '1GB'))
        self._chunk_size = parse_mem_size(cache_config.get('chunk-size', '1MB'))

        self._index = dict()
        self._index_lock = RLock()

        for file_id in os.listdir(self._cache_path):
            f = None
            try:
                f = File(self._cache_path, file_id, mode='r')
                self._cache_used += f.size_on_disk()
                f.close()
            except Exception as e:
                logging.error('Could not open cache file [{}]: {}'.format(file_id, str(e)))
                try:
                    f.remove()
                    logging.debug('Removed file [{}] from cache'.format(file_id))
                except:
                    pass
        
        logging.debug('File cache used [{}]'.format(str_mem_size(self._cache_used)))
        logging.debug('File cache size [{}]'.format(str_mem_size(self._cache_size)))
        logging.debug('File chunk size [{}]'.format(str_mem_size(self._chunk_size)))

    def create_cache_entry(self, file_id, file_path=None, file_name=None):
        if not File.is_valid_file_id(file_id):
            raise FileCacheError('Invalid file id!')
        with self._index_lock:
            if file_id in self._index:
                raise FileCacheError('File [{}] already exists in cache'.format(file_id))
            self._index[file_id] = {
                READABLE_FLAG: False,
                WRITABLE_FLAG: True,
                REMOVABLE_FLAG: False,
                FILE_PATH: file_path,
                FILE_NAME: file_name
            }

    '''
        Open file in cache.
        While file is opened, it will be locked preventing its removal from the cache.
        Allow multipled readers and single writer.

        In read mode. If file is found (cache hit) return the index entry - the readable
        flag will indicate whether the file can be read from.
        Otherwise, the file is not found (cache miss) and None is returned.
 
    '''
    def open_file(self, file_id=None, file_path=None, file_name=None, mode='r'):
        if mode == 'w':
            file = self._file_factory(self._cache_path, mode=mode)
            self.create_cache_entry(file.file_id(), file_path, file_name)
            return file
        elif mode == 'a':
            if file_id is not None:
                if File.is_valid_file_id(file_id):
                    with self._index_lock:
                        entry = self._index[file_id]
                        if entry[READABLE_FLAG]:
                            raise FileCacheError('Attempt to append cache file which is readable')
                        if entry[REMOVABLE_FLAG]:
                            raise FileCacheError('Attempt to append cache file which is removable')
                        if entry[WRITABLE_FLAG]:
                            entry = dict(entry)
                            entry['file'] = self._file_factory(self._cache_path, file_id, mode)
                        else:
                            raise FileCacheError('Attempt to append cache file which is not writable')
                        return entry
                else:
                    raise FileCacheError('Invalid file id')
            else:
                raise FileCacheError('No file id specified')
        elif mode == 'r':
            if file_id is not None:
                if File.is_valid_file_id(file_id):
                    with self._index_lock:
                        if file_id in self._index:
                            entry = self._index[file_id]
                            if entry[WRITABLE_FLAG]:
                                raise FileCacheError('Attempt to read cache file which is writable')
                            if entry[READABLE_FLAG]:
                                entry[REMOVABLE_FLAG] = False
                                entry = dict(entry)
                                entry['file'] = self._file_factory(self._cache_path, file_id, mode)
                            else:
                                entry = dict(entry)
                                entry['file'] = None
                            return entry
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
            if file_id in self._index:
                entry = self._index[file.file_id()]
                entry[READABLE_FLAG] = readable
                entry[WRITABLE_FLAG] = writable
                entry[REMOVABLE_FLAG] = removable
            else:
                raise FileCacheError('File [{}] not found in cache'.format(file_id))

    '''
        Remove file from cache.

    '''
    def remove_file(self, file_id):
        pass

    '''
        Remove the LRU (least recently used file) from the cache to free up space.

    '''
    def remove_lru_file(self):
        # TODO
        pass
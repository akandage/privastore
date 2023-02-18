import logging
import time
from typing import BinaryIO, Optional
import uuid

from ..db.conn_pool import DbConnectionPool
from ..db.file_data_dao import FileDataDAOFactory
from ..error import FileError, NotImplementedError
from ..file import FileData
from .file_handle import FileHandle
from .file_store import FileStore

class DbFileStore(FileStore):

    CHUNK_SIZE = 64*1024

    def __init__(self, conn_pool: DbConnectionPool, dao_factory: FileDataDAOFactory):
        self._conn_pool = conn_pool
        self._dao_factory = dao_factory
    
    def conn_pool(self) -> DbConnectionPool:
        return self._conn_pool
    
    def dao_factory(self) -> FileDataDAOFactory:
        return self._dao_factory

    @staticmethod
    def generate_uid():
        return 'FD-' + str(uuid.uuid4())

    def open_for_reading(self, uid: str, blocking: Optional[bool] = False) -> FileHandle:
        fh = DbFileReadHandle(uid, self)
        fh.open()
        return fh
    
    def open_for_writing(self) -> FileHandle:
        uid = DbFileStore.generate_uid()
        fh = DbFileWriteHandle(uid, self)
        fh.open()
        return fh

class DbFileReadHandle(FileHandle):

    def __init__(self, uid: str, store: DbFileStore):
        super().__init__()
        self._fd = None
        self._uid = uid
        self._store = store
        self._chunk_id = 1
    
    def open(self):
        conn = self.store().conn_pool().acquire()
        conn.set_autocommit(True)
        try:
            dao = self.store().dao_factory().file_data_dao(conn)
            self._fd = dao.get_file_data(self.uid())
        finally:
            self.store().conn_pool().release(conn)

    def store(self) -> DbFileStore:
        return self._store

    def uid(self) -> str:
        return self._uid

    def fd(self) -> FileData:
        return self._fd

    def read_chunk(self, timeout: Optional[float]=None) -> bytes:
        if self._chunk_id > self.fd().total_blocks():
            return b''

        start = time.time()
        now = start
        while timeout is None or now >= start+timeout:
            conn = self.store().conn_pool().acquire()
            conn.set_autocommit(True)
            try:
                dao = self.store().dao_factory().file_data_dao(conn)
                chunk_data = dao.read_chunk(self.fd().id(), self._chunk_id)
            finally:
                self.store().conn_pool().release(conn)
            
            if len(chunk_data) > 0:
                return chunk_data
            else:
                # TODO: Configure this.
                time.sleep(0.1)
                now = time.time()
        
        raise FileError('Timed out trying to read file [{}] chunk [{}]'.format(self._uid, self._chunk_id), FileError.IO_TIMEOUT)

    def close(self):
        # No-op.
        pass

class DbFileWriteHandle(FileHandle):

    def __init__(self, uid: str, store: DbFileStore):
        super().__init__()
        self._fd_id = None
        self._uid = uid
        self._store = store
    
    def open(self):
        conn = self.store().conn_pool().acquire()
        conn.set_autocommit(True)
        try:
            dao = self.store().dao_factory().file_data_dao(conn)
            self._fd_id = dao.create_file_data(self.uid())
        finally:
            self.store().conn_pool().release(conn)

    def store(self) -> DbFileStore:
        return self._store

    def uid(self) -> str:
        return self._uid

    def fd_id(self) -> int:
        return self._fd_id
    
    def append_all(self, stream: BinaryIO) -> int:
        blocks = 0
        try:
            while True:
                chunk = stream.read(DbFileStore.CHUNK_SIZE)
                if len(chunk) > 0:
                    self.append_chunk(chunk)
                else:
                    break
                blocks += 1
            return blocks
        finally:
            try:
                stream.close()
            except:
                pass

    def append_chunk(self, data: bytes) -> int:
        conn = self.store().conn_pool().acquire()
        conn.set_autocommit(True)
        try:
            dao = self.store().dao_factory().file_data_dao(conn)
            return dao.append_chunk(self.fd_id(), data)
        finally:
            self.store().conn_pool().release(conn)
    
    def close(self):
        # No-op.
        pass

    def remove(self):
        conn = self.store().conn_pool().acquire()
        conn.set_autocommit(True)
        try:
            dao = self.store().dao_factory().file_data_dao(conn)
            dao.remove_file_data(self.uid())
        finally:
            self.store().conn_pool().release(conn)
    
    def remove_nothrow(self):
        try:
            self.remove()
        except Exception as e:
            logging.error('Error removing file data [{}]: {}'.format(self.uid(), str(e)))
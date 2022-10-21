import os
import unittest
import uuid
from ....error import DirectoryError, FileError
from .setup import sqlite_conn_factory, setup_db
from .directory_dao import SqliteDirectoryDAO
from .file_dao import SqliteFileDAO
from ...file_type import FileType
from ...file_transfer_status import FileTransferStatus

class TestSqliteFileDAO(unittest.TestCase):
    
    def setUp(self):
        config = {
            'sqlite-db-path': 'test_file_dao.db'
        }
        try:
            os.remove('test_file_dao.db')
        except:
            pass
        setup_db(config)
        self.conn = sqlite_conn_factory('test_file_dao.db')()
        self.dir_dao = SqliteDirectoryDAO(self.conn)
        self.dao = SqliteFileDAO(self.conn)

    def tearDown(self):
        try:
            self.conn.close()
        except:
            pass
        try:
            os.remove('test_file_dao.db')
        except:
            pass
    
    def test_get_file_version_metadata(self):
        self.dir_dao.create_file([], 'file_1')
        self.dir_dao.create_file([], 'file_2')
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_1'), {
            'file_type': FileType.BINARY_DATA,
            'version': 1,
            'local_id': None,
            'remote_id': None,
            'size_bytes': 0,
            'transfer_status': FileTransferStatus.EMPTY
        })
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_1'), {
            'file_type': FileType.BINARY_DATA,
            'version': 1,
            'local_id': None,
            'remote_id': None,
            'size_bytes': 0,
            'transfer_status': FileTransferStatus.EMPTY
        })

    def test_update_local_file(self):
        f1_local_id = 'F-{}'.format(uuid.uuid4())
        f2_local_id = 'F-{}'.format(uuid.uuid4())
        self.dir_dao.create_file([], 'file_1')
        self.dir_dao.create_file([], 'file_2')
        self.dao.update_file_local([], 'file_1', 1, f1_local_id, 100)
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_1'), {
            'file_type': FileType.BINARY_DATA,
            'version': 1,
            'local_id': f1_local_id,
            'remote_id': None,
            'size_bytes': 100,
            'transfer_status': FileTransferStatus.RECEIVED
        })
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_2'), {
            'file_type': FileType.BINARY_DATA,
            'version': 1,
            'local_id': None,
            'remote_id': None,
            'size_bytes': 0,
            'transfer_status': FileTransferStatus.EMPTY
        })
        self.dao.update_file_local([], 'file_2', 1, f2_local_id, 120)
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_1'), {
            'file_type': FileType.BINARY_DATA,
            'version': 1,
            'local_id': f1_local_id,
            'remote_id': None,
            'size_bytes': 100,
            'transfer_status': FileTransferStatus.RECEIVED
        })
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_2'), {
            'file_type': FileType.BINARY_DATA,
            'version': 1,
            'local_id': f2_local_id,
            'remote_id': None,
            'size_bytes': 120,
            'transfer_status': FileTransferStatus.RECEIVED
        })
    
    def test_update_remote_file(self):
        f1_local_id = 'F-{}'.format(uuid.uuid4())
        f1_remote_id = 'F-{}'.format(uuid.uuid4())
        f2_local_id = 'F-{}'.format(uuid.uuid4())
        f2_remote_id = 'F-{}'.format(uuid.uuid4())
        self.dir_dao.create_file([], 'file_1')
        self.dir_dao.create_file([], 'file_2')
        self.dao.update_file_local([], 'file_1', 1, f1_local_id, 100)
        self.dao.update_file_remote([], 'file_1', 1, f1_remote_id, FileTransferStatus.TRANSFERRED_TO_REMOTE)
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_1'), {
            'file_type': FileType.BINARY_DATA,
            'version': 1,
            'local_id': f1_local_id,
            'remote_id': f1_remote_id,
            'size_bytes': 100,
            'transfer_status': FileTransferStatus.TRANSFERRED_TO_REMOTE
        })
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_2'), {
            'file_type': FileType.BINARY_DATA,
            'version': 1,
            'local_id': None,
            'remote_id': None,
            'size_bytes': 0,
            'transfer_status': FileTransferStatus.EMPTY
        })
        self.dao.update_file_local([], 'file_2', 1, f2_local_id, 120)
        self.dao.update_file_remote([], 'file_2', 1, f2_remote_id, FileTransferStatus.TRANSFERRED_TO_REMOTE)
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_1'), {
            'file_type': FileType.BINARY_DATA,
            'version': 1,
            'local_id': f1_local_id,
            'remote_id': f1_remote_id,
            'size_bytes': 100,
            'transfer_status': FileTransferStatus.TRANSFERRED_TO_REMOTE
        })
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_2'), {
            'file_type': FileType.BINARY_DATA,
            'version': 1,
            'local_id': f2_local_id,
            'remote_id': f2_remote_id,
            'size_bytes': 120,
            'transfer_status': FileTransferStatus.TRANSFERRED_TO_REMOTE
        })
    
    def test_update_file_transfer_status(self):
        f1_local_id = 'F-{}'.format(uuid.uuid4())
        self.dir_dao.create_file([], 'file_1')
        self.dir_dao.create_file([], 'file_2')
        self.dao.update_file_transfer_status([], 'file_1', 1, FileTransferStatus.RECEIVING)
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_1'), {
            'file_type': FileType.BINARY_DATA,
            'version': 1,
            'local_id': None,
            'remote_id': None,
            'size_bytes': 0,
            'transfer_status': FileTransferStatus.RECEIVING
        })
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_2'), {
            'file_type': FileType.BINARY_DATA,
            'version': 1,
            'local_id': None,
            'remote_id': None,
            'size_bytes': 0,
            'transfer_status': FileTransferStatus.EMPTY
        })
        self.dao.update_file_local([], 'file_1', 1, f1_local_id, 100)
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_1'), {
            'file_type': FileType.BINARY_DATA,
            'version': 1,
            'local_id': f1_local_id,
            'remote_id': None,
            'size_bytes': 100,
            'transfer_status': FileTransferStatus.RECEIVED
        })
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_2'), {
            'file_type': FileType.BINARY_DATA,
            'version': 1,
            'local_id': None,
            'remote_id': None,
            'size_bytes': 0,
            'transfer_status': FileTransferStatus.EMPTY
        })
        self.dao.update_file_transfer_status([], 'file_2', 1, FileTransferStatus.RECEIVING)
        self.dao.update_file_transfer_status([], 'file_1', 1, FileTransferStatus.TRANSFERRING_TO_REMOTE)
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_1'), {
            'file_type': FileType.BINARY_DATA,
            'version': 1,
            'local_id': f1_local_id,
            'remote_id': None,
            'size_bytes': 100,
            'transfer_status': FileTransferStatus.TRANSFERRING_TO_REMOTE
        })
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_2'), {
            'file_type': FileType.BINARY_DATA,
            'version': 1,
            'local_id': None,
            'remote_id': None,
            'size_bytes': 0,
            'transfer_status': FileTransferStatus.RECEIVING
        })
import os
import unittest
import uuid
from ....error import DirectoryError, FileError
from ....db.sqlite.conn_factory import sqlite_conn_factory
from .setup import setup_db
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
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_1'), (
            FileType.BINARY_DATA,
            1,
            None,
            None,
            0,
            0,
            0,
            FileTransferStatus.EMPTY
        ))
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_1'), (
            FileType.BINARY_DATA,
            1,
            None,
            None,
            0,
            0,
            0,
            FileTransferStatus.EMPTY
        ))

    def test_update_local_file(self):
        f1_local_id = 'F-{}'.format(uuid.uuid4())
        f2_local_id = 'F-{}'.format(uuid.uuid4())
        self.dir_dao.create_file([], 'file_1')
        self.dir_dao.create_file([], 'file_2')
        self.dao.update_file_local([], 'file_1', 1, f1_local_id, 100, 120, 1)
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_1'), (
            FileType.BINARY_DATA,
            1,
            f1_local_id,
            None,
            100,
            120,
            1,
            FileTransferStatus.RECEIVED
        ))
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_2'), (
            FileType.BINARY_DATA,
            1,
            None,
            None,
            0,
            0,
            0,
            FileTransferStatus.EMPTY
        ))
        self.dao.update_file_local([], 'file_2', 1, f2_local_id, 2000, 2200, 2)
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_1'), (
            FileType.BINARY_DATA,
            1,
            f1_local_id,
            None,
            100,
            120,
            1,
            FileTransferStatus.RECEIVED
        ))
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_2'), (
            FileType.BINARY_DATA,
            1,
            f2_local_id,
            None,
            2000,
            2200,
            2,
            FileTransferStatus.RECEIVED
        ))
    
    def test_update_remote_file(self):
        f1_local_id = 'F-{}'.format(uuid.uuid4())
        f1_remote_id = 'F-{}'.format(uuid.uuid4())
        f2_local_id = 'F-{}'.format(uuid.uuid4())
        f2_remote_id = 'F-{}'.format(uuid.uuid4())
        self.dir_dao.create_file([], 'file_1')
        self.dir_dao.create_file([], 'file_2')
        self.dao.update_file_local([], 'file_1', 1, f1_local_id, 100, 120, 1)
        self.dao.update_file_remote([], 'file_1', 1, f1_remote_id, FileTransferStatus.TRANSFERRED_TO_REMOTE)
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_1'), (
            FileType.BINARY_DATA,
            1,
            f1_local_id,
            f1_remote_id,
            100,
            120,
            1,
            FileTransferStatus.TRANSFERRED_TO_REMOTE
        ))
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_2'), (
            FileType.BINARY_DATA,
            1,
            None,
            None,
            0,
            0,
            0,
            FileTransferStatus.EMPTY
        ))
        self.dao.update_file_local([], 'file_2', 1, f2_local_id, 120, 140, 1)
        self.dao.update_file_remote([], 'file_2', 1, f2_remote_id, FileTransferStatus.TRANSFERRED_TO_REMOTE)
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_1'), (
            FileType.BINARY_DATA,
            1,
            f1_local_id,
            f1_remote_id,
            100,
            120,
            1,
            FileTransferStatus.TRANSFERRED_TO_REMOTE
        ))
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_2'), (
            FileType.BINARY_DATA,
            1,
            f2_local_id,
            f2_remote_id,
            120,
            140,
            1,
            FileTransferStatus.TRANSFERRED_TO_REMOTE
        ))
    
    def test_update_file_transfer_status(self):
        f1_local_id = 'F-{}'.format(uuid.uuid4())
        self.dir_dao.create_file([], 'file_1')
        self.dir_dao.create_file([], 'file_2')
        self.dao.update_file_transfer_status([], 'file_1', 1, FileTransferStatus.RECEIVING)
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_1'), (
            FileType.BINARY_DATA,
            1,
            None,
            None,
            0,
            0,
            0,
            FileTransferStatus.RECEIVING
        ))
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_2'), (
            FileType.BINARY_DATA,
            1,
            None,
            None,
            0,
            0,
            0,
            FileTransferStatus.EMPTY
        ))
        self.dao.update_file_local([], 'file_1', 1, f1_local_id, 100, 100, 1)
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_1'), (
            FileType.BINARY_DATA,
            1,
            f1_local_id,
            None,
            100,
            100,
            1,
            FileTransferStatus.RECEIVED
        ))
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_2'), (
            FileType.BINARY_DATA,
            1,
            None,
            None,
            0,
            0,
            0,
            FileTransferStatus.EMPTY
        ))
        self.dao.update_file_transfer_status([], 'file_2', 1, FileTransferStatus.RECEIVING)
        self.dao.update_file_transfer_status([], 'file_1', 1, FileTransferStatus.TRANSFERRING_TO_REMOTE)
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_1'), (
            FileType.BINARY_DATA,
            1,
            f1_local_id,
            None,
            100,
            100,
            1,
            FileTransferStatus.TRANSFERRING_TO_REMOTE
        ))
        self.assertEqual(self.dao.get_file_version_metadata([], 'file_2'), (
            FileType.BINARY_DATA,
            1,
            None,
            None,
            0,
            0,
            0,
            FileTransferStatus.RECEIVING
        ))
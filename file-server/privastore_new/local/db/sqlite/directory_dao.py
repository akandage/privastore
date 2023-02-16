import logging
import time
from typing import Optional

from ....crypto.util import hash_user_password
from ....db.conn import SqliteConnection
from ....db.dao import DataAccessObject
from ..directory_dao import DirectoryDAO
from ....directory import Directory, DirectoryEntry
from ....error import DirectoryError, FileError
from ....file import File
from ....util import str_path

class SqliteDirectoryDAO(DataAccessObject, DirectoryDAO):

    def __init__(self, conn: SqliteConnection):
        super().__init__(conn)
    
    def conn(self) -> SqliteConnection:
        return super().conn()
    
    def get_root_directory_id(self, owner: str) -> int:
        cur = self.conn().cursor()
        try:
            cur.execute('SELECT id FROM ps_directory WHERE name = ? AND owner = ?', ('/', owner))
            res = cur.fetchone()
            if res is None:
                raise DirectoryError('User [{}] root directory not found!'.format(owner))
            return res[0]
        finally:
            try:
                cur.close()
            except:
                pass

    def get_root_directory_uid(self, owner: str) -> str:
        cur = self.conn().cursor()
        try:
            cur.execute('SELECT uid FROM ps_directory WHERE name = ? AND owner = ?', ('/', owner))
            res = cur.fetchone()
            if res is None:
                raise DirectoryError('User [{}] root directory not found!'.format(owner))
            return res[0]
        finally:
            try:
                cur.close()
            except:
                pass

    def get_directory_id_by_uid(self, uid: str, owner: str) -> int:
        cur = self.conn().cursor()
        try:
            cur.execute('''
                SELECT id
                FROM ps_directory
                WHERE uid = ? AND owner = ?
            ''', (uid, owner))
            res = cur.fetchone()
            if res is None:
                raise DirectoryError('Directory [{}] not found'.format(uid), DirectoryError.DIRECTORY_NOT_FOUND)
            return res[0]
        finally:
            cur.close()

    def get_directory_id(self, path: list[str], owner: str) -> int:
        curr_id = self.get_root_directory_id(owner)
        traversed = []
        cur = self.conn().cursor()
        try:
            for name in path:
                cur.execute('''
                    SELECT D.id 
                    FROM ps_directory AS D INNER JOIN ps_directory_link AS L ON D.id = L.child_id
                    WHERE L.parent_id = ? AND D.name = ? AND D.owner = ?
                ''', (curr_id, name, owner))
                res = cur.fetchone()
                if res is None:
                    raise DirectoryError('Directory [{}] not found in path [{}]'.format(name, traversed), DirectoryError.INVALID_PATH)
                traversed.append(name)
                curr_id = res[0]
        finally:
            try:
                cur.close()
            except:
                pass
        return curr_id

    def abs_path(self, dir_uid: str, owner: str) -> list[str]:
        root_id = self.get_root_directory_id(owner)
        curr_id = self.get_directory_id_by_uid(dir_uid, owner)
        path = []

        cur = self.conn().cursor()
        try:
            while curr_id != root_id:
                cur.execute('''
                    SELECT D.name, L.parent_id
                    FROM ps_directory AS D INNER JOIN ps_directory_link AS L ON D.id = L.child_id
                    WHERE D.id = ? AND D.owner = ?
                ''', (curr_id, owner))
                res = cur.fetchone()
                if res is None:
                    raise DirectoryError('Could not get directory [{}] absolute path. No path to root directory!'.format(dir_uid))
                name, parent_id = res
                path.append(name)
                curr_id = parent_id
            path.reverse()
            return path
        finally:
            try:
                cur.close()
            except:
                pass
    
    def directory_exists(self, parent_id: int, name: str, owner: str) -> bool:
        cur = self.conn().cursor()
        try:
            cur.execute('''
                SELECT D.id
                FROM ps_directory AS D INNER JOIN ps_directory_link AS L ON D.id = L.child_id
                WHERE L.parent_id = ? AND D.name = ? AND D.owner = ?
            ''', (parent_id, name, owner))
            res = cur.fetchone()
            return res is not None
        finally:
            try:
                cur.close()
            except:
                pass

    def file_exists(self, parent_id: int, name: str, owner: str) -> bool:
        cur = self.conn().cursor()
        try:
            cur.execute('''
                SELECT id
                FROM ps_file
                WHERE parent_id = ? AND name = ? AND owner = ?
            ''', (parent_id, name, owner))
            res = cur.fetchone()
            return res is not None
        finally:
            try:
                cur.close()
            except:
                pass

    def check_exists(self, parent_id: int, name: str, owner: str):
        if self.directory_exists(parent_id, name, owner):
            raise DirectoryError('Directory [{}] exists'.format(name), DirectoryError.DIRECTORY_EXISTS)
        elif self.file_exists(parent_id, name, owner):
            raise FileError('File [{}] exists'.format(name), FileError.FILE_EXISTS)

    def get_root_directory(self, owner: str) -> Directory:
        self.begin_transaction()
        cur = self.conn().cursor()
        try:
            cur.execute('''
                SELECT name, uid, created_timestamp, modified_timestamp, owner
                FROM ps_directory
                WHERE name = ? AND owner = ?
            ''', ('/', owner))
            res = cur.fetchone()
            if res is None:
                raise DirectoryError('No root directory found for user [{}]!'.format(owner))
            name, uid, created_timestamp, modified_timestamp, owner = res
            self.commit()
            return Directory(
                name,
                uid,
                '/',
                created_timestamp,
                modified_timestamp,
                owner
            )
        except DirectoryError as e:
            logging.error('Directory error: {}'.format(str(e)))
            self.rollback_nothrow()
            raise e
        except Exception as e:
            logging.error('Query error: {}'.format(str(e)))
            self.rollback_nothrow()
            raise e
        finally:
            try:
                cur.close()
            except:
                pass

    def path_to_directory(self, dir_uid: str, owner: str) -> list[str]:
        Directory.validate_uuid(dir_uid)

        self.begin_transaction()
        try:
            abs_path = self.abs_path(dir_uid, owner)
            self.commit()
            return abs_path
        except DirectoryError as e:
            logging.error('Directory error: {}'.format(str(e)))
            self.rollback_nothrow()
            raise e
        except Exception as e:
            logging.error('Query error: {}'.format(str(e)))
            self.rollback_nothrow()
            raise e

    def create_directory(self, parent_uid: str, name: str, owner: str) -> Directory:
        Directory.validate_uuid(parent_uid)
        if len(name) == 0:
            raise DirectoryError('Directory name cannot be empty', DirectoryError.INVALID_DIRECTORY_NAME)

        logging.debug('Create directory [{}], parent-uid [{}]'.format(name, parent_uid))
        self.begin_transaction()
        cur = self.conn().cursor()
        try:
            parent_id = self.get_directory_id_by_uid(parent_uid, owner)
            self.check_exists(parent_id, name, owner)
            uid = Directory.generate_uid()
            now = round(time.time())
            cur.execute('''INSERT INTO ps_directory 
                (name, uid, created_timestamp, modified_timestamp, owner)
                VALUES (?, ?, ?, ?, ?)''', (name, uid, now, now, owner))
            cur.execute('''INSERT INTO ps_directory_link
                (parent_id, child_id) VALUES (?, ?)''', (parent_id, cur.lastrowid))
            abs_path = self.abs_path(uid, owner)
            self.commit()
            logging.debug('Created directory')
            return Directory(
                name,
                uid,
                abs_path,
                now,
                now,
                owner
            )
        except DirectoryError as e:
            logging.error('Directory error: {}'.format(str(e)))
            self.rollback_nothrow()
            raise e
        except FileError as e:
            logging.error('File error: {}'.format(str(e)))
            self.rollback_nothrow()
            raise e
        except Exception as e:
            logging.error('Query error: {}'.format(str(e)))
            self.rollback_nothrow()
            raise e
        finally:
            try:
                cur.close()
            except:
                pass
    
    def create_file(self, parent_uid: str, name: str, mime_type: str, owner: str) -> File:
        Directory.validate_uuid(parent_uid)
        if len(name) == 0:
            raise FileError('File name cannot be empty', FileError.INVALID_FILE_NAME)

        logging.debug('Create file [{}], type [{}], parent-uid [{}]'.format(name, mime_type, parent_uid))
        self.begin_transaction()
        cur = self.conn().cursor()
        try:
            parent_id = self.get_directory_id_by_uid(parent_uid, owner)
            self.check_exists(parent_id, name, owner)
            uid = File.generate_uid()
            now = round(time.time())
            cur.execute('''INSERT INTO ps_file 
                (name, uid, mime_type, created_timestamp, modified_timestamp, owner, parent_id)
                VALUES (?, ?, ?, ?, ?, ?)''', (name, uid, mime_type, now, now, owner, parent_id))
            abs_path = self.abs_path(uid, owner)
            self.commit()
            logging.debug('Created file')
            return File(
                name,
                uid,
                abs_path,
                now,
                now,
                owner
            )
        except DirectoryError as e:
            logging.error('Directory error: {}'.format(str(e)))
            self.rollback_nothrow()
            raise e
        except FileError as e:
            logging.error('File error: {}'.format(str(e)))
            self.rollback_nothrow()
            raise e
        except Exception as e:
            logging.error('Query error: {}'.format(str(e)))
            self.rollback_nothrow()
            raise e
        finally:
            try:
                cur.close()
            except:
                pass
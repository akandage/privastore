import logging
import time

from ....db.conn import SqliteConnection
from ....db.dao import DataAccessObject
from ..directory_dao import DirectoryDAO
from ....directory import Directory
from ....error import DirectoryError, FileError
from ....file import File, FileData, FileVersion

class SqliteDirectoryDAO(DataAccessObject, DirectoryDAO):

    def __init__(self, conn: SqliteConnection):
        super().__init__(conn)
    
    def conn(self) -> SqliteConnection:
        return super().conn()
    
    def _get_root_directory_id(self, owner: str) -> int:
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

    def _get_root_directory_uid(self, owner: str) -> str:
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

    def _get_directory_id_by_uid(self, uid: str, owner: str) -> int:
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

    def _get_file_id_by_uid(self, uid: str, owner: str) -> int:
        cur = self.conn().cursor()
        try:
            cur.execute('''
                SELECT id
                FROM ps_file
                WHERE uid = ? AND owner = ?
            ''', (uid, owner))
            res = cur.fetchone()
            if res is None:
                raise FileError('File [{}] not found'.format(uid), FileError.FILE_NOT_FOUND)
            return res[0]
        finally:
            cur.close()

    def _get_file_data_id_by_uid(self, uid: str) -> int:
        cur = self.conn().cursor()
        try:
            cur.execute('''
                SELECT id
                FROM ps_file_data
                WHERE uid = ?
            ''', (uid,))
            res = cur.fetchone()
            if res is None:
                raise FileError('File data [{}] not found'.format(uid), FileError.FILE_DATA_NOT_FOUND)
            return res[0]
        finally:
            cur.close()

    def _get_directory_id(self, path: list[str], owner: str) -> int:
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

    def _abs_path(self, dir_uid: str, owner: str) -> list[str]:
        root_id = self._get_root_directory_id(owner)
        curr_id = self._get_directory_id_by_uid(dir_uid, owner)
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
    
    def _directory_exists(self, parent_id: int, name: str, owner: str) -> bool:
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

    def _file_exists(self, parent_id: int, name: str, owner: str) -> bool:
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

    def _check_exists(self, parent_id: int, name: str, owner: str):
        if self._directory_exists(parent_id, name, owner):
            raise DirectoryError('Directory [{}] exists'.format(name), DirectoryError.DIRECTORY_EXISTS)
        elif self._file_exists(parent_id, name, owner):
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
                None,
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
            abs_path = self._abs_path(dir_uid, owner)
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

        logging.debug('Create directory [{}], parent-uid [{}], owner [{}]'.format(name, parent_uid, owner))
        self.begin_transaction()
        cur = self.conn().cursor()
        try:
            parent_id = self._get_directory_id_by_uid(parent_uid, owner)
            self._check_exists(parent_id, name, owner)
            uid = Directory.generate_uid()
            now = round(time.time())
            cur.execute('''INSERT INTO ps_directory 
                (name, uid, created_timestamp, modified_timestamp, owner)
                VALUES (?, ?, ?, ?, ?)''', (name, uid, now, now, owner))
            cur.execute('''INSERT INTO ps_directory_link
                (parent_id, child_id) VALUES (?, ?)''', (parent_id, cur.lastrowid))
            abs_path = self._abs_path(uid, owner)
            self.commit()
            logging.debug('Created directory')
            return Directory(
                name,
                uid,
                parent_uid,
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

        logging.debug('Create file [{}], type [{}], parent-uid [{}], owner [{}]'.format(name, mime_type, parent_uid, owner))
        self.begin_transaction()
        cur = self.conn().cursor()
        try:
            parent_id = self._get_directory_id_by_uid(parent_uid, owner)
            self._check_exists(parent_id, name, owner)
            uid = File.generate_uid()
            now = round(time.time())
            cur.execute('''INSERT INTO ps_file 
                (name, uid, mime_type, created_timestamp, modified_timestamp, owner, parent_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)''', (name, uid, mime_type, now, now, owner, parent_id))
            abs_path = self._abs_path(parent_uid, owner)
            abs_path.append(name)
            self.commit()
            logging.debug('Created file')
            return File(
                name,
                uid,
                mime_type,
                parent_uid,
                abs_path,
                now,
                now,
                owner,
                []
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
    
    def create_file_version(self, file_uid: str, file_data_uid: str, owner: str) -> FileVersion:
        logging.debug('Create file [{}] version, file-data-uid [{}], owner [{}]'.format(file_uid, file_data_uid, owner))
        self.begin_transaction()
        cur = self.conn().cursor()
        try:
            file_id = self._get_file_id_by_uid(file_uid, owner)
            file_data_id = self._get_file_data_id_by_uid(file_data_uid)

            cur.execute('''
                SELECT count(*)
                FROM ps_file_version AS V
                WHERE V.file_id = ?
            ''', (file_id,))
            version = 1
            res = cur.fetchone()
            if res is not None:
                version = res[0]+1
            cur.execute('''
                INSERT INTO ps_file_version (file_id, version, fd_id) VALUES (?, ?, ?)
            ''', (file_id, version, file_data_id))
            cur.execute('''
                UPDATE ps_file_data
                SET is_writable = 1
                WHERE id = ?
            ''', (file_data_id,))
            cur.execute('''
                SELECT size, total_chunks, created_timestamp, modified_timestamp, is_writable, is_synced
                FROM ps_file_data
                WHERE id = ?
            ''', (file_data_id,))
            size, total_chunks, created_timestamp, modified_timestamp, is_writable, is_synced = cur.fetchone()
            self.commit()
            logging.debug('Created file version')
            return FileVersion(
                file_uid,
                version,
                FileData(
                    file_data_id,
                    file_data_uid,
                    size,
                    total_chunks,
                    created_timestamp,
                    modified_timestamp,
                    is_writable,
                    is_synced
                )
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
    
    def file_exists(self, parent_uid: str, name: str, owner: str) -> File:
        Directory.validate_uuid(parent_uid)
        if len(name) == 0:
            raise FileError('File name cannot be empty', FileError.INVALID_FILE_NAME)

        self.begin_transaction()
        cur = self.conn().cursor()
        try:
            parent_id = self._get_directory_id_by_uid(parent_uid, owner)
            return self._file_exists(parent_id, name, owner)
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
    
    def get_file(self, parent_uid: str, name: str, owner: str) -> File:
        Directory.validate_uuid(parent_uid)
        if len(name) == 0:
            raise FileError('File name cannot be empty', FileError.INVALID_FILE_NAME)

        self.begin_transaction()
        cur = self.conn().cursor()
        try:
            parent_id = self._get_directory_id_by_uid(parent_uid, owner)
            cur.execute('''
                SELECT uid, mime_type, created_timestamp, modified_timestamp, owner
                FROM ps_file
                WHERE parent_id = ? AND name = ? AND owner = ?
            ''', (parent_id, name, owner))
            res = cur.fetchone()
            if res is None:
                raise FileError('File [{}] not found'.format(name), FileError.FILE_NOT_FOUND)
            uid, mime_type, created_timestamp, modified_timestamp, owner = res
            abs_path = self._abs_path(parent_uid, owner)
            abs_path.append(name)
            self.commit()
            return File(
                name,
                uid,
                mime_type,
                parent_uid,
                abs_path,
                created_timestamp,
                modified_timestamp,
                owner,
                []
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
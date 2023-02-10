import sqlite3
from typing import Optional

def sqlite_conn_factory(db_path: str, check_same_thread:bool=False, isolation_level:Optional[str]=None):
    def factory():
        conn = sqlite3.connect(db_path, check_same_thread=check_same_thread, isolation_level=isolation_level)
        # By default, SQLite disables foreign keys.
        conn.execute('PRAGMA foreign_keys = ON')
        return conn
    return factory
import sqlite3

def sqlite_conn_factory(db_path, check_same_thread=False, isolation_level=None):
    def connect():
        conn = sqlite3.connect(db_path, check_same_thread=check_same_thread, isolation_level=isolation_level)
        # By default, SQLite disables foreign keys.
        conn.execute('PRAGMA foreign_keys = ON')
        return conn
    return connect
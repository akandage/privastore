import hashlib
import logging
import sqlite3

def setup_db(db_config):
    db_path = db_config.get('sqlite-db-path', 'local_server.db')

    logging.debug('Setting up SQLite database \'{}\''.format(db_path))
    conn = sqlite3.connect(db_path)
    try:
        create_user_account_table(conn)
        create_directory_table(conn)
        create_file_table(conn)
        create_file_version_table(conn)
    finally:
        try:
            conn.close()
        except:
            pass

def create_user_account_table(conn):
    cur = conn.cursor()
    try:
        cur.execute(
            '''
            CREATE TABLE user_account (
                username VARCHAR(50) PRIMARY KEY NOT NULL,
                password_hash VARCHAR(32) NOT NULL
            )
            '''
        )
        # Create admin user with default password.
        cur.execute(
            '''
            INSERT INTO user_account (username, password_hash) VALUES 
            (?, ?)
            ''',
            ('psadmin', hashlib.sha256(b'psadmin').digest())
        )
        conn.commit()
    finally:
        try:
            cur.close()
        except:
            pass

def create_directory_table(conn):
    cur = conn.cursor()
    try:
        cur.execute(
            '''
            CREATE TABLE directory (
                id INTEGER PRIMARY KEY NOT NULL,
                name VARCHAR(256) NOT NULL
            )
            '''
        )
        # Create root directory.
        cur.execute("INSERT INTO directory (name) VALUES ('/')")
        cur.execute(
            '''
            CREATE TABLE link (
                parent_id INTEGER NOT NULL,
                child_id INTEGER NOT NULL,
                PRIMARY KEY (parent_id, child_id),
                FOREIGN KEY (parent_id) REFERENCES directory (id) ON DELETE CASCADE,
                FOREIGN KEY (child_id) REFERENCES directory (id) ON DELETE CASCADE
            )
            '''
        )
        conn.commit()
    finally:
        try:
            cur.close()
        except:
            pass

def create_file_table(conn):
    pass

def create_file_version_table(conn):
    pass